//
// UtpManager.cs
//
// Multiplexes uTP connections over a single UdpTransport (or any sender + packet source).
// Responsibilities:
//   - Connection ID allocation
//   - (remote, connId) → UtpConnection lookup / creation on SYN
//   - Dispatching incoming packets (after demux in UdpTransport)
//   - Driving periodic Tick() on all live sockets
//   - Cleanup of dead connections
//
// This is the equivalent of libtorrent's utp_socket_manager.
//

using System;
using System.Collections.Generic;
using System.Diagnostics;

using MonoTorrent;

namespace MonoTorrent.Connections.Utp
{
    public sealed class UtpManager
    {
        readonly UdpTransport _transport;
        readonly Dictionary<(CompactEndPoint Remote, ushort ConnId), UtpConnection> _sockets
            = new Dictionary<(CompactEndPoint, ushort), UtpConnection> ();

        readonly Random _random = new Random ();

        // Newly established passive (incoming) connections that the listener should pick up.
        readonly Queue<UtpConnection> _pendingIncoming = new Queue<UtpConnection> ();

        /// <summary>
        /// Fired when a new passive (incoming) uTP connection has been established at the uTP layer.
        /// Listeners (UtpPeerConnectionListener) can wrap it and raise their ConnectionReceived.
        /// </summary>
        public event Action<UtpConnection>? NewIncomingConnection;

        public UtpManager (UdpTransport transport,
            int targetDelayMs = 75,
            double gainFactor = 1.0,
            int receiveWindow = 1024 * 1024,
            int maxPacketSize = 1500,
            int synResends = 2,
            int finResends = 2,
            int numResends = 3,
            bool logEnabled = false,
            TimeSpan tickInterval = default,
            bool allowDynamicMtu = true)
        {
            _transport = transport ?? throw new ArgumentNullException (nameof (transport));

            TargetDelayMs = targetDelayMs;
            GainFactor = gainFactor;
            ReceiveWindow = receiveWindow;
            MaxPacketSize = maxPacketSize;
            SynResends = synResends;
            FinResends = finResends;
            NumResends = numResends;
            LogEnabled = logEnabled;
            TickInterval = tickInterval == default ? TimeSpan.FromMilliseconds(50) : tickInterval;
            AllowDynamicMtu = allowDynamicMtu;

            // Hook the demuxed uTP packets coming from the shared transport.
            _transport.UtpPacketReceived += OnUtpPacketReceived;
        }

        // Exposed config (production surface)
        public int TargetDelayMs { get; }
        public double GainFactor { get; }
        public int ReceiveWindow { get; }
        public int MaxPacketSize { get; }
        public int SynResends { get; }
        public int FinResends { get; }
        public int NumResends { get; }
        public bool LogEnabled { get; }
        public TimeSpan TickInterval { get; }
        public bool AllowDynamicMtu { get; }

        void OnUtpPacketReceived (ReadOnlyMemory<byte> packet, CompactEndPoint remote)
        {
            if (!UtpHeader.TryParse (packet.Span, out var hdr, out _))
                return;

            // basic resource limit on concurrent uTP sockets (hardening)
            if (_sockets.Count > 200)
                return;

            var key = (remote, hdr.ConnectionId);

            if (!_sockets.TryGetValue (key, out var conn)) {
                // Possible new incoming connection (SYN)
                if (hdr.Type == UtpPacketType.ST_SYN) {
                    // Allocate our side IDs.
                    // Responder convention: our recv id == the conn id we saw in the SYN.
                    ushort ourRecvId = hdr.ConnectionId;
                    ushort ourSendId = (ushort)((ourRecvId + 1) & UtpSeq.Mask); // common pattern

                    conn = new UtpConnection (ourSendId, ourRecvId, remote, SendPacket, incoming: true, 
                        targetDelayMs: TargetDelayMs, gainFactor: GainFactor,
                        synResends: SynResends, finResends: FinResends, numResends: NumResends,
                        minRtoMs: 500, initialRtoMs: 1000, connectTimeoutMs: 30000,
                        receiveWindow: ReceiveWindow, logEnabled: LogEnabled,
                        allowDynamicMtu: AllowDynamicMtu, maxPacketSize: MaxPacketSize);
                    _sockets[(remote, ourRecvId)] = conn;
                    _sockets[(remote, ourSendId)] = conn; // allow lookup either way for replies

                    // Deliver the SYN so the connection moves to Connected and acks it
                    conn.ProcessIncoming (packet, remote);

                    if (conn.State == UtpState.Connected) {
                        _pendingIncoming.Enqueue (conn);
                        NewIncomingConnection?.Invoke (conn);
                    }
                    return;
                }

                // Stray packet for unknown connection – ignore
                return;
            }

            // Existing connection
            bool alive = conn.ProcessIncoming (packet, remote);

            if (conn.State == UtpState.Deleting || conn.State == UtpState.ErrorWait) {
                RemoveConnection (conn);
            }
        }

        void SendPacket (ReadOnlyMemory<byte> buffer, CompactEndPoint endpoint)
        {
            // Fire-and-forget send via the shared transport.
            // We ignore the task here; errors are rare on UDP and will surface as missing acks.
            _ = _transport.SendAsync (buffer, endpoint);
        }

        /// <summary>
        /// Creates a new outgoing uTP connection to the given remote endpoint.
        /// The returned connection is already in SynSent state and has sent its initial SYN.
        /// </summary>
        public UtpConnection CreateOutgoing (CompactEndPoint remote)
        {
            // Pick a random connection id for the initiator side.
            ushort sendId = (ushort)_random.Next (0, 0x10000);
            ushort recvId = (ushort)((sendId + 1) & UtpSeq.Mask);

            var conn = new UtpConnection (sendId, recvId, remote, SendPacket, incoming: false, 
                targetDelayMs: TargetDelayMs, gainFactor: GainFactor,
                synResends: SynResends, finResends: FinResends, numResends: NumResends,
                minRtoMs: 500, initialRtoMs: 1000, connectTimeoutMs: 30000,
                receiveWindow: ReceiveWindow, logEnabled: LogEnabled,
                allowDynamicMtu: AllowDynamicMtu, maxPacketSize: MaxPacketSize);

            // Store under both ids so incoming replies (which may use either) find us.
            _sockets[(remote, sendId)] = conn;
            _sockets[(remote, recvId)] = conn;

            return conn;
        }

        /// <summary>
        /// Returns a newly established incoming connection if one is available.
        /// The caller (UtpPeerConnectionListener) is responsible for wrapping it
        /// into a UtpPeerConnection and raising the ConnectionReceived event.
        /// </summary>
        public UtpConnection? TryGetPendingIncoming ()
        {
            return _pendingIncoming.Count > 0 ? _pendingIncoming.Dequeue () : null;
        }

        /// <summary>
        /// Periodic driver. Should be called roughly every 50-100 ms.
        /// </summary>
        public void Tick ()
        {
            long now = Stopwatch.GetTimestamp ();

            List<UtpConnection>? toRemove = null;

            foreach (var kv in _sockets) {
                var conn = kv.Value;

                conn.Tick (now);

                if (conn.State == UtpState.Deleting || conn.State == UtpState.ErrorWait) {
                    toRemove ??= new List<UtpConnection> ();
                    toRemove.Add (conn);
                }
            }

            if (toRemove != null) {
                foreach (var c in toRemove)
                    RemoveConnection (c);
            }
        }

        void RemoveConnection (UtpConnection conn)
        {
            // Remove all keys that point to this connection
            var keysToRemove = new List<(CompactEndPoint, ushort)> ();
            foreach (var kv in _sockets) {
                if (kv.Value == conn)
                    keysToRemove.Add (kv.Key);
            }
            foreach (var k in keysToRemove)
                _sockets.Remove (k);

            conn.Dispose ();
        }

        /// <summary>
        /// Gracefully shut down all connections and detach from the transport.
        /// </summary>
        public void Dispose ()
        {
            _transport.UtpPacketReceived -= OnUtpPacketReceived;

            foreach (var conn in _sockets.Values)
                conn.Dispose ();
            _sockets.Clear ();
            _pendingIncoming.Clear ();
        }
    }
}
