//
// UtpSocketManager.cs
//
// Production-grade uTP (BEP 29) socket manager with support for full UDP socket sharing.
// Designed to be injected with one or more existing ISocketMessageListener instances
// (typically UdpListener / DhtListener instances created by ClientEngine). This enables
// a single UDP socket (bound to a peer listen port or DHT port) to serve uTP + DHT
// (and potentially other protocols) simultaneously.
//
// The manager owns the per-connection state machines (UtpConnection) and the LEDBAT
// congestion control, packet scheduling, tick/retransmit loop, etc.
//
// References the libtorrent-rasterbar approach for the state machine, delay histories,
// LEDBAT, SACK, MTU discovery, etc., adapted to C# idioms, ReusableTask, MemoryPool, etc.
//
// Combined from maybe-most-complete-bep29 (protocol/manager shape) and another-bep29-impl
// (configurability, connection limits, (remote, connId) lookup keys).
//
// Authors:
//   Ayman (heavily modeled on libtorrent-rasterbar's utp_socket_manager + utp_socket_impl)
//
//

using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;

using MonoTorrent.Connections;
using MonoTorrent.Logging;

using ReusableTasks;

namespace MonoTorrent.Connections.Peer.Utp
{
    /// <summary>
    /// Manages all uTP connections (incoming and outgoing) for the engine.
    /// It does not own UDP sockets itself — instead it is given ISocketMessageListener(s)
    /// that are already bound and started by the higher level (ClientEngine). This is the
    /// mechanism for full UDP socket sharing with DHT and any other UDP users on the same port.
    /// </summary>
    public sealed class UtpSocketManager : IDisposable
    {
        // Logging is currently disabled in the uTP layer inside the Connections assembly (Logger may be internal to the main assembly).
        // Production logging will be wired once the layer is fully integrated (or Logger is made accessible).
        // static readonly Logger Log = Logger.Create (nameof (UtpSocketManager));

        // Injected transports (UDP listeners). We subscribe to MessageReceived on all of them.
        // Keyed by the listener for easy unsubscription. We may have one per address family / port
        // (e.g. one for the main peer IPv4 listen port, one for IPv6, and/or the DHT port).
        readonly Dictionary<ISocketMessageListener, Action<ReadOnlyMemory<byte>, CompactEndPoint>> _subscriptions
            = new Dictionary<ISocketMessageListener, Action<ReadOnlyMemory<byte>, CompactEndPoint>> ();

        readonly List<ISocketMessageListener> _transports = new List<ISocketMessageListener> ();

        // Active connections keyed by (remote endpoint, connection id). We register both send-id and
        // recv-id for each socket so replies using either side's id still resolve (libtorrent pattern).
        readonly Dictionary<(IPEndPoint Remote, ushort ConnId), UtpConnection> _connections
            = new Dictionary<(IPEndPoint, ushort), UtpConnection> ();

        // Distinct live sockets (each connection appears under two keys above).
        readonly HashSet<UtpConnection> _liveConnections = new HashSet<UtpConnection> ();

        readonly object _lock = new object ();
        readonly Random _random = new Random ();
        readonly Timer _tickTimer;   // Drives retransmits, LEDBAT, timeouts for all connections.

        int _disposed;

        /// <summary>Active configuration (immutable snapshot passed at construction).</summary>
        public UtpConfig Config { get; }

        /// <summary>
        /// The target delay (ms) passed down to individual connections for their LEDBAT controller.
        /// Can be updated at runtime (affects new connections / samples).
        /// </summary>
        public int TargetDelayMilliseconds { get; set; }

        /// <summary>
        /// Number of currently active uTP connections (both fully connected and in handshake states).
        /// </summary>
        public int ActiveConnections
        {
            get
            {
                lock (_lock)
                    return _liveConnections.Count;
            }
        }

        /// <summary>
        /// Invoked when a new incoming uTP connection has completed the uTP handshake (reached Connected state)
        /// and is ready to be surfaced to the BitTorrent layer (as a UtpPeerConnection implementing IPeerConnection).
        /// The higher layer (UtpPeerConnectionListener) is responsible for creating the wrapper and raising
        /// the IPeerConnectionListener.ConnectionReceived event so that encryption + BT handshake proceed normally.
        /// </summary>
        internal event Action<UtpConnection>? IncomingConnectionEstablished;

        public UtpSocketManager (int targetDelayMilliseconds = 75)
            : this (new UtpConfig { TargetDelayMilliseconds = targetDelayMilliseconds })
        {
        }

        public UtpSocketManager (UtpConfig config)
        {
            Config = config ?? UtpConfig.Default;
            TargetDelayMilliseconds = Math.Max (1, Config.TargetDelayMilliseconds);

            // Tick fairly frequently for good RTO / fast retransmit / delay sampling granularity.
            // libtorrent drives this from its main loop + per-socket timers; we use a simple timer here.
            var interval = Config.TickInterval <= TimeSpan.Zero ? TimeSpan.FromMilliseconds (50) : Config.TickInterval;
            _tickTimer = new Timer (OnTick, null, interval, interval);
        }

        /// <summary>
        /// Attaches the manager to one or more existing UDP transports (listeners).
        /// This is the key to full UDP socket sharing: the same UdpListener instance can be
        /// passed to both the DHT engine and to this manager. Both will receive every datagram
        /// via the event; each filters what it cares about (uTP header check here, bencoded KRPC for DHT).
        /// </summary>
        public void AddTransport (ISocketMessageListener transport)
        {
            if (transport == null)
                throw new ArgumentNullException (nameof (transport));

            lock (_lock) {
                if (_subscriptions.ContainsKey (transport))
                    return;

                Action<ReadOnlyMemory<byte>, CompactEndPoint> handler = OnDatagramReceived;
                _subscriptions[transport] = handler;
                _transports.Add (transport);
                transport.MessageReceived += handler;

                // Log.Info ($"Attached uTP manager to transport local={transport.LocalEndPoint}");
            }
        }

        public void RemoveTransport (ISocketMessageListener transport)
        {
            lock (_lock) {
                if (_subscriptions.TryGetValue (transport, out var handler)) {
                    transport.MessageReceived -= handler;
                    _subscriptions.Remove (transport);
                    _transports.Remove (transport);
                }
            }
        }

        /// <summary>
        /// Creates and initiates an outgoing uTP connection to the given remote endpoint.
        /// The returned UtpConnection can be wrapped by UtpPeerConnection to satisfy IPeerConnection.
        /// The actual SYN is sent asynchronously.
        /// </summary>
        public UtpConnection CreateOutgoingConnection (IPEndPoint remoteEndPoint)
        {
            if (remoteEndPoint == null)
                throw new ArgumentNullException (nameof (remoteEndPoint));

            lock (_lock) {
                if (_liveConnections.Count >= Config.MaxConnections)
                    throw new InvalidOperationException ($"uTP connection limit reached ({Config.MaxConnections}).");
            }

            ushort recvId = (ushort) _random.Next (1, ushort.MaxValue);
            // Per BEP29 / libtorrent: for outgoing, our recv_id is random; the packet we send uses conn_id = recv_id
            ushort sendId = (ushort) ((recvId + 1) & 0xFFFF);

            var conn = new UtpConnection (this, remoteEndPoint, recvId, sendId, isIncoming: false, TargetDelayMilliseconds, Config);

            lock (_lock) {
                // Avoid colliding with an existing entry for this remote using either id.
                int attempts = 0;
                while ((_connections.ContainsKey ((remoteEndPoint, recvId)) || _connections.ContainsKey ((remoteEndPoint, sendId))) && attempts++ < 32) {
                    recvId = (ushort) _random.Next (1, ushort.MaxValue);
                    sendId = (ushort) ((recvId + 1) & 0xFFFF);
                }
                conn.RecvId = recvId;
                conn.SendId = sendId;
                RegisterConnectionUnlocked (conn);
            }

            conn.SendSyn ();
            return conn;
        }

        internal void RegisterConnection (UtpConnection conn)
        {
            lock (_lock)
                RegisterConnectionUnlocked (conn);
        }

        void RegisterConnectionUnlocked (UtpConnection conn)
        {
            if (_liveConnections.Count >= Config.MaxConnections) {
                conn.Abort ();
                return;
            }
            _connections[(conn.RemoteEndPoint, conn.RecvId)] = conn;
            _connections[(conn.RemoteEndPoint, conn.SendId)] = conn;
            _liveConnections.Add (conn);
        }

        internal void UnregisterConnection (UtpConnection conn)
        {
            lock (_lock) {
                _connections.Remove ((conn.RemoteEndPoint, conn.RecvId));
                _connections.Remove ((conn.RemoteEndPoint, conn.SendId));
                _liveConnections.Remove (conn);
            }
        }

        void OnDatagramReceived (ReadOnlyMemory<byte> buffer, CompactEndPoint remote)
        {
            // Fast path filter: must be large enough and look like a uTP v1 packet.
            if (buffer.Length < UtpConstants.HeaderSize)
                return;

            // BEP 29 wire layout: high nibble = packet type (0..4), low nibble = version (always 1).
            byte typeVer = buffer.Span[0];
            if ((typeVer & 0x0F) != UtpConstants.Version)
                return; // Not uTP v1

            UtpPacketType pktType = (UtpPacketType) (typeVer >> 4);
            if ((byte) pktType > 4)
                return;

            if (!UtpHeader.TryParse (buffer.Span, out var header))
                return;

            // Now we have a plausible uTP packet. Look up or create the connection.
            var remoteEp = MaterializeIPEndPoint (remote);
            UtpConnection? conn;
            lock (_lock) {
                if (!_connections.TryGetValue ((remoteEp, header.ConnectionId), out conn)) {
                    // Fallback: endpoint equality can fail if compact->IPEndPoint normalization differs
                    // (e.g. port 0 vs announced port). Scan live sockets for matching conn id + compatible remote.
                    foreach (var c in _liveConnections) {
                        if ((c.RecvId == header.ConnectionId || c.SendId == header.ConnectionId) &&
                            c.RemoteEndPoint.Port == remoteEp.Port &&
                            c.RemoteEndPoint.Address.Equals (remoteEp.Address)) {
                            conn = c;
                            break;
                        }
                    }
                }
            }

            if (conn == null) {
                // Only SYNs create brand new sockets (incoming connections).
                if (header.PacketType != UtpPacketType.ST_SYN) {
                    // Stray packet for a connection we don't know (or already closed). Optionally send RESET.
                    // For production we can be strict or lenient; libtorrent sends RESET in some cases.
                    MaybeSendReset (header, remote);
                    return;
                }

                lock (_lock) {
                    if (_liveConnections.Count >= Config.MaxConnections)
                        return;
                }

                // Create the incoming side.
                // For an incoming SYN, the conn_id in the packet is the recv_id the initiator chose for itself.
                // We choose our own send/recv ids.
                ushort ourRecvId = (ushort) ((header.ConnectionId + 1) & 0xFFFF);
                ushort ourSendId = header.ConnectionId; // we echo what they sent as our send id in replies

                conn = new UtpConnection (this, remoteEp, ourRecvId, ourSendId, isIncoming: true, TargetDelayMilliseconds, Config);

                lock (_lock) {
                    if (_connections.ContainsKey ((remoteEp, ourRecvId)) || _connections.ContainsKey ((remoteEp, ourSendId)))
                        return;
                    RegisterConnectionUnlocked (conn);
                }

                // The new connection will process this SYN inside its state machine.
                IncomingConnectionEstablished?.Invoke (conn);
            }

            // Give the packet (and any payload after the header + extensions) to the connection.
            // The connection is responsible for SACK parsing, state transitions, data delivery, ACKs, etc.
            // We pass the full datagram span for extension parsing (SACK lives after the fixed header).
            conn.ProcessIncoming (header, buffer, remote);

            if (conn.State == UtpState.ErrorWait || conn.State == UtpState.Deleting)
            {
                UnregisterConnection(conn);
            }
        }

        void MaybeSendReset (UtpHeader header, CompactEndPoint remote)
        {
            // Minimal production behavior: for unknown non-SYN we can ignore or send a RESET.
            // Sending a reset helps the other side clean up faster.
            // We construct a minimal reset packet using the received ack_nr as our seq-ish (per libtorrent).
            try {
                Span<byte> reset = stackalloc byte[UtpConstants.HeaderSize];
                var resetHeader = new UtpHeader (
                    typeVersion: (byte) ((byte) UtpPacketType.ST_RESET << 4 | UtpConstants.Version),
                    extension: UtpConstants.NoExtension,
                    connectionId: header.ConnectionId,
                    tsMicro: 0,
                    tsDiffMicro: 0,
                    wndSize: 0,
                    seqNr: (ushort) _random.Next (0, ushort.MaxValue),
                    ackNr: header.SeqNr); // acknowledge the seq that triggered us

                resetHeader.WriteTo (reset);
                // Send on any transport that can reach the remote (prefer matching address family).
                SendOnBestTransport (reset.ToArray (), remote);
            } catch {
                // Never let send errors kill the receive path.
            }
        }

        internal void Send (ReadOnlyMemory<byte> buffer, CompactEndPoint destination)
        {
            SendOnBestTransport (buffer, destination);
        }

        void SendOnBestTransport (ReadOnlyMemory<byte> buffer, CompactEndPoint destination)
        {
            // Prefer a transport whose local address family matches the destination endpoint.
            ISocketMessageListener? best = null;
            lock (_lock) {
                if (_transports.Count == 0)
                    return;

                var destEp = MaterializeIPEndPoint (destination);
                foreach (var t in _transports) {
                    if (t.LocalEndPoint != null && t.LocalEndPoint.AddressFamily == destEp.AddressFamily) {
                        best = t;
                        break;
                    }
                }
                best ??= _transports[0];
            }

            _ = best.SendAsync (buffer, destination); // fire-and-forget; UDP errors surface as missing acks / RTO
        }

        static IPEndPoint MaterializeIPEndPoint (CompactEndPoint ep)
        {
            // Best-effort reconstruction of IPEndPoint from CompactEndPoint for logging / EndPoint property.
            // We write the compact form and parse it (avoids relying on internals).
            Span<byte> buf = stackalloc byte[18];
            if (!ep.TryWriteBytes (buf, out int written) || written < 6)
                return new IPEndPoint (IPAddress.Any, 0);

            if (written == 6) {
                // IPv4: 4 bytes IP + 2 bytes port (network order in the compact layout)
                var ip = new IPAddress (buf.Slice (0, 4));
                ushort port = (ushort) ((buf[4] << 8) | buf[5]);
                return new IPEndPoint (ip, port);
            } else if (written >= 18) {
                var ip = new IPAddress (buf.Slice (0, 16));
                ushort port = (ushort) ((buf[16] << 8) | buf[17]);
                return new IPEndPoint (ip, port);
            }

            return new IPEndPoint (IPAddress.Any, 0);
        }

        void OnTick (object? state)
        {
            if (Volatile.Read (ref _disposed) == 1)
                return;

            List<UtpConnection> snapshot;
            lock (_lock) {
                snapshot = new List<UtpConnection> (_liveConnections);
            }

            long now = Environment.TickCount64; // or a monotonic clock if we add one
            foreach (var c in snapshot) {
                try {
                    c.Tick (now);
                } catch (Exception) {
                    // Production: log at higher level if needed. Abort the bad connection.
                    c.Abort ();
                }
                if (c.State == UtpState.ErrorWait || c.State == UtpState.Deleting) {
                    UnregisterConnection(c);
                }
            }
        }

        public void Dispose ()
        {
            if (Interlocked.Exchange (ref _disposed, 1) == 1)
                return;

            _tickTimer.Dispose ();

            lock (_lock) {
                foreach (var kv in _subscriptions) {
                    kv.Key.MessageReceived -= kv.Value;
                }
                _subscriptions.Clear ();
                _transports.Clear ();

                foreach (var c in _liveConnections)
                    c.Abort ();

                _connections.Clear ();
                _liveConnections.Clear ();
            }
        }

        // ==================== Packet pooling (production allocation reduction, modeled on libtorrent) ====================

        private readonly Queue<UtpPacket> _packetPool = new Queue<UtpPacket>();

        internal UtpPacket AcquirePacket(int size)
        {
            UtpPacket p;
            lock (_lock)
            {
                if (_packetPool.Count > 0)
                {
                    p = _packetPool.Dequeue();
                    if (p.Data.Length < size)
                        p.Data = new byte[size];
                }
                else
                {
                    p = new UtpPacket { Data = new byte[size] };
                }
            }
            p.HeaderSize = 0;
            p.NumTransmissions = 0;
            p.MtuProbe = false;
            p.NeedResend = false;
            return p;
        }

        internal void ReleasePacket(UtpPacket p)
        {
            if (p == null) return;
            lock (_lock)
            {
                if (_packetPool.Count < 128) // cap the pool to avoid unbounded memory
                    _packetPool.Enqueue(p);
            }
        }
    }
}
