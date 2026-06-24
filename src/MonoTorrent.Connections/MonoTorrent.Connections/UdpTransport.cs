//
// UdpTransport.cs
//
// Provides a single UDP socket that can be shared by multiple protocols
// (DHT + uTP + future) via demultiplexing. This ensures only one UDP
// port is required for the engine.
//
// The receive loop classifies incoming datagrams:
//   - Likely uTP packets (BEP 29) are delivered via the internal UtpPacketReceived event (exclusive).
//   - All other packets are delivered via the public MessageReceived event (for DHT etc.).
//
// This implements the "single UDP port with demux from the start" requirement.
//

using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;

using ReusableTasks;

namespace MonoTorrent.Connections
{
    /// <summary>
    /// A concrete, shareable UDP transport that owns a single datagram socket.
    /// Multiple protocols (DHT, uTP, etc.) can receive packets from the same
    /// underlying socket. Packets are demultiplexed based on their content.
    /// </summary>
    public sealed class UdpTransport : SocketListener, ISocketMessageListener, MonoTorrent.Connections.Dht.IDhtListener
    {
        public event Action<ReadOnlyMemory<byte>, CompactEndPoint>? MessageReceived;

        // uTP (BEP29) packets are delivered here exclusively when detected.
        // The UtpManager (when implemented) will subscribe to this.
        internal event Action<ReadOnlyMemory<byte>, CompactEndPoint>? UtpPacketReceived;

        Socket? Client { get; set; }
        SocketAddress? sendAddress;
        SocketAddress? receiveAddress;

        public UdpTransport (IPEndPoint endpoint)
            : base (endpoint)
        {
        }

        public async ReusableTask SendAsync (ReadOnlyMemory<byte> buffer, CompactEndPoint endpoint)
        {
            if (Status == ListenerStatus.PortNotFree)
                throw new InvalidOperationException ($"The transport could not bind to {LocalEndPoint}. Choose a new listening endpoint.");

            if (Status == ListenerStatus.NotListening || Client == null)
                throw new InvalidOperationException ("You must invoke Start before sending or receiving with this transport.");

            if (!endpoint.TryWriteBytes (sendAddress!))
                throw new InvalidOperationException ("Couldn't write compact endpoint to socketaddress");

            await Client.SendToAsync (buffer, SocketFlags.None, sendAddress!).ConfigureAwait (false);
        }

        protected override void Start (CancellationToken token)
        {
            base.Start (token);

            sendAddress = new SocketAddress (PreferredLocalEndPoint.AddressFamily);
            receiveAddress = new SocketAddress (PreferredLocalEndPoint.AddressFamily);

            var socket = new Socket (
                PreferredLocalEndPoint.AddressFamily,
                SocketType.Dgram,
                ProtocolType.Udp);

            socket.Bind (PreferredLocalEndPoint);

            Client = socket;
            LocalEndPoint = (IPEndPoint?) socket.LocalEndPoint;

            token.Register (() => {
                Client?.Dispose ();
                Client = null;
            });

            ReceiveAsync (Client, token);
        }

        /// <summary>
        /// Public helper to start the transport (used by engine wiring for the shared UDP socket).
        /// </summary>
        public void StartTransport (CancellationToken token) => Start (token);

        async void ReceiveAsync (Socket client, CancellationToken token)
        {
            Memory<byte> buffer = new byte[8 * 1024];

            while (!token.IsCancellationRequested && receiveAddress is not null) {
                try {
                    var bytesReceived = await client.ReceiveFromAsync (
                        buffer,
                        SocketFlags.None,
                        receiveAddress).ConfigureAwait (false);

                    if (bytesReceived == 0)
                        continue;

                    ReadOnlyMemory<byte> packet = buffer.Slice (0, bytesReceived);
                    var endPoint = new CompactEndPoint (receiveAddress);

                    if (!token.IsCancellationRequested) {
                        if (IsLikelyUtpPacket (packet.Span)) {
                            // Deliver exclusively to uTP handler(s). Do not wake DHT etc.
                            // We copy to detach from the reusable receive buffer (matches original UdpListener behavior).
                            var copy = packet.ToArray ();
                            UtpPacketReceived?.Invoke (copy, endPoint);
                        } else {
                            var copy = packet.ToArray ();
                            MessageReceived?.Invoke (copy, endPoint);
                        }
                    }
                } catch (SocketException ex) {
                    // 10054 (WSAECONNRESET) - keep receiving to clear error states on UDP.
                    if (ex.ErrorCode == 10054)
                        continue;
                } catch {
                    // Swallow other errors to keep the receive loop alive (matches original behavior).
                }
            }
        }

        static bool IsLikelyUtpPacket (ReadOnlySpan<byte> data)
        {
            // uTP header (BEP 29):
            // byte 0: (type << 4) | version   where version must be 1, type in [0..4]
            if (data.Length < 4)
                return false;

            int typeVer = data[0];
            int version = typeVer & 0x0F;
            int type = typeVer >> 4;

            return version == 1 && (uint)type <= 4; // ST_DATA..ST_SYN
        }
    }
}
