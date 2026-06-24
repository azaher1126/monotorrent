//
// UtpPeerConnectionListener.cs
//
// Listens for incoming uTP connections via a UtpManager and raises
// ConnectionReceived (IPeerConnectionListener) so that the existing
// ListenManager / ConnectionManager / handshake path works exactly
// the same as for TCP.
//

using System;
using System.Net;
using System.Threading;

using MonoTorrent.Connections;
using MonoTorrent.Connections.Utp;

namespace MonoTorrent.Connections.Peer
{
    public sealed class UtpPeerConnectionListener : SocketListener, IPeerConnectionListener
    {
        public event EventHandler<PeerConnectionEventArgs>? ConnectionReceived;

        private readonly UtpManager _utpManager;

        public UtpPeerConnectionListener (UtpManager utpManager, IPEndPoint endpoint)
            : base (endpoint)
        {
            _utpManager = utpManager ?? throw new ArgumentNullException (nameof (utpManager));

            _utpManager.NewIncomingConnection += OnNewIncomingUtpConnection;
        }

        private void OnNewIncomingUtpConnection (UtpConnection utpConn)
        {
            try {
                var connection = new UtpPeerConnection (utpConn, isIncoming: true);
                ConnectionReceived?.Invoke (this, new PeerConnectionEventArgs (connection, null));
            } catch {
                utpConn.Dispose ();
            }
        }

        protected override void Start (CancellationToken token)
        {
            base.Start (token);

            // The actual UDP socket is owned by the UdpTransport passed to the UtpManager.
            // We just record the local endpoint from the transport if available.
            LocalEndPoint = _utpManager.GetType()
                .GetProperty ("Transport", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)?
                .GetValue (_utpManager) is UdpTransport t ? t.LocalEndPoint : PreferredLocalEndPoint;

            // The transport should already be started by the engine / wiring code.
            token.Register (() => {
                // Manager lifecycle is managed by the engine
            });
        }
    }
}
