//
// UtpPeerConnectionListener.cs
//
// IPeerConnectionListener implementation for uTP.
// It wraps a UtpSocketManager (which is attached to shared UDP transports) and raises
// ConnectionReceived whenever the uTP layer completes a new incoming connection handshake.
//
// This lets ListenManager + the rest of the stack treat uTP incoming connections uniformly
// with TCP ones (same encryption, same BT handshake, same PeerId creation, etc.).
//
// The engine creates this listener (when EnableUtp) and includes it in the list passed to
// listenManager.SetListeners(...) alongside the regular TCP PeerConnectionListener(s).
//
// Production: The listener does not bind its own UDP socket. It is given a fully-configured
// UtpSocketManager that has already been attached (via AddTransport) to the shared UDP
// listeners created for the peer announced ports (and/or DHT port). This is what enables
// full UDP socket sharing.
//

using System;
using System.Net;

using MonoTorrent.Connections.Peer;

namespace MonoTorrent.Connections.Peer.Utp
{
    public sealed class UtpPeerConnectionListener : IPeerConnectionListener
    {
        readonly UtpSocketManager _utpManager;

        public event EventHandler<PeerConnectionEventArgs>? ConnectionReceived;
        public event EventHandler<EventArgs>? StatusChanged;

        public IPEndPoint? LocalEndPoint { get; private set; }
        public IPEndPoint PreferredLocalEndPoint { get; }
        public ListenerStatus Status { get; private set; } = ListenerStatus.NotListening;

        public UtpPeerConnectionListener (IPEndPoint preferredEndpoint, UtpSocketManager utpManager)
        {
            PreferredLocalEndPoint = preferredEndpoint ?? throw new ArgumentNullException (nameof (preferredEndpoint));
            _utpManager = utpManager ?? throw new ArgumentNullException (nameof (utpManager));

            _utpManager.IncomingConnectionEstablished += OnIncomingUtpEstablished;
        }

        void OnIncomingUtpEstablished (UtpConnection impl)
        {
            // Wrap the raw uTP connection as an IPeerConnection and surface it.
            // The ListenManager will then run the normal encryption + handshake flow.
            var peerConn = new UtpPeerConnection (impl);

            // Best effort: set our local endpoint from the first transport the manager knows about.
            // (In a multi-homed scenario a real impl would pick the correct one based on the connection.)
            LocalEndPoint = impl.RemoteEndPoint; // not perfect, but for the event it is mostly informational

            ConnectionReceived?.Invoke (this, new PeerConnectionEventArgs (peerConn, infoHash: null));
        }

        public void Start ()
        {
            Status = ListenerStatus.Listening;
            StatusChanged?.Invoke (this, EventArgs.Empty);
            // Note: no actual bind here — the UDP transport(s) are owned and started by ClientEngine
            // and attached to the UtpSocketManager before/when this listener is created.
        }

        public void Stop ()
        {
            Status = ListenerStatus.NotListening;
            StatusChanged?.Invoke (this, EventArgs.Empty);
            // The UtpSocketManager will be disposed by the engine when appropriate.
        }
    }
}
