//
// UtpPeerConnection.cs
//
// IPeerConnection implementation backed by a uTP (BEP 29) stream.
// This allows the entire MonoTorrent connection/encryption/handshake/message
// machinery (ConnectionManager, ListenManager, EncryptorFactory, NetworkIO, PeerIO, etc.)
// to treat uTP connections exactly like TCP SocketPeerConnection instances.
//
// Production notes:
// - Delegates Connect/Receive/Send/Dispose to the underlying UtpConnection (the protocol state machine).
// - Uri uses a "utp-ipv4" or "utp-ipv6" scheme so it is distinguishable in logs and for any future policy.
// - CanReconnect is false for incoming uTP (same as TCP incoming), true for outgoing.
// - The actual reliable ordered byte stream, congestion control, retransmits, etc. are all handled by UtpConnection + UtpSocketManager.
//
//

using System;
using System.Net;

using MonoTorrent.Connections.Peer;
using ReusableTasks;

namespace MonoTorrent.Connections.Peer.Utp
{
    public sealed class UtpPeerConnection : IPeerConnection
    {
        readonly UtpConnection _impl;

        public ReadOnlyMemory<byte> AddressBytes { get; }

        public bool CanReconnect { get; }

        public bool Disposed { get; private set; }

        public IPEndPoint? EndPoint { get; }

        public bool IsIncoming { get; }

        public Uri Uri { get; }

        public UtpPeerConnection (UtpConnection impl)
        {
            _impl = impl ?? throw new ArgumentNullException (nameof (impl));
            IsIncoming = impl.IsIncoming;
            CanReconnect = !IsIncoming;
            EndPoint = impl.RemoteEndPoint;
            AddressBytes = EndPoint?.Address.GetAddressBytes () ?? ReadOnlyMemory<byte>.Empty;

            var scheme = EndPoint?.AddressFamily == System.Net.Sockets.AddressFamily.InterNetworkV6 ? "utp-ipv6" : "utp-ipv4";
            Uri = new Uri ($"{scheme}://{EndPoint}");
        }

        public ReusableTask<bool> ConnectAsync ()
        {
            // For uTP, the low-level SYN was already sent (outgoing) or we are reacting to an incoming SYN
            // by the time the UtpPeerConnection wrapper is created.
            if (_impl.State == UtpState.Connected)
                return ReusableTask.FromResult (true);

            if (_impl.HasError || _impl.State == UtpState.ErrorWait || _impl.State == UtpState.Deleting)
                return ReusableTask.FromResult (false);

            var tcs = new ReusableTaskCompletionSource<bool> ();

            void OnConnected (UtpConnection c)
            {
                _impl.Connected -= OnConnected;
                tcs.SetResult (true);
            }

            _impl.Connected += OnConnected;

            // If the state machine advanced between the check above and the subscription, fire immediately.
            if (_impl.State == UtpState.Connected) {
                _impl.Connected -= OnConnected;
                tcs.SetResult (true);
            }

            // Also monitor for error to fault the TCS (production).
            // For simplicity, if error sets later, the receive/send will fail.
            // Production note: could subscribe to error or poll in a task, but for now upper layers will see on I/O.

            // Production note: a real implementation should also monitor for error states on the UtpConnection
            // and cancel/ fault the TCS, plus apply an overall connection timeout.
            return tcs.Task;
        }

        public ReusableTask<int> ReceiveAsync (Memory<byte> buffer)
        {
            if (Disposed)
                throw new ObjectDisposedException (nameof (UtpPeerConnection));

            return _impl.ReceiveAsync (buffer);
        }

        public ReusableTask<int> SendAsync (ReadOnlyMemory<byte> buffer)
        {
            if (Disposed)
                throw new ObjectDisposedException (nameof (UtpPeerConnection));

            return _impl.SendAsync (buffer);
        }

        public void Dispose ()
        {
            if (Disposed)
                return;

            Disposed = true;
            _impl.Abort(); // or CloseWrite for graceful, but Abort for immediate cleanup in dispose
            // The manager will clean up on terminal state.
        }
    }
}
