//
// UtpPeerConnection.cs
//
// Implementation of IPeerConnection over uTP (BEP 29).
// Wraps a UtpConnection (from the uTP stack) and provides the async
// Connect/Receive/Send surface expected by the rest of MonoTorrent
// (ConnectionManager, PeerIO, NetworkIO, encryption, etc.).
//
// This allows the entire BitTorrent protocol, encryption, rate limiting,
// etc. to run unchanged on top of uTP exactly as they do on TCP.
//

using System;
using System.Net;
using System.Threading;

using MonoTorrent;
using MonoTorrent.Connections.Utp;
using ReusableTasks;

namespace MonoTorrent.Connections.Peer
{
    public sealed class UtpPeerConnection : IPeerConnection
    {
        public ReadOnlyMemory<byte> AddressBytes { get; }

        public bool CanReconnect => !IsIncoming;

        private readonly CancellationTokenSource _cancellation = new CancellationTokenSource();

        public bool Disposed { get; private set; }

        public IPEndPoint EndPoint { get; }

        public bool IsIncoming { get; }

        private readonly UtpConnection _utp;

        public Uri Uri { get; }

        // For async waiting
        private ReusableTaskCompletionSource<bool>? _connectTcs;
        private ReusableTaskCompletionSource<int>? _receiveTcs;
        private Memory<byte> _pendingReceiveBuffer;

        private ReusableTaskCompletionSource<bool>? _sendTcs;
        private ReadOnlyMemory<byte> _pendingSendBuffer;

        public UtpPeerConnection (UtpConnection utpConnection, bool isIncoming)
        {
            _utp = utpConnection ?? throw new ArgumentNullException (nameof (utpConnection));
            IsIncoming = isIncoming;

            EndPoint = new IPEndPoint (new IPAddress (utpConnection.Remote.Address), utpConnection.Remote.Port);
            AddressBytes = EndPoint.Address.GetAddressBytes ();

            var scheme = EndPoint.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork ? "ipv4" : "ipv6";
            Uri = new Uri ($"{scheme}://{EndPoint}");

            // Hook events for async completion
            _utp.Connected += OnUtpConnected;
            _utp.DataReady += OnUtpDataReady;
            _utp.CanWriteMore += OnUtpCanWriteMore;
            _utp.ConnectionClosed += OnUtpClosed;

            // If already connected (e.g. incoming that just completed handshake), complete any pending
            if (_utp.IsConnected && _connectTcs != null) {
                _connectTcs.SetResult (true);
                _connectTcs = null;
            }
        }

        private void OnUtpConnected ()
        {
            var tcs = _connectTcs;
            if (tcs != null) {
                _connectTcs = null;
                tcs.SetResult (true);
            }
        }

        private void OnUtpDataReady ()
        {
            var tcs = _receiveTcs;
            if (tcs != null && _pendingReceiveBuffer.Length > 0) {
                int read = _utp.Read (_pendingReceiveBuffer);
                _pendingReceiveBuffer = default;
                _receiveTcs = null;
                tcs.SetResult (read);
            }
        }

        private void OnUtpCanWriteMore ()
        {
            var tcs = _sendTcs;
            if (tcs != null && _pendingSendBuffer.Length > 0) {
                bool sent = _utp.TryQueueSend (_pendingSendBuffer);
                if (sent) {
                    _pendingSendBuffer = default;
                    _sendTcs = null;
                    tcs.SetResult (true);
                }
            }
        }

        private void OnUtpClosed (Exception? error)
        {
            var connectTcs = _connectTcs;
            if (connectTcs != null) {
                _connectTcs = null;
                connectTcs.SetException (error ?? new System.IO.IOException ("uTP connection closed"));
            }

            var recvTcs = _receiveTcs;
            if (recvTcs != null) {
                _receiveTcs = null;
                _pendingReceiveBuffer = default;
                recvTcs.SetResult (0);  // 0 bytes -> closed, NetworkIO will throw
            }

            var sendTcs = _sendTcs;
            if (sendTcs != null) {
                _sendTcs = null;
                _pendingSendBuffer = default;
                sendTcs.SetResult (false);
            }
        }

        public async ReusableTask<bool> ConnectAsync ()
        {
            if (_utp.IsConnected)
                return true;

            if (_connectTcs != null)
                return await _connectTcs.Task.ConfigureAwait (false);

            _connectTcs = new ReusableTaskCompletionSource<bool> ();

            // The uTP connection may already be in progress (outgoing SynSent)
            // or waiting for remote (incoming). The Connected event will fire when ready.
            return await _connectTcs.Task.ConfigureAwait (false);
        }

        public async ReusableTask<int> ReceiveAsync (Memory<byte> buffer)
        {
            if (Disposed)
                throw new ObjectDisposedException (nameof (UtpPeerConnection));

            int bytes = _utp.Read (buffer);
            if (bytes > 0)
                return bytes;

            if (_utp.IsClosingOrClosed) {
                return 0;  // NetworkIO will turn 0-byte receive into ConnectionClosedException
            }

            if (_receiveTcs != null)
                throw new InvalidOperationException ("Concurrent receive on uTP connection");

            _pendingReceiveBuffer = buffer;
            _receiveTcs = new ReusableTaskCompletionSource<int> ();

            return await _receiveTcs.Task.ConfigureAwait (false);
        }

        public async ReusableTask<int> SendAsync (ReadOnlyMemory<byte> buffer)
        {
            if (Disposed)
                throw new ObjectDisposedException (nameof (UtpPeerConnection));

            if (buffer.Length == 0)
                return 0;

            if (_utp.TryQueueSend (buffer))
                return buffer.Length;

            if (_utp.IsClosingOrClosed) {
                return 0;  // NetworkIO will turn 0-byte send into ConnectionClosedException
            }

            if (_sendTcs != null)
                throw new InvalidOperationException ("Concurrent send on uTP connection");

            _pendingSendBuffer = buffer;
            _sendTcs = new ReusableTaskCompletionSource<bool> ();

            await _sendTcs.Task.ConfigureAwait (false);

            // After wake, the data should have been accepted (or connection died)
            if (_utp.TryQueueSend (buffer))
                return buffer.Length;

            return 0;
        }

        public void Dispose ()
        {
            if (Disposed)
                return;

            Disposed = true;
            _cancellation.Cancel ();

            _utp.Connected -= OnUtpConnected;
            _utp.DataReady -= OnUtpDataReady;
            _utp.CanWriteMore -= OnUtpCanWriteMore;
            _utp.ConnectionClosed -= OnUtpClosed;

            if (!_utp.IsClosingOrClosed)
                _utp.Close();
            _utp.Dispose ();

            // Complete any pending operations
            _connectTcs?.SetException (new ObjectDisposedException (nameof (UtpPeerConnection)));
            _receiveTcs?.SetException (new ObjectDisposedException (nameof (UtpPeerConnection)));
            _sendTcs?.SetException (new ObjectDisposedException (nameof (UtpPeerConnection)));

            _connectTcs = null;
            _receiveTcs = null;
            _sendTcs = null;
        }
    }
}
