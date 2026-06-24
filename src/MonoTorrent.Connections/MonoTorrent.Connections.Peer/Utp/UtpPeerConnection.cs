//
// UtpPeerConnection.cs
//
// IPeerConnection implementation backed by a uTP (BEP 29) stream.
// Follows Microsoft dispose-pattern guidance (idempotent Dispose, ObjectDisposedException
// on members after dispose): https://learn.microsoft.com/dotnet/standard/design-guidelines/dispose-pattern
//

using System;
using System.Net;
using System.Threading;

using MonoTorrent.Connections.Peer;
using ReusableTasks;

namespace MonoTorrent.Connections.Peer.Utp
{
    public sealed class UtpPeerConnection : IPeerConnection
    {
        readonly UtpConnection _impl;
        int _disposed;

        public ReadOnlyMemory<byte> AddressBytes { get; }

        public bool CanReconnect { get; }

        public bool Disposed => Volatile.Read (ref _disposed) == 1;

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
            ThrowIfDisposed ();

            // SYN is already in flight (outgoing) or the peer initiated (incoming) when this wrapper is created.
            if (_impl.State == UtpState.Connected)
                return ReusableTask.FromResult (true);

            if (_impl.HasError || _impl.State == UtpState.ErrorWait || _impl.State == UtpState.Deleting)
                return ReusableTask.FromResult (false);

            var tcs = new ReusableTaskCompletionSource<bool> ();
            var completed = 0;

            void Complete (bool success)
            {
                if (Interlocked.Exchange (ref completed, 1) != 0)
                    return;
                _impl.Connected -= OnConnected;
                tcs.SetResult (success);
            }

            void OnConnected (UtpConnection _)
                => Complete (true);

            _impl.Connected += OnConnected;

            // Race: connection may have completed between the checks above and subscription.
            if (_impl.State == UtpState.Connected) {
                Complete (true);
            } else if (_impl.HasError || _impl.State == UtpState.ErrorWait || _impl.State == UtpState.Deleting) {
                Complete (false);
            }

            return tcs.Task;
        }

        public ReusableTask<int> ReceiveAsync (Memory<byte> buffer)
        {
            ThrowIfDisposed ();
            return _impl.ReceiveAsync (buffer);
        }

        public ReusableTask<int> SendAsync (ReadOnlyMemory<byte> buffer)
        {
            ThrowIfDisposed ();
            return _impl.SendAsync (buffer);
        }

        public void Dispose ()
        {
            // Idempotent Dispose (Microsoft dispose pattern: safe to call more than once; do not throw).
            if (Interlocked.Exchange (ref _disposed, 1) == 1)
                return;

            try {
                if (_impl.State == UtpState.Connected || _impl.State == UtpState.FinSent)
                    _impl.CloseWrite ();
                else
                    _impl.Abort ();
            } catch {
                // Dispose must not throw (design guidelines).
                try { _impl.Abort (); } catch { /* ignored */ }
            }
        }

        void ThrowIfDisposed ()
        {
            if (Disposed)
                throw new ObjectDisposedException (nameof (UtpPeerConnection));
        }
    }
}
