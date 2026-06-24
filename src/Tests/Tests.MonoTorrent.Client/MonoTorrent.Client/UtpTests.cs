//
// UtpTests.cs
//
// Public-surface unit tests for the uTP (BEP 29) stack.
// Uses a mock ISocketMessageListener so no real UDP sockets are required.
//

using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;

using MonoTorrent.Connections;
using MonoTorrent.Connections.Peer.Utp;

using NUnit.Framework;

using ReusableTasks;

namespace MonoTorrent.Client
{
    [TestFixture]
    public class UtpTests
    {
        class MockTransport : ISocketMessageListener
        {
            public IPEndPoint LocalEndPoint { get; } = new IPEndPoint (IPAddress.Loopback, 0);
            public ListenerStatus Status { get; private set; } = ListenerStatus.NotListening;
            public IPEndPoint PreferredLocalEndPoint => LocalEndPoint;

            public List<(byte[] Data, CompactEndPoint Endpoint)> SentPackets { get; } = new ();

            public event Action<ReadOnlyMemory<byte>, CompactEndPoint>? MessageReceived;
            public event EventHandler<EventArgs>? StatusChanged;

            public void Start ()
            {
                Status = ListenerStatus.Listening;
                StatusChanged?.Invoke (this, EventArgs.Empty);
            }

            public void Stop ()
            {
                Status = ListenerStatus.NotListening;
                StatusChanged?.Invoke (this, EventArgs.Empty);
            }

            public ReusableTask SendAsync (ReadOnlyMemory<byte> buffer, CompactEndPoint endpoint)
            {
                SentPackets.Add ((buffer.ToArray (), endpoint));
                return ReusableTask.CompletedTask;
            }

            public void SimulateReceive (ReadOnlyMemory<byte> data, CompactEndPoint fromEp)
                => MessageReceived?.Invoke (data, fromEp);
        }

        [Test]
        public void UtpConfig_Defaults_AreSensible ()
        {
            var cfg = UtpConfig.Default;
            Assert.AreEqual (75, cfg.TargetDelayMilliseconds);
            Assert.AreEqual (1.0, cfg.GainFactor);
            Assert.Greater (cfg.ReceiveWindow, 0);
            Assert.Greater (cfg.MaxPacketSize, 0);
            Assert.Greater (cfg.MaxConnections, 0);
        }

        [Test]
        public void UtpSocketManager_AttachAndCreateOutgoing_SendsSyn ()
        {
            var transport = new MockTransport ();
            transport.Start ();
            using var manager = new UtpSocketManager (new UtpConfig { TargetDelayMilliseconds = 75 });
            manager.AddTransport (transport);

            var remote = new IPEndPoint (IPAddress.Loopback, 6881);
            var conn = manager.CreateOutgoingConnection (remote);

            Assert.GreaterOrEqual (transport.SentPackets.Count, 1);
            Assert.GreaterOrEqual (manager.ActiveConnections, 1);
            Assert.AreEqual (UtpState.SynSent, conn.State);

            // First outbound packet: BEP 29 encodes type in the high nibble, version (1) in the low nibble.
            byte typeVer = transport.SentPackets[0].Data[0];
            Assert.AreEqual (1, typeVer & 0x0F);           // version
            Assert.LessOrEqual (typeVer >> 4, 4);          // type 0..4
            Assert.GreaterOrEqual (transport.SentPackets[0].Data.Length, 20);
        }

        [Test]
        public void UtpSocketManager_RespectsMaxConnections ()
        {
            var transport = new MockTransport ();
            transport.Start ();
            using var manager = new UtpSocketManager (new UtpConfig { MaxConnections = 1 });
            manager.AddTransport (transport);

            manager.CreateOutgoingConnection (new IPEndPoint (IPAddress.Loopback, 1));
            Assert.Throws<InvalidOperationException> (() =>
                manager.CreateOutgoingConnection (new IPEndPoint (IPAddress.Loopback, 2)));
        }

        [Test]
        public void UtpPeerConnection_WrapsConnection ()
        {
            var transport = new MockTransport ();
            transport.Start ();
            using var manager = new UtpSocketManager ();
            manager.AddTransport (transport);

            var impl = manager.CreateOutgoingConnection (new IPEndPoint (IPAddress.Loopback, 7000));
            using var peerConn = new UtpPeerConnection (impl);

            Assert.IsFalse (peerConn.IsIncoming);
            Assert.IsTrue (peerConn.CanReconnect);
            Assert.IsTrue (peerConn.Uri.Scheme.StartsWith ("utp-ipv", StringComparison.Ordinal));
        }

        [Test]
        public void EngineSettings_CreateUtpConfig_MapsFields ()
        {
            var settings = new EngineSettingsBuilder {
                EnableUtp = true,
                UtpTargetDelayMilliseconds = 100,
                UtpGainFactor = 1.5,
                UtpReceiveWindow = 512 * 1024,
                UtpMaxPacketSize = 1200,
                UtpMaxConnections = 50,
            }.ToSettings ();

            var cfg = settings.CreateUtpConfig ();
            Assert.AreEqual (100, cfg.TargetDelayMilliseconds);
            Assert.AreEqual (1.5, cfg.GainFactor);
            Assert.AreEqual (512 * 1024, cfg.ReceiveWindow);
            Assert.AreEqual (1200, cfg.MaxPacketSize);
            Assert.AreEqual (50, cfg.MaxConnections);
        }
    }
}
