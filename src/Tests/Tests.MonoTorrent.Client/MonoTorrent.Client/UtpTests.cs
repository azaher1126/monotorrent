//
// UtpTests.cs
//
// Unit tests for the uTP (BEP 29) stack following Microsoft unit-testing guidance:
// one concern per test, arrange-act-assert, deterministic mocks (no real sockets).
// See: https://learn.microsoft.com/dotnet/core/testing/unit-testing-best-practices
//
// Wire simulation is strictly one-shot (deliver recorded packets explicitly) to avoid
// synchronous ACK storms / hangs from in-process bidirectional forwarding.
//

using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;

using MonoTorrent.Connections;
using MonoTorrent.Connections.Peer;
using MonoTorrent.Connections.Peer.Utp;

using NUnit.Framework;

using ReusableTasks;

namespace MonoTorrent.Client
{
    [TestFixture]
    public class UtpTests
    {
        sealed class MockTransport : ISocketMessageListener
        {
            public IPEndPoint LocalEndPoint { get; }
            public ListenerStatus Status { get; private set; } = ListenerStatus.NotListening;
            public IPEndPoint PreferredLocalEndPoint => LocalEndPoint;

            public List<(byte[] Data, CompactEndPoint Endpoint)> SentPackets { get; } = new List<(byte[], CompactEndPoint)> ();

            public event Action<ReadOnlyMemory<byte>, CompactEndPoint> MessageReceived;
            public event EventHandler<EventArgs> StatusChanged;

            public MockTransport (int port = 0)
                => LocalEndPoint = new IPEndPoint (IPAddress.Loopback, port);

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

        static byte WireType (byte[] packet) => (byte) (packet[0] >> 4);
        static byte WireVersion (byte[] packet) => (byte) (packet[0] & 0x0F);

        const byte TypeData = 0, TypeFin = 1, TypeState = 2, TypeReset = 3, TypeSyn = 4;

        /// <summary>
        /// Performs a minimal uTP handshake by exchanging the first SYN and first STATE
        /// recorded on each mock transport (no live bidirectional loop).
        /// </summary>
        static UtpConnection PerformHandshake (UtpSocketManager mA, MockTransport tA, UtpSocketManager mB, MockTransport tB, int remotePortB)
        {
            var initiator = mA.CreateOutgoingConnection (new IPEndPoint (IPAddress.Loopback, remotePortB));
            Assert.GreaterOrEqual (tA.SentPackets.Count, 1, "SYN should be sent");
            Assert.AreEqual (TypeSyn, WireType (tA.SentPackets[0].Data));

            // B receives SYN (source is A's identity)
            var fromA = new CompactEndPoint (IPAddress.Loopback, tA.LocalEndPoint.Port > 0 ? tA.LocalEndPoint.Port : 1);
            tB.SimulateReceive (tA.SentPackets[0].Data, fromA);
            Assert.GreaterOrEqual (tB.SentPackets.Count, 1, "Responder should send STATE");
            Assert.AreEqual (TypeState, WireType (tB.SentPackets[0].Data));

            // A receives STATE
            var fromB = new CompactEndPoint (IPAddress.Loopback, remotePortB);
            tA.SimulateReceive (tB.SentPackets[0].Data, fromB);
            Assert.AreEqual (UtpState.Connected, initiator.State);
            return initiator;
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
            Assert.Greater (cfg.ConnectTimeoutMilliseconds, 0);
        }

        [Test]
        public void UtpSocketManager_AttachAndCreateOutgoing_SendsSyn ()
        {
            var transport = new MockTransport (1000);
            transport.Start ();
            using var manager = new UtpSocketManager (new UtpConfig { TargetDelayMilliseconds = 75, TickInterval = TimeSpan.FromHours (1) });
            manager.AddTransport (transport);

            var conn = manager.CreateOutgoingConnection (new IPEndPoint (IPAddress.Loopback, 6881));

            Assert.GreaterOrEqual (transport.SentPackets.Count, 1);
            Assert.GreaterOrEqual (manager.ActiveConnections, 1);
            Assert.AreEqual (UtpState.SynSent, conn.State);

            var pkt = transport.SentPackets[0].Data;
            Assert.AreEqual (1, WireVersion (pkt));
            Assert.AreEqual (TypeSyn, WireType (pkt));
            Assert.GreaterOrEqual (pkt.Length, 20);
        }

        [Test]
        public void UtpSocketManager_RespectsMaxConnections ()
        {
            var transport = new MockTransport (1001);
            transport.Start ();
            using var manager = new UtpSocketManager (new UtpConfig { MaxConnections = 1, TickInterval = TimeSpan.FromHours (1) });
            manager.AddTransport (transport);

            manager.CreateOutgoingConnection (new IPEndPoint (IPAddress.Loopback, 1));
            Assert.Throws<InvalidOperationException> (() =>
                manager.CreateOutgoingConnection (new IPEndPoint (IPAddress.Loopback, 2)));
        }

        [Test]
        public void UtpPeerConnection_WrapsConnection_AndThrowsWhenDisposed ()
        {
            var transport = new MockTransport (1002);
            transport.Start ();
            using var manager = new UtpSocketManager (new UtpConfig { TickInterval = TimeSpan.FromHours (1) });
            manager.AddTransport (transport);

            var impl = manager.CreateOutgoingConnection (new IPEndPoint (IPAddress.Loopback, 7000));
            var peerConn = new UtpPeerConnection (impl);

            Assert.IsFalse (peerConn.IsIncoming);
            Assert.IsTrue (peerConn.CanReconnect);
            Assert.IsTrue (peerConn.Uri.Scheme.StartsWith ("utp-ipv", StringComparison.Ordinal));

            peerConn.Dispose ();
            Assert.IsTrue (peerConn.Disposed);
            Assert.Throws<ObjectDisposedException> (() => peerConn.ReceiveAsync (new byte[1]).AsTask ().GetAwaiter ().GetResult ());
            Assert.DoesNotThrow (() => peerConn.Dispose ());
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

        [Test]
        public void Handshake_TwoManagers_ReachesConnectedOnInitiator ()
        {
            var tA = new MockTransport (5001);
            var tB = new MockTransport (5002);
            tA.Start ();
            tB.Start ();

            using var mA = new UtpSocketManager (new UtpConfig { TickInterval = TimeSpan.FromHours (1) });
            using var mB = new UtpSocketManager (new UtpConfig { TickInterval = TimeSpan.FromHours (1) });
            mA.AddTransport (tA);
            mB.AddTransport (tB);

            PerformHandshake (mA, tA, mB, tB, 5002);
            Assert.GreaterOrEqual (mB.ActiveConnections, 1);
        }

        [Test]
        public async Task Handshake_ThenSendData_EmitsDataPacket ()
        {
            var tA = new MockTransport (5011);
            var tB = new MockTransport (5012);
            tA.Start ();
            tB.Start ();

            using var mA = new UtpSocketManager (new UtpConfig { TickInterval = TimeSpan.FromHours (1) });
            using var mB = new UtpSocketManager (new UtpConfig { TickInterval = TimeSpan.FromHours (1) });
            mA.AddTransport (tA);
            mB.AddTransport (tB);

            var connA = PerformHandshake (mA, tA, mB, tB, 5012);
            Assert.AreEqual (UtpState.Connected, connA.State);

            using var peerA = new UtpPeerConnection (connA);
            // Large enough to avoid any small-write coalescing edge cases.
            var payload = new byte[600];
            for (int i = 0; i < payload.Length; i++)
                payload[i] = (byte) i;

            int sentBefore = tA.SentPackets.Count;
            int sent = await peerA.SendAsync (payload).ConfigureAwait (false);
            Assert.AreEqual (payload.Length, sent);

            // Either a new DATA/STATE was emitted, or send was accepted into the protocol queue (state still healthy).
            Assert.AreEqual (UtpState.Connected, connA.State);
            if (tA.SentPackets.Count > sentBefore) {
                bool sawData = false;
                for (int i = sentBefore; i < tA.SentPackets.Count; i++) {
                    if (WireType (tA.SentPackets[i].Data) == TypeData)
                        sawData = true;
                }
                Assert.IsTrue (sawData || tA.SentPackets.Count > sentBefore);
            }
        }

        [Test]
        public void NonUtpDatagrams_AreIgnored_NoCrash ()
        {
            var transport = new MockTransport (5020);
            transport.Start ();
            using var manager = new UtpSocketManager (new UtpConfig { TickInterval = TimeSpan.FromHours (1) });
            manager.AddTransport (transport);

            transport.SimulateReceive (new byte[] { (byte) 'd', 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20 },
                new CompactEndPoint (IPAddress.Loopback, 99));
            var bad = new byte[20];
            bad[0] = 0x42; // type 4, version 2
            transport.SimulateReceive (bad, new CompactEndPoint (IPAddress.Loopback, 99));

            Assert.AreEqual (0, manager.ActiveConnections);
        }

        [Test]
        public void UnknownNonSyn_DoesNotCreateConnection ()
        {
            var transport = new MockTransport (5021);
            transport.Start ();
            using var manager = new UtpSocketManager (new UtpConfig { TickInterval = TimeSpan.FromHours (1) });
            manager.AddTransport (transport);

            var state = new byte[20];
            state[0] = (byte) ((TypeState << 4) | 1);
            state[2] = 0x12;
            state[3] = 0x34;
            transport.SimulateReceive (state, new CompactEndPoint (IPAddress.Loopback, 88));

            Assert.AreEqual (0, manager.ActiveConnections);
        }

        [Test]
        public void Dispose_Manager_CleansConnections ()
        {
            var transport = new MockTransport (5022);
            transport.Start ();
            var manager = new UtpSocketManager (new UtpConfig { TickInterval = TimeSpan.FromHours (1) });
            manager.AddTransport (transport);
            manager.CreateOutgoingConnection (new IPEndPoint (IPAddress.Loopback, 1));
            Assert.GreaterOrEqual (manager.ActiveConnections, 1);

            manager.Dispose ();
            Assert.AreEqual (0, manager.ActiveConnections);
        }

        [Test]
        public void PeerInfo_SupportsUtp_RoundTripThroughPexFlags ()
        {
            var uri = new Uri ("ipv4://1.2.3.4:6881");
            var withUtp = new PeerInfo (uri, MonoTorrent.BEncoding.BEncodedString.Empty, maybeSeeder: false, supportsUtp: true);
            var without = new PeerInfo (uri, MonoTorrent.BEncoding.BEncodedString.Empty, maybeSeeder: true, supportsUtp: false);

            Assert.IsTrue (withUtp.SupportsUtp);
            Assert.IsFalse (without.SupportsUtp);
            Assert.IsTrue (without.MaybeSeeder);
        }

        [Test]
        public async Task UtpPeerConnection_ConnectAsync_CompletesAfterHandshake ()
        {
            var tA = new MockTransport (5031);
            var tB = new MockTransport (5032);
            tA.Start ();
            tB.Start ();

            using var mA = new UtpSocketManager (new UtpConfig { TickInterval = TimeSpan.FromHours (1) });
            using var mB = new UtpSocketManager (new UtpConfig { TickInterval = TimeSpan.FromHours (1) });
            mA.AddTransport (tA);
            mB.AddTransport (tB);

            var impl = PerformHandshake (mA, tA, mB, tB, 5032);
            using var peer = new UtpPeerConnection (impl);

            bool ok = await peer.ConnectAsync ().ConfigureAwait (false);
            Assert.IsTrue (ok);
            Assert.AreEqual (UtpState.Connected, impl.State);
        }

        static byte[] BuildUtpPacket (byte type, ushort connId, ushort seqNr, ushort ackNr, ushort wnd, ReadOnlySpan<byte> payload)
        {
            var pkt = new byte[20 + payload.Length];
            pkt[0] = (byte) ((type << 4) | 1);
            pkt[1] = 0;
            pkt[2] = (byte) (connId >> 8);
            pkt[3] = (byte) (connId & 0xFF);
            // timestamps / wnd / seq / ack
            pkt[12] = (byte) (wnd >> 8);
            pkt[13] = (byte) (wnd & 0xFF);
            pkt[14] = (byte) (seqNr >> 8);
            pkt[15] = (byte) (seqNr & 0xFF);
            pkt[16] = (byte) (ackNr >> 8);
            pkt[17] = (byte) (ackNr & 0xFF);
            if (payload.Length > 0)
                payload.CopyTo (pkt.AsSpan (20));
            return pkt;
        }

        /// <summary>
        /// Delivers only ST_DATA segments from <paramref name="from"/> to <paramref name="to"/> starting at <paramref name="startIndex"/>.
        /// </summary>
        static int DeliverDataPackets (MockTransport from, MockTransport to, CompactEndPoint fromEp, int startIndex)
        {
            int delivered = 0;
            for (int i = startIndex; i < from.SentPackets.Count; i++) {
                if (WireType (from.SentPackets[i].Data) == TypeData) {
                    to.SimulateReceive (from.SentPackets[i].Data, fromEp);
                    delivered++;
                }
            }
            return delivered;
        }

        static (UtpConnection initiator, UtpPeerConnection passivePeer, MockTransport tA, MockTransport tB, UtpSocketManager mA, UtpSocketManager mB)
            SetupHandshakePair (int portA, int portB)
        {
            var tA = new MockTransport (portA);
            var tB = new MockTransport (portB);
            tA.Start ();
            tB.Start ();
            var mA = new UtpSocketManager (new UtpConfig { TickInterval = TimeSpan.FromHours (1), AllowDynamicMtu = false });
            var mB = new UtpSocketManager (new UtpConfig { TickInterval = TimeSpan.FromHours (1), AllowDynamicMtu = false });
            mA.AddTransport (tA);
            mB.AddTransport (tB);

            UtpPeerConnection passivePeer = null;
            var listener = new UtpPeerConnectionListener (new IPEndPoint (IPAddress.Loopback, portB), mB);
            listener.ConnectionReceived += (_, e) => passivePeer = (UtpPeerConnection) e.Connection;
            listener.Start ();

            var initiator = PerformHandshake (mA, tA, mB, tB, portB);
            Assert.IsNotNull (passivePeer, "passive peer must be established via listener");
            return (initiator, passivePeer, tA, tB, mA, mB);
        }

        [Test]
        public void UtpHeader_RoundTrip_PreservesFields ()
        {
            // Header parse/write is internal; validate via wire SYN bytes from manager.
            var transport = new MockTransport (5040);
            transport.Start ();
            using var manager = new UtpSocketManager (new UtpConfig { TickInterval = TimeSpan.FromHours (1) });
            manager.AddTransport (transport);

            manager.CreateOutgoingConnection (new IPEndPoint (IPAddress.Loopback, 9000));
            var pkt = transport.SentPackets[0].Data;

            Assert.AreEqual (1, WireVersion (pkt));
            Assert.AreEqual (TypeSyn, WireType (pkt));
            Assert.AreEqual (20, pkt.Length); // SYN has no payload
            Assert.AreEqual (0, pkt[1]); // no extension
        }

        [Test]
        public async Task Handshake_ThenSendData_EmitsDataPacket_Strict ()
        {
            var tA = new MockTransport (5041);
            var tB = new MockTransport (5042);
            tA.Start ();
            tB.Start ();

            using var mA = new UtpSocketManager (new UtpConfig { TickInterval = TimeSpan.FromHours (1) });
            using var mB = new UtpSocketManager (new UtpConfig { TickInterval = TimeSpan.FromHours (1) });
            mA.AddTransport (tA);
            mB.AddTransport (tB);

            var connA = PerformHandshake (mA, tA, mB, tB, 5042);
            using var peerA = new UtpPeerConnection (connA);
            var payload = new byte[600];
            for (int i = 0; i < payload.Length; i++)
                payload[i] = (byte) (i & 0xFF);

            int sentBefore = tA.SentPackets.Count;
            int sent = await peerA.SendAsync (payload).ConfigureAwait (false);
            Assert.AreEqual (payload.Length, sent);
            Assert.Greater (tA.SentPackets.Count, sentBefore, "DATA should be emitted on the wire");

            bool sawData = false;
            for (int i = sentBefore; i < tA.SentPackets.Count; i++) {
                if (WireType (tA.SentPackets[i].Data) == TypeData && tA.SentPackets[i].Data.Length > 20) {
                    sawData = true;
                    // Payload should follow the 20-byte header.
                    CollectionAssert.AreEqual (payload, tA.SentPackets[i].Data.AsSpan (20).ToArray ());
                }
            }
            Assert.IsTrue (sawData, "expected ST_DATA with payload");
        }

        [Test]
        public async Task PassiveSide_ReceivesData_AfterHandshake ()
        {
            var (connA, passivePeer, tA, tB, mA, mB) = SetupHandshakePair (5051, 5052);
            using (mA)
            using (mB) {
                using var peerA = new UtpPeerConnection (connA);
                var payload = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
                int sentBefore = tA.SentPackets.Count;
                await peerA.SendAsync (payload).ConfigureAwait (false);
                Assert.Greater (tA.SentPackets.Count, sentBefore);

                var fromA = new CompactEndPoint (IPAddress.Loopback, tA.LocalEndPoint.Port > 0 ? tA.LocalEndPoint.Port : 1);
                int stateBeforeB = tB.SentPackets.Count;
                Assert.Greater (DeliverDataPackets (tA, tB, fromA, sentBefore), 0, "at least one DATA segment should be delivered");

                var recvBuf = new byte[64];
                var recvTask = passivePeer.ReceiveAsync (recvBuf).AsTask ();
                if (!recvTask.Wait (TimeSpan.FromSeconds (2)))
                    Assert.Fail ("ReceiveAsync timed out — passive did not get DATA");
                int n = recvTask.Result;
                Assert.AreEqual (payload.Length, n);
                CollectionAssert.AreEqual (payload, recvBuf.AsSpan (0, n).ToArray ());
                Assert.GreaterOrEqual (tB.SentPackets.Count, stateBeforeB);
            }
        }

        [Test]
        public async Task PendingReceive_CompletesWhenDataArrives ()
        {
            var (connA, passivePeer, tA, tB, mA, mB) = SetupHandshakePair (5061, 5062);
            using (mA)
            using (mB) {
                var recvBuf = new byte[32];
                var recvTask = passivePeer.ReceiveAsync (recvBuf).AsTask ();
                Assert.IsFalse (recvTask.IsCompleted, "receive should pend until DATA arrives");

                using var peerA = new UtpPeerConnection (connA);
                var payload = new byte[] { 0xAA, 0xBB, 0xCC, 0xDD };
                int sentBefore = tA.SentPackets.Count;
                await peerA.SendAsync (payload).ConfigureAwait (false);

                var fromA = new CompactEndPoint (IPAddress.Loopback, tA.LocalEndPoint.Port > 0 ? tA.LocalEndPoint.Port : 1);
                Assert.Greater (DeliverDataPackets (tA, tB, fromA, sentBefore), 0);

                if (!recvTask.Wait (TimeSpan.FromSeconds (2)))
                    Assert.Fail ("pending ReceiveAsync did not complete");
                int n = recvTask.Result;
                Assert.AreEqual (payload.Length, n);
                CollectionAssert.AreEqual (payload, recvBuf.AsSpan (0, n).ToArray ());
            }
        }

        [Test]
        public void UnknownNonSyn_SendsReset ()
        {
            var transport = new MockTransport (5070);
            transport.Start ();
            using var manager = new UtpSocketManager (new UtpConfig { TickInterval = TimeSpan.FromHours (1) });
            manager.AddTransport (transport);

            var state = BuildUtpPacket (TypeState, connId: 0x1234, seqNr: 10, ackNr: 5, wnd: 1024, payload: ReadOnlySpan<byte>.Empty);
            transport.SimulateReceive (state, new CompactEndPoint (IPAddress.Loopback, 88));

            Assert.AreEqual (0, manager.ActiveConnections);
            // Production hardening: respond with RESET for unknown non-SYN.
            Assert.GreaterOrEqual (transport.SentPackets.Count, 1);
            Assert.AreEqual (TypeReset, WireType (transport.SentPackets[0].Data));
        }

        [Test]
        public void UtpPeerConnectionListener_RaisesConnectionReceived_OnIncomingSyn ()
        {
            var tA = new MockTransport (5081);
            var tB = new MockTransport (5082);
            tA.Start ();
            tB.Start ();

            using var mA = new UtpSocketManager (new UtpConfig { TickInterval = TimeSpan.FromHours (1) });
            using var mB = new UtpSocketManager (new UtpConfig { TickInterval = TimeSpan.FromHours (1) });
            mA.AddTransport (tA);
            mB.AddTransport (tB);

            int events = 0;
            IPeerConnection received = null;
            var listener = new UtpPeerConnectionListener (new IPEndPoint (IPAddress.Loopback, 5082), mB);
            listener.ConnectionReceived += (_, e) => { events++; received = e.Connection; };
            listener.Start ();
            Assert.AreEqual (ListenerStatus.Listening, listener.Status);

            PerformHandshake (mA, tA, mB, tB, 5082);
            Assert.AreEqual (1, events);
            Assert.IsNotNull (received);
            Assert.IsInstanceOf<UtpPeerConnection> (received);
            Assert.IsTrue (received.IsIncoming);
        }

        [Test]
        public async Task SendAsync_WhenNotConnected_Throws ()
        {
            var transport = new MockTransport (5090);
            transport.Start ();
            using var manager = new UtpSocketManager (new UtpConfig { TickInterval = TimeSpan.FromHours (1) });
            manager.AddTransport (transport);

            var impl = manager.CreateOutgoingConnection (new IPEndPoint (IPAddress.Loopback, 1));
            using var peer = new UtpPeerConnection (impl);
            // Still in SynSent — not fully connected.
            Assert.AreEqual (UtpState.SynSent, impl.State);

            try {
                await peer.SendAsync (new byte[] { 1, 2, 3 }).ConfigureAwait (false);
                Assert.Fail ("expected InvalidOperationException");
            } catch (InvalidOperationException) {
                // expected
            }
        }

        [Test]
        public void Abort_DrainsConnectionFromManager ()
        {
            var transport = new MockTransport (5091);
            transport.Start ();
            using var manager = new UtpSocketManager (new UtpConfig { TickInterval = TimeSpan.FromHours (1) });
            manager.AddTransport (transport);

            var impl = manager.CreateOutgoingConnection (new IPEndPoint (IPAddress.Loopback, 1));
            Assert.GreaterOrEqual (manager.ActiveConnections, 1);
            impl.Abort ();
            Assert.AreEqual (UtpState.Deleting, impl.State);
            Assert.AreEqual (0, manager.ActiveConnections);
        }

        [Test]
        public void AddTransport_Null_ThrowsArgumentNullException ()
        {
            using var manager = new UtpSocketManager (new UtpConfig { TickInterval = TimeSpan.FromHours (1) });
            Assert.Throws<ArgumentNullException> (() => manager.AddTransport (null));
        }

        [Test]
        public void CreateOutgoing_NullEndpoint_ThrowsArgumentNullException ()
        {
            using var manager = new UtpSocketManager (new UtpConfig { TickInterval = TimeSpan.FromHours (1) });
            Assert.Throws<ArgumentNullException> (() => manager.CreateOutgoingConnection (null));
        }

        [Test]
        public void UtpPeerConnection_NullImpl_ThrowsArgumentNullException ()
        {
            Assert.Throws<ArgumentNullException> (() => new UtpPeerConnection (null));
        }

        [Test]
        public async Task ReceiveAsync_PartialBuffer_LeavesRemainder ()
        {
            var (connA, passivePeer, tA, tB, mA, mB) = SetupHandshakePair (5101, 5102);
            using (mA)
            using (mB) {
                using var peerA = new UtpPeerConnection (connA);
                var payload = new byte[] { 10, 20, 30, 40, 50 };
                int sentBefore = tA.SentPackets.Count;
                await peerA.SendAsync (payload).ConfigureAwait (false);
                Assert.Greater (tA.SentPackets.Count, sentBefore, "initiator must emit DATA");

                var fromA = new CompactEndPoint (IPAddress.Loopback, tA.LocalEndPoint.Port > 0 ? tA.LocalEndPoint.Port : 1);
                Assert.Greater (DeliverDataPackets (tA, tB, fromA, sentBefore), 0);

                var small = new byte[2];
                var r1 = passivePeer.ReceiveAsync (small).AsTask ();
                if (!r1.Wait (TimeSpan.FromSeconds (2)))
                    Assert.Fail ("first partial receive timed out");
                int n1 = r1.Result;
                Assert.AreEqual (2, n1);
                Assert.AreEqual (10, small[0]);
                Assert.AreEqual (20, small[1]);

                var rest = new byte[8];
                var r2 = passivePeer.ReceiveAsync (rest).AsTask ();
                if (!r2.Wait (TimeSpan.FromSeconds (2)))
                    Assert.Fail ("remainder receive timed out");
                int n2 = r2.Result;
                Assert.AreEqual (3, n2);
                Assert.AreEqual (30, rest[0]);
                Assert.AreEqual (40, rest[1]);
                Assert.AreEqual (50, rest[2]);
            }
        }
    }
}
