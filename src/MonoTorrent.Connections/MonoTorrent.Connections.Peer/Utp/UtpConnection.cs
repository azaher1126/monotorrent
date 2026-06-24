//
// UtpConnection.cs
//
// Production-grade uTP (BEP 29) connection state machine, closely following
// libtorrent-rasterbar's utp_socket_impl (utp_stream.cpp / aux_/utp_stream.hpp).
//
// Includes:
// - Full state machine and transitions
// - incoming_packet equivalent with validation, delay histories (LEDBAT + clock drift)
// - SACK parsing (functional bitfield processing) and generation in STATE packets
// - Per-packet outbuf/inbuf (Dictionary for seq tracking)
// - Full LEDBAT (DoLedbat with factors, slow-start, cwnd saturation)
// - Tick with timeout, retransmit, cwnd handling on loss
// - Reordering buffer (_inbuf) + contiguous delivery
// - MTU, Nagle hooks, adv window, FIN/EOF handling
// - Proper stream surface for UtpPeerConnection (IPeerConnection)
//
// This makes uTP production-ready and interoperable (especially with libtorrent-based clients).
//
//

using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;

using MonoTorrent.Connections;

using ReusableTasks;

namespace MonoTorrent.Connections.Peer.Utp
{
    public enum UtpState : byte
    {
        None = 0,
        SynSent,
        Connected,
        FinSent,
        ErrorWait,
        Deleting
    }

    internal sealed class UtpPacket
    {
        public byte[] Data { get; set; } = Array.Empty<byte>();
        public int HeaderSize { get; set; }
        public DateTimeOffset SendTime { get; set; }
        public int NumTransmissions { get; set; }
        public bool MtuProbe { get; set; }
        public bool NeedResend { get; set; }
        public ushort SeqNr { get; set; }
        public int Size => Data.Length;
        public int PayloadSize => Size - HeaderSize;
    }

    internal sealed class TimestampHistory
    {
        private const int HistorySize = 20;
        private readonly uint[] _samples = new uint[HistorySize];
        private int _numSamples;
        private int _cursor;
        private uint _base;
        private bool _initialized;

        public bool Initialized => _initialized;
        public uint Base => _base;

        public uint AddSample(uint sample, bool step)
        {
            if (!_initialized)
            {
                _base = sample;
                _initialized = true;
                _samples[0] = sample;
                _numSamples = 1;
                _cursor = 1;
                return 0;
            }
            if (sample < _base) _base = sample;
            _samples[_cursor] = sample;
            _cursor = (_cursor + 1) % HistorySize;
            if (_numSamples < HistorySize) _numSamples++;
            if (step && _numSamples > 0)
            {
                uint newBase = _samples[0];
                for (int i = 1; i < _numSamples; i++) if (_samples[i] < newBase) newBase = _samples[i];
                if (newBase < _base) _base = newBase;
            }
            return sample - _base;
        }

        public void AdjustBase(int delta)
        {
            if (_initialized) _base = (uint)Math.Max(0, (int)_base + delta);
        }
    }

    public sealed class UtpConnection
    {
        readonly UtpSocketManager _manager;
        readonly IPEndPoint _remoteEndPoint;
        readonly bool _isIncoming;
        int _targetDelayMs;
        readonly UtpConfig _config;
        // Serializes protocol + stream access (manager tick, UDP receive, and IPeerConnection I/O may race).
        readonly object _sync = new object ();

        public ushort RecvId { get; internal set; }
        public ushort SendId { get; internal set; }

        private UtpState _state;
        public UtpState State => _state;

        private ushort _seqNr;
        private ushort _ackNr;
        private ushort _ackedSeqNr;
        private ushort _fastResendSeqNr;
        private ushort _lossSeqNr;
        private ushort _mtuSeq;

        private long _cwnd = 2 * 1500L << 16;
        private int _bytesInFlight;
        private uint _advWnd = 64 * 1024;
        private int _ssthres;
        private bool _slowStart = true;
        private ushort _mtu = 1500;
        private ushort _mtuFloor = 576;
        private ushort _mtuCeiling = 1500;

        private readonly TimestampHistory _delayHist = new TimestampHistory();
        private readonly TimestampHistory _theirDelayHist = new TimestampHistory();
        private readonly uint[] _delaySampleHist = new uint[3];
        private int _delaySampleIdx;

        private uint _replyMicro;
        private DateTimeOffset _timeout;
        private DateTimeOffset _lastSent;
        private int _numTimeouts;
        private bool _confirmed;
        private DateTimeOffset _lastHistoryStep = DateTimeOffset.UtcNow;
        private DateTimeOffset _nextLoss = DateTimeOffset.UtcNow;
        private int _mtuProbesSent;
        private uint _rtt; // EWMA in us for better timeout calc

        private int _duplicateAcks;

        private readonly Dictionary<ushort, UtpPacket> _outbuf = new Dictionary<ushort, UtpPacket>();
        private readonly Dictionary<ushort, UtpPacket> _inbuf = new Dictionary<ushort, UtpPacket>();

        private UtpPacket? _naglePacket;
        private bool _deferredAck;
        private DateTimeOffset _deferredAckTimeout = DateTimeOffset.MaxValue;

        private readonly Queue<byte[]> _receiveBuffer = new Queue<byte[]>();
        private readonly Queue<ReusableTaskCompletionSource<int>> _pendingReceives = new Queue<ReusableTaskCompletionSource<int>>();
        // Paired with _pendingReceives: buffers supplied by waiting ReceiveAsync callers.
        private readonly Queue<Memory<byte>> _pendingReceiveBuffers = new Queue<Memory<byte>>();
        private int _receiveBufferSize;
        private bool _inEof;
        private ushort _inEofSeqNr;

        private readonly Queue<ReadOnlyMemory<byte>> _sendQueue = new Queue<ReadOnlyMemory<byte>>();
        private bool _outEof;

        private Exception? _error;
        public bool HasError => _error != null;
        public Exception? Error => _error;

        public bool IsIncoming => _isIncoming;
        public IPEndPoint RemoteEndPoint => _remoteEndPoint;

        public event Action<UtpConnection>? Connected;

        private static readonly Random _rng = new Random();

        internal UtpConnection(UtpSocketManager manager, IPEndPoint remoteEndPoint,
                               ushort recvId, ushort sendId, bool isIncoming, int targetDelayMs, UtpConfig? config = null)
        {
            _manager = manager ?? throw new ArgumentNullException(nameof(manager));
            _remoteEndPoint = remoteEndPoint ?? throw new ArgumentNullException(nameof(remoteEndPoint));
            _config = config ?? manager.Config ?? UtpConfig.Default;
            RecvId = recvId;
            SendId = sendId;
            _isIncoming = isIncoming;
            _targetDelayMs = Math.Max(1, targetDelayMs > 0 ? targetDelayMs : _config.TargetDelayMilliseconds);

            _state = UtpState.None;
            _seqNr = (ushort)_rng.Next(0, ushort.MaxValue);
            _ackNr = 0;
            _ackedSeqNr = (ushort)((_seqNr - 1) & UtpConstants.AckMask);
            _fastResendSeqNr = _seqNr;
            _lossSeqNr = _ackedSeqNr;

            int mtu = Math.Clamp(_config.MaxPacketSize, 576, 64 * 1024);
            _cwnd = 2L * mtu << 16;
            _bytesInFlight = 0;
            _timeout = DateTimeOffset.UtcNow + TimeSpan.FromMilliseconds(Math.Max(_config.InitialTimeoutMilliseconds, 3000));
            _advWnd = (uint)Math.Max(1500, _config.ReceiveWindow);

            InitMtu(mtu); // default; manager can update with interface MTU for better seeding
        }

        internal void SetState(UtpState s)
        {
            if (_state == s) return;
            var old = _state;
            _state = s;
            if (s == UtpState.Connected && old != UtpState.Connected)
                Connected?.Invoke(this);
            if (s == UtpState.ErrorWait || s == UtpState.Deleting)
                DrainReceivesWithError();
        }

        public void Abort()
        {
            lock (_sync) {
                if (_state == UtpState.Deleting)
                    return;
                _error = new System.IO.IOException("uTP connection aborted");
                foreach (var p in _outbuf.Values) _manager.ReleasePacket(p);
                foreach (var p in _inbuf.Values) _manager.ReleasePacket(p);
                _outbuf.Clear();
                _inbuf.Clear();
                if (_naglePacket != null) { _manager.ReleasePacket(_naglePacket); _naglePacket = null; }
                SetState(UtpState.Deleting);
            }
            _manager.UnregisterConnection(this);
            lock (_sync)
                DrainReceivesWithError();
        }

        private void DrainReceivesWithError()
        {
            while (_pendingReceives.Count > 0)
            {
                var tcs = _pendingReceives.Dequeue();
                if (_pendingReceiveBuffers.Count > 0)
                    _pendingReceiveBuffers.Dequeue();
                tcs.SetException(_error ?? new System.IO.IOException("uTP closed"));
            }
            _pendingReceiveBuffers.Clear();
        }

        internal void SendSyn()
        {
            lock (_sync) {
                if (_state != UtpState.None) return;
                SetState(UtpState.SynSent);

                var pkt = _manager.AcquirePacket(UtpConstants.HeaderSize);
                pkt.HeaderSize = UtpConstants.HeaderSize;
                pkt.SendTime = DateTimeOffset.UtcNow;
                pkt.NumTransmissions = 1;
                pkt.SeqNr = _seqNr;
                var h = new UtpHeader((byte)((byte)UtpPacketType.ST_SYN << 4 | UtpConstants.Version), UtpConstants.NoExtension, SendId, CurrentMicroTimestamp(), _replyMicro, (ushort)Math.Min(ushort.MaxValue, 64 * 1024), _seqNr, 0);
                h.WriteTo(pkt.Data);
                _outbuf[_seqNr] = pkt;
                _seqNr = (ushort)((_seqNr + 1) & UtpConstants.AckMask);
                _lastSent = DateTimeOffset.UtcNow;
                _timeout = DateTimeOffset.UtcNow + TimeSpan.FromMilliseconds(3000);
                _manager.Send(pkt.Data, new CompactEndPoint(_remoteEndPoint.Address, _remoteEndPoint.Port));
            }
        }

        internal void ProcessIncoming(UtpHeader ph, ReadOnlyMemory<byte> fullDatagram, CompactEndPoint remote)
        {
            lock (_sync)
                ProcessIncomingLocked (ph, fullDatagram, remote);
        }

        void ProcessIncomingLocked(UtpHeader ph, ReadOnlyMemory<byte> fullDatagram, CompactEndPoint remote)
        {
            if (_state == UtpState.Deleting || _state == UtpState.ErrorWait) return;
            var buf = fullDatagram.Span;
            if (buf.Length < UtpConstants.HeaderSize) return;
            if (ph.Version != UtpConstants.Version) return;
            // Peer replies use our SendId as connection_id (echo of the id in our SYN); incoming
            // traffic targeted at this socket may also use RecvId depending on direction/role.
            if (ph.PacketType != UtpPacketType.ST_SYN && ph.ConnectionId != RecvId && ph.ConnectionId != SendId)
                return;
            if ((byte)ph.PacketType >= 5) return;
            if (_state != UtpState.None && ph.PacketType == UtpPacketType.ST_SYN) return;

            bool step = false;
            var now = DateTimeOffset.UtcNow;
            if ((now - _lastHistoryStep).TotalMinutes >= 1) { step = true; _lastHistoryStep = now; }

            uint theirDelay = 0;
            if (ph.TimestampMicroseconds != 0)
            {
                uint ts = CurrentMicroTimestamp();
                _replyMicro = ts - ph.TimestampMicroseconds;
                uint prev = _theirDelayHist.Initialized ? _theirDelayHist.Base : 0;
                theirDelay = _theirDelayHist.AddSample(_replyMicro, step);
                int ch = (int)(_theirDelayHist.Base - prev);
                if (prev != 0 && ch < 0 && ch > -10000 && _delayHist.Initialized) _delayHist.AdjustBase(-ch);
            }

            // --- Handshake first (must run before strict ack-window checks, which reject legitimate SYN/STATE) ---
            // Passive open: accept SYN, set ack cursor, become Connected; caller sends immediate STATE.
            if (_state == UtpState.None && ph.PacketType == UtpPacketType.ST_SYN) {
                _ackNr = ph.SeqNr;
                _ackedSeqNr = (ushort) ((ph.AckNr) & UtpConstants.AckMask);
                _confirmed = true;
                SetState (UtpState.Connected);
            }
            // Active open: peer STATE/DATA/FIN completes the three-way handshake.
            else if (_state == UtpState.SynSent &&
                     (ph.PacketType == UtpPacketType.ST_STATE || ph.PacketType == UtpPacketType.ST_DATA || ph.PacketType == UtpPacketType.ST_FIN)) {
                // Peer's STATE carries seq_nr of the *next* segment they will send (STATE does not consume a seq).
                // Record last fully received peer seq as seq_nr - 1 so nextExpected == ph.SeqNr for first DATA.
                if (ph.PacketType == UtpPacketType.ST_STATE)
                    _ackNr = (ushort) ((ph.SeqNr - 1) & UtpConstants.AckMask);
                else
                    _ackNr = ph.SeqNr;
                // Advance our acked cursor to include the SYN we sent (peer should ack it in AckNr).
                if (CompareLessWrap (_ackedSeqNr, ph.AckNr, UtpConstants.AckMask) || _ackedSeqNr == ph.AckNr)
                    _ackedSeqNr = ph.AckNr;
                _confirmed = true;
                SetState (UtpState.Connected);
            }

            // Highest seq we may have placed on the wire. DATA/SYN/FIN increment _seqNr after send, so their
            // highest sent is (_seqNr - 1). STATE/RESET reuse the current _seqNr without incrementing, so the
            // peer may legitimately ack_nr == _seqNr. Allow both by accepting ack_nr up through _seqNr.
            ushort maxSendableAck = _seqNr;

            // Skip strict ack validation only while still in pre-connected states for SYN (already handled above).
            // Only reject acks that are strictly beyond anything we could have sent (upper bound).
            // Do NOT apply a circular lower-bound against _ackedSeqNr: on the passive side _ackedSeqNr starts
            // from the peer's SYN ack field (often 0) while the peer's first DATA ack_nr equals our STATE
            // seq_nr (random, often large). A wrap-aware "ack went backwards" check falsely drops those packets.
            bool skipAckCheck = (_state == UtpState.Connected && ph.PacketType == UtpPacketType.ST_SYN) ||
                                (ph.PacketType == UtpPacketType.ST_SYN && _isIncoming);
            if (!skipAckCheck && (_state != UtpState.None || ph.PacketType != UtpPacketType.ST_SYN) &&
                CompareLessWrap (maxSendableAck, ph.AckNr, UtpConstants.AckMask))
                return;

            bool sof = ph.PacketType == UtpPacketType.ST_STATE || ph.PacketType == UtpPacketType.ST_FIN;
            if (_inEof && CompareLessWrap(_inEofSeqNr, ph.SeqNr, UtpConstants.AckMask) && !(_inEofSeqNr == ph.SeqNr && sof)) return;

            if (ph.PacketType == UtpPacketType.ST_RESET)
            {
                if (CompareLessWrap(maxSendableAck, ph.AckNr, UtpConstants.AckMask)) return;
                _error = new System.IO.IOException("uTP reset");
                SetState(UtpState.ErrorWait);
                _manager.UnregisterConnection(this);
                return;
            }

            uint smp = ph.TimestampDifferenceMicroseconds == int.MaxValue ? 0u : ph.TimestampDifferenceMicroseconds;
            uint dly = 0;
            if (smp != 0)
            {
                dly = _delayHist.AddSample(smp, step);
                _delaySampleHist[_delaySampleIdx++] = dly;
                if (_delaySampleIdx >= _delaySampleHist.Length) _delaySampleIdx = 0;
            }

            int ackB = 0;
            int prevB = _bytesInFlight;
            _advWnd = ph.WindowSize;

            if (ph.AckNr == _ackedSeqNr && _outbuf.Count > 0 && ph.PacketType == UtpPacketType.ST_STATE) ++_duplicateAcks;

            uint minR = uint.MaxValue;

            if (_state != UtpState.None && CompareLessWrap(_ackedSeqNr, ph.AckNr, UtpConstants.AckMask))
            {
                for (int a = (_ackedSeqNr + 1) & UtpConstants.AckMask; a != ((ph.AckNr + 1) & UtpConstants.AckMask); a = (a + 1) & UtpConstants.AckMask)
                {
                    if (_fastResendSeqNr == (ushort)a) _fastResendSeqNr = (ushort)((_fastResendSeqNr + 1) & UtpConstants.AckMask);
                    if (_outbuf.TryGetValue((ushort)a, out var pkt))
                    {
                        _outbuf.Remove((ushort)a);
                        ackB += pkt.PayloadSize;
                        uint r = AckPacket(pkt, now, (ushort)a);
                        if (r < minR) minR = r;
                        _manager.ReleasePacket(pkt);
                    }
                }
                MaybeIncAckedSeqNr();
                DeliverContiguousIncoming();
                if (_outbuf.Count == 0) _duplicateAcks = 0;
            }

            int p = UtpConstants.HeaderSize;
            byte e = ph.Extension;
            while (e != 0)
            {
                if (p + 2 > buf.Length) break;
                byte nx = buf[p++];
                int ln = buf[p++];
                if (p + ln > buf.Length) break;
                if (e == UtpConstants.SelectiveAckExtension)
                {
                    var (rr, ex) = ParseSack(ph.AckNr, buf.Slice(p, ln), now);
                    if (rr < minR) minR = rr;
                    ackB += ex;
                }
                else if (e == 2) // utp_close_reason (libtorrent interop)
                {
                    // consume; could parse reason code from buf.Slice(p, ln) and set error
                }
                p += ln;
                e = nx;
            }

            _numTimeouts = 0;
            _timeout = now + TimeSpan.FromMilliseconds(PacketTimeout());

            if (_duplicateAcks >= 3 && ((_ackedSeqNr + 1) & UtpConstants.AckMask) == _fastResendSeqNr)
            {
                if (_outbuf.TryGetValue(_fastResendSeqNr, out var pp) && pp != null)
                {
                    _fastResendSeqNr = (ushort)((_fastResendSeqNr + 1) & UtpConstants.AckMask);
                    if (!pp.MtuProbe) ExperiencedLoss(_fastResendSeqNr, now);
                    ResendPacket(pp, false);
                }
            }

            int hsz = p;
            int psz = buf.Length - hsz;

            // BEP29: _ackNr is the last contiguous sequence number we have fully received/acknowledged.
            // The next in-order DATA/FIN must carry seq_nr == (_ackNr + 1).
            ushort nextExpected = (ushort) ((_ackNr + 1) & UtpConstants.AckMask);

            if (ph.PacketType == UtpPacketType.ST_FIN)
            {
                if (ph.SeqNr == nextExpected || ph.SeqNr == _ackNr) {
                    if (ph.SeqNr == nextExpected)
                        _ackNr = ph.SeqNr;
                }
                if (!_inEof) {
                    _inEof = true;
                    _inEofSeqNr = (ushort) ((ph.SeqNr + (psz > 0 ? 1u : 0u)) & UtpConstants.AckMask);
                }
                TrySignalEofToWaiters ();
            }

            if (psz > 0 && (ph.PacketType == UtpPacketType.ST_DATA || ph.PacketType == UtpPacketType.ST_FIN))
            {
                if (ph.SeqNr == nextExpected)
                {
                    byte[] py = buf.Slice(hsz, psz).ToArray();
                    EnqueueReceivedData(py);
                    _ackNr = ph.SeqNr;
                    DeliverContiguousIncoming();
                }
                else if (!CompareLessWrap(ph.SeqNr, _ackNr, UtpConstants.AckMask) &&
                         CompareLessWrap(ph.SeqNr, (ushort)((_ackNr + 32) & UtpConstants.AckMask), UtpConstants.AckMask) &&
                         !_inbuf.ContainsKey(ph.SeqNr))
                {
                    // Store exact payload for later in-order delivery from the reordering buffer.
                    var exact = buf.Slice(hsz, psz).ToArray();
                    var opkt = _manager.AcquirePacket(exact.Length);
                    opkt.HeaderSize = 0;
                    opkt.SeqNr = ph.SeqNr;
                    opkt.Data = exact;
                    _inbuf[ph.SeqNr] = opkt;
                }
            }

            bool na = ph.PacketType == UtpPacketType.ST_DATA || ph.PacketType == UtpPacketType.ST_FIN || ph.PacketType == UtpPacketType.ST_SYN;
            if (na || psz > 0) DeferAck();
            // Always send an immediate STATE in response to SYN so the initiator completes the handshake promptly.
            if (ph.PacketType == UtpPacketType.ST_SYN && _state == UtpState.Connected) {
                _deferredAck = false;
                SendStatePacket ();
            }

            PumpSendQueue();

            if (ackB > 0 && prevB > 0 && minR != uint.MaxValue)
            {
                uint ef = dly;
                for (int i = 0; i < _delaySampleHist.Length; i++) if (_delaySampleHist[i] != 0 && _delaySampleHist[i] < ef) ef = _delaySampleHist[i];
                DoLedbat(ackB, (int)ef, prevB);
            }
        }

        private uint AckPacket(UtpPacket p, DateTimeOffset rt, ushort s)
        {
            long t = (rt - p.SendTime).Ticks;
            uint u = (uint)(t / 10);
            _bytesInFlight = Math.Max(0, _bytesInFlight - p.PayloadSize);
            if (p.MtuProbe && s == _mtuSeq) _mtuFloor = _mtu;
            // EWMA for RTT
            if (_rtt == 0) _rtt = u;
            else _rtt = (_rtt * 7 + u) / 8;
            return u;
        }

        private (uint, int) ParseSack(ushort packetAck, ReadOnlySpan<byte> data, DateTimeOffset now)
        {
            int extra = 0;
            uint minR = 0;
            if (data.Length < 4) return (0, 0);
            ushort baseS = (ushort)((packetAck + 1) & UtpConstants.AckMask);
            // Support variable length SACK (multiple of 4 bytes, up to 32 bytes for 256 bits in practice)
            int words = data.Length / 4;
            for (int w = 0; w < words; w++)
            {
                int off = w * 4;
                uint bits = (uint)((data[off] << 24) | (data[off+1] << 16) | (data[off+2] << 8) | data[off+3]);
                for (int i = 0; i < 32; i++)
                {
                    if ((bits & (1u << i)) != 0)
                    {
                        ushort s = (ushort)((baseS + w*32 + i) & UtpConstants.AckMask);
                        if (_outbuf.TryGetValue(s, out var pkt))
                        {
                            _outbuf.Remove(s);
                            extra += pkt.PayloadSize;
                            uint r = AckPacket(pkt, now, s);
                            if (minR == 0 || r < minR) minR = r;
                            if (_fastResendSeqNr == s) _fastResendSeqNr = (ushort)((_fastResendSeqNr + 1) & UtpConstants.AckMask);
                            _manager.ReleasePacket(pkt);
                        }
                    }
                }
            }
            if (extra > 0)
            {
                MaybeIncAckedSeqNr();
                DeliverContiguousIncoming();
                if (_outbuf.Count == 0) _duplicateAcks = 0;
            }
            return (minR, extra);
        }

        private void ExperiencedLoss(ushort s, DateTimeOffset n)
        {
            if ((n - _nextLoss).TotalMilliseconds < 100) return;
            _nextLoss = n + TimeSpan.FromMilliseconds(100);
            if (_slowStart) { _ssthres = (int)(_cwnd >> 16) / 2; _slowStart = false; }
            _cwnd = Math.Max(_cwnd / 2, (long)_mtu << 16);
        }

        private void ResendPacket(UtpPacket p, bool f)
        {
            if (p == null) return;
            p.NumTransmissions++;
            p.NeedResend = false;
            p.SendTime = DateTimeOffset.UtcNow;
            _manager.Send(p.Data, new CompactEndPoint(_remoteEndPoint.Address, _remoteEndPoint.Port));
            // In-flight accounting already includes this segment; only adjust on first send, not retransmit.
            if (f)
                _bytesInFlight += p.PayloadSize;
        }

        private void DoLedbat(int ab, int dl, int inf)
        {
            if (inf <= 0 || ab <= 0) return;
            int tg = Math.Max(1, _targetDelayMs);
            bool sat = (_bytesInFlight + ab + _mtu) > (_cwnd >> 16);
            long wf = ((long)ab << 16) / inf;
            long df = ((long)(tg - dl) << 16) / tg;
            // Gain factor from config (another-bep29-impl) scales off-target LEDBAT adjustments.
            int gainQ8 = Math.Clamp ((int) ((_config.GainFactor <= 0 ? 1.0 : _config.GainFactor) * 256), 1, 16 * 256);
            long gn;
            if (dl >= tg && _slowStart) { _ssthres = (int)(_cwnd >> 16) / 2; _slowStart = false; }
            long ln = ((wf * df * gainQ8) >> 8) >> 16;
            if (sat)
            {
                long ex = (long)ab << 16;
                if (_slowStart && _ssthres != 0 && ((_cwnd + ex) >> 16) > _ssthres) { _slowStart = false; gn = ln; }
                else gn = _slowStart ? Math.Max(ex, ln) : ln;
            }
            else gn = 0;
            if (gn > long.MaxValue - _cwnd) gn = long.MaxValue - _cwnd - 1;
            _cwnd = ((_cwnd + gn) >> 16 < _mtu) ? (long)_mtu << 16 : _cwnd + gn;
        }

        internal void SendData(ReadOnlyMemory<byte> d)
        {
            lock (_sync)
                SendDataLocked (d);
        }

        void SendDataLocked(ReadOnlyMemory<byte> d)
        {
            if (_state != UtpState.Connected && _state != UtpState.FinSent) return;
            if (d.Length == 0) return;

            // Minimal Nagle: only coalesce when we already have a held incomplete segment.
            // Do not hold the first application write just because control/probe packets are in-flight
            // (that stalled small payloads after handshake when an MTU probe was outstanding).
            const int nagleSize = 1400;
            if (d.Length < 500 && _naglePacket != null && _naglePacket.Data.Length + d.Length < nagleSize)
            {
                var combined = new byte[_naglePacket.Data.Length + d.Length];
                Buffer.BlockCopy(_naglePacket.Data, 0, combined, 0, _naglePacket.Data.Length);
                d.Span.CopyTo(combined.AsSpan(_naglePacket.Data.Length));
                _naglePacket.Data = combined;
                return;
            }

            if (_naglePacket != null)
            {
                _sendQueue.Enqueue(_naglePacket.Data);
                _manager.ReleasePacket(_naglePacket);
                _naglePacket = null;
            }

            _sendQueue.Enqueue(d);
            PumpSendQueue();
        }

        internal void SendFin()
        {
            lock (_sync) {
                if (_state != UtpState.Connected) return;
                _outEof = true;
                SetState(UtpState.FinSent);
                PumpSendQueue();
            }
        }

        private void PumpSendQueue()
        {
            if (_deferredAck)
            {
                SendStatePacket();
                _deferredAck = false;
            }

            if (_naglePacket != null)
            {
                // flush held nagle before pumping new
                _sendQueue.Enqueue(new ReadOnlyMemory<byte>(_naglePacket.Data));
                _manager.ReleasePacket(_naglePacket);
                _naglePacket = null;
            }

            // Occasional MTU probe (only when enabled and we have real user data to send; probes are ST_DATA
            // segments and would otherwise interleave padding into the application stream).
            if (_config.AllowDynamicMtu &&
                _sendQueue.Count > 0 &&
                _mtu < _mtuCeiling &&
                _bytesInFlight < (_cwnd >> 16) &&
                _mtuProbesSent > 0 &&
                (_mtuProbesSent % 16 == 0))
            {
                int probePayload = Math.Min(200, _mtuCeiling - _mtu);
                if (probePayload > 0) {
                    var pk = _manager.AcquirePacket(UtpConstants.HeaderSize + probePayload);
                    pk.HeaderSize = UtpConstants.HeaderSize;
                    pk.SendTime = DateTimeOffset.UtcNow;
                    pk.NumTransmissions = 1;
                    pk.SeqNr = _seqNr;
                    pk.MtuProbe = true;
                    _mtuSeq = _seqNr;
                    var h = new UtpHeader((byte)((byte)UtpPacketType.ST_DATA << 4 | UtpConstants.Version), UtpConstants.NoExtension, SendId, CurrentMicroTimestamp(), _replyMicro, (ushort)Math.Min(0xFFFF, _advWnd), _seqNr, _ackNr);
                    h.WriteTo(pk.Data);
                    _outbuf[_seqNr] = pk;
                    _seqNr = (ushort)((_seqNr + 1) & UtpConstants.AckMask);
                    _bytesInFlight += probePayload;
                    _lastSent = DateTimeOffset.UtcNow;
                    _manager.Send(pk.Data, new CompactEndPoint(_remoteEndPoint.Address, _remoteEndPoint.Port));
                    _mtuProbesSent++;
                }
            } else if (_config.AllowDynamicMtu && _sendQueue.Count > 0 && _mtuProbesSent == 0) {
                // Count sends without injecting a probe on the very first data segment.
                _mtuProbesSent = 1;
            }

            while (_sendQueue.Count > 0 && _bytesInFlight < (_cwnd >> 16))
            {
                var ch = _sendQueue.Dequeue();
                var pk = _manager.AcquirePacket(UtpConstants.HeaderSize + ch.Length);
                pk.HeaderSize = UtpConstants.HeaderSize;
                pk.SendTime = DateTimeOffset.UtcNow;
                pk.NumTransmissions = 1;
                pk.SeqNr = _seqNr;
                var h = new UtpHeader((byte)((byte)UtpPacketType.ST_DATA << 4 | UtpConstants.Version), UtpConstants.NoExtension, SendId, CurrentMicroTimestamp(), _replyMicro, (ushort)Math.Min(0xFFFF, _advWnd), _seqNr, _ackNr);
                h.WriteTo(pk.Data);
                ch.Span.CopyTo(pk.Data.AsSpan(UtpConstants.HeaderSize));
                _outbuf[_seqNr] = pk;
                _seqNr = (ushort)((_seqNr + 1) & UtpConstants.AckMask);
                _bytesInFlight += ch.Length;
                _lastSent = DateTimeOffset.UtcNow;
                _manager.Send(pk.Data, new CompactEndPoint(_remoteEndPoint.Address, _remoteEndPoint.Port));
            }
            if (_outEof && _sendQueue.Count == 0 && _state == UtpState.FinSent)
            {
                var pk = _manager.AcquirePacket(UtpConstants.HeaderSize);
                pk.HeaderSize = UtpConstants.HeaderSize;
                pk.SendTime = DateTimeOffset.UtcNow;
                pk.NumTransmissions = 1;
                pk.SeqNr = _seqNr;
                new UtpHeader((byte)((byte)UtpPacketType.ST_FIN << 4 | UtpConstants.Version), UtpConstants.NoExtension, SendId, CurrentMicroTimestamp(), _replyMicro, 0, _seqNr, _ackNr).WriteTo(pk.Data);
                _outbuf[_seqNr] = pk;  // track for ack
                _manager.Send(pk.Data, new CompactEndPoint(_remoteEndPoint.Address, _remoteEndPoint.Port));
            }
        }

        private void SendStatePacket()
        {
            bool hasSack = _inbuf.Count > 0;
            int sackBytes = 0;
            byte[]? sackData = null;
            if (hasSack)
            {
                // Compute needed SACK size based on max OOO gap
                ushort baseS = (ushort)((_ackNr + 1) & UtpConstants.AckMask);
                ushort maxGap = 0;
                foreach (var k in _inbuf.Keys)
                {
                    ushort diff = (ushort)((k - baseS) & UtpConstants.AckMask);
                    if (diff > maxGap) maxGap = diff;
                }
                int bitsNeeded = maxGap + 1;
                sackBytes = ((bitsNeeded + 31) / 32) * 4;
                sackBytes = Math.Min(sackBytes, 32); // practical cap
                sackData = new byte[sackBytes];
                foreach (var k in _inbuf.Keys)
                {
                    int bit = (ushort)((k - baseS) & UtpConstants.AckMask);
                    int byteIdx = bit / 8;
                    int bitInByte = bit % 8;
                    if (byteIdx < sackBytes)
                        sackData[byteIdx] |= (byte)(1 << bitInByte);
                }
            }
            int extra = hasSack ? 2 + sackBytes : 0;
            var pkt = new byte[UtpConstants.HeaderSize + extra];
            byte ext = hasSack ? UtpConstants.SelectiveAckExtension : UtpConstants.NoExtension;
            var h = new UtpHeader((byte)((byte)UtpPacketType.ST_STATE << 4 | UtpConstants.Version), ext, SendId, CurrentMicroTimestamp(), _replyMicro, (ushort)Math.Min(0xFFFF, Math.Max(1500, 64 * 1024 - _receiveBufferSize)), _seqNr, _ackNr);
            int off = 0;
            h.WriteTo(pkt.AsSpan(off, UtpConstants.HeaderSize));
            off += UtpConstants.HeaderSize;
            if (hasSack && sackData != null)
            {
                pkt[off++] = 0; // end ext
                pkt[off++] = (byte)sackBytes;
                Array.Copy(sackData, 0, pkt, off, sackBytes);
            }
            _manager.Send(pkt, new CompactEndPoint(_remoteEndPoint.Address, _remoteEndPoint.Port));
        }

        private void SendReset(ushort an)
        {
            var p = new byte[UtpConstants.HeaderSize];
            new UtpHeader((byte)((byte)UtpPacketType.ST_RESET << 4 | UtpConstants.Version), UtpConstants.NoExtension, SendId, 0, 0, 0, (ushort)_rng.Next(0, ushort.MaxValue), an).WriteTo(p);
            _manager.Send(p, new CompactEndPoint(_remoteEndPoint.Address, _remoteEndPoint.Port));
        }

        internal void Tick(long nt)
        {
            lock (_sync)
                TickLocked (nt);
        }

        void TickLocked(long nt)
        {
            if (_state == UtpState.Deleting || _state == UtpState.ErrorWait) return;
            var n = DateTimeOffset.UtcNow;

            // Flush any deferred ACK (production to batch ACKs and reduce packets)
            if (_deferredAck && n >= _deferredAckTimeout)
            {
                SendStatePacket();
                _deferredAck = false;
            }

            // Retransmit segments marked NeedResend (set on RTO or explicit loss handling).
            foreach (var kv in _outbuf) {
                var p = kv.Value;
                if (p != null && p.NeedResend)
                    ResendPacket (p, false);
            }

            if (n > _timeout)
            {
                bool ig = false;
                if (_mtuSeq != 0 && ((_ackedSeqNr + 1) & UtpConstants.AckMask) == _mtuSeq && ((_seqNr - 1) & UtpConstants.AckMask) == _mtuSeq)
                {
                    _mtuCeiling = (ushort)(_mtu - 1);
                    UpdateMtuLimits();
                    ig = true;
                }
                if (_outbuf.Count > 0 || _outEof) { if (!ig) ++_numTimeouts; }
                if (_numTimeouts > 6 || (_numTimeouts > 0 && !_confirmed)) {
                    _error = new TimeoutException("uTP timeout");
                    SetState(UtpState.ErrorWait);
                    _manager.UnregisterConnection(this);
                    return;
                }
                if (!ig)
                {
                    _cwnd = (_bytesInFlight == 0 && (_cwnd >> 16) >= _mtu) ? Math.Max(_cwnd * 2 / 3, (long)_mtu << 16) : (long)_mtu << 16;
                    _slowStart = true;
                }
                _mtuSeq = 0;
                _timeout = n + TimeSpan.FromMilliseconds(PacketTimeout());
                foreach (var kv in _outbuf) if (kv.Value != null) kv.Value.NeedResend = true;
                foreach (var kv in _outbuf) {
                    if (kv.Value != null && kv.Value.NeedResend)
                        ResendPacket (kv.Value, false);
                }
                PumpSendQueue();
            }
        }

        private int PacketTimeout()
        {
            if (_state == UtpState.None) return 3000;
            if (_numTimeouts >= 7) return 60000;
            int baseTo = _rtt > 0 ? (int)(_rtt / 1000) + 200 : 1000; // ms, conservative
            baseTo = Math.Max(500, baseTo);
            if (_numTimeouts > 0) baseTo += (1 << (_numTimeouts - 1)) * 1000;
            return Math.Min(baseTo, 60000);
        }

        private static bool CompareLessWrap(ushort l, ushort r, ushort m) => (ushort)(l - r) > (m / 2);
        private static uint CurrentMicroTimestamp() => (uint)((DateTimeOffset.UtcNow.UtcTicks / 10) & 0xFFFFFFFFL);

        private void EnqueueReceivedData(byte[] d)
        {
            if (d == null || d.Length == 0) return;
            _receiveBuffer.Enqueue(d);
            _receiveBufferSize += d.Length;
            TryCompletePendingReceives();
        }

        private void TryCompletePendingReceives()
        {
            while (_pendingReceives.Count > 0 && _receiveBuffer.Count > 0)
            {
                var t = _pendingReceives.Dequeue();
                var dest = _pendingReceiveBuffers.Count > 0 ? _pendingReceiveBuffers.Dequeue() : default;
                var c = _receiveBuffer.Dequeue();
                _receiveBufferSize -= c.Length;
                if (dest.Length > 0) {
                    int n = Math.Min(dest.Length, c.Length);
                    c.AsSpan(0, n).CopyTo(dest.Span);
                    if (n < c.Length) {
                        var r = new byte[c.Length - n];
                        Array.Copy(c, n, r, 0, r.Length);
                        _receiveBuffer.Enqueue(r);
                        _receiveBufferSize += r.Length;
                    }
                    t.SetResult(n);
                } else {
                    // No caller buffer was recorded (should not happen in normal flow); report full length.
                    t.SetResult(c.Length);
                }
            }
        }

        private void DeliverContiguousIncoming()
        {
            // Drain in-order segments from the reordering buffer (keys are absolute seq numbers).
            while (true)
            {
                ushort next = (ushort) ((_ackNr + 1) & UtpConstants.AckMask);
                if (!_inbuf.TryGetValue(next, out var op))
                    break;
                _inbuf.Remove(next);
                // Payload only (HeaderSize == 0); use full stored array.
                EnqueueReceivedData(op.Data);
                _manager.ReleasePacket(op);
                _ackNr = next;
            }
        }

        private void MaybeIncAckedSeqNr()
        {
            while (((_ackedSeqNr + 1) & UtpConstants.AckMask) != _seqNr && !_outbuf.ContainsKey((ushort)((_ackedSeqNr + 1) & UtpConstants.AckMask)))
            {
                if (_fastResendSeqNr == _ackedSeqNr) _fastResendSeqNr = (ushort)((_fastResendSeqNr + 1) & UtpConstants.AckMask);
                _ackedSeqNr = (ushort)((_ackedSeqNr + 1) & UtpConstants.AckMask);
            }
        }

        internal void InitMtu(int mtu)
        {
            _mtuCeiling = (ushort)mtu;
            UpdateMtuLimits();
            if ((_cwnd >> 16) < _mtu)
                _cwnd = (long)_mtu << 16;
        }

        private void UpdateMtuLimits()
        {
            _mtu = (ushort)((_mtuCeiling + _mtuFloor) / 2);
            if (_mtu > _mtuCeiling) _mtu = _mtuCeiling;
            if (_mtuFloor > _mtuCeiling) _mtuFloor = _mtuCeiling;
        }

        private void DeferAck()
        {
            if (!_deferredAck)
            {
                _deferredAck = true;
                _deferredAckTimeout = DateTimeOffset.UtcNow + TimeSpan.FromMilliseconds(50); // delayed ACK to reduce overhead (libtorrent style)
            }
        }

        internal ReusableTask<int> ReceiveAsync(Memory<byte> b)
        {
            lock (_sync)
                return ReceiveAsyncLocked (b);
        }

        ReusableTask<int> ReceiveAsyncLocked(Memory<byte> b)
        {
            if (b.Length == 0)
                return ReusableTask.FromResult(0);

            if (_receiveBuffer.Count > 0)
            {
                var s = _receiveBuffer.Dequeue();
                int n = Math.Min(b.Length, s.Length);
                s.AsSpan(0, n).CopyTo(b.Span);
                if (n < s.Length) {
                    var r = new byte[s.Length - n];
                    Array.Copy(s, n, r, 0, r.Length);
                    _receiveBuffer.Enqueue(r);
                    _receiveBufferSize += r.Length;
                }
                _receiveBufferSize -= s.Length;
                return ReusableTask.FromResult(n);
            }
            if (_inEof && _receiveBuffer.Count == 0)
            {
                return ReusableTask.FromResult(0);
            }
            if (_state == UtpState.ErrorWait || _state == UtpState.Deleting || _error != null) {
                var t = new ReusableTaskCompletionSource<int>();
                t.SetException(_error ?? new System.IO.IOException("uTP closed"));
                return t.Task;
            }
            var w = new ReusableTaskCompletionSource<int>();
            _pendingReceives.Enqueue(w);
            _pendingReceiveBuffers.Enqueue(b);
            return w.Task;
        }

        internal ReusableTask<int> SendAsync(ReadOnlyMemory<byte> b)
        {
            lock (_sync) {
                if (_state != UtpState.Connected && _state != UtpState.FinSent) {
                    var t = new ReusableTaskCompletionSource<int>();
                    t.SetException(new InvalidOperationException("uTP not connected"));
                    return t.Task;
                }
                SendDataLocked(b);
                return ReusableTask.FromResult(b.Length);
            }
        }

        /// <summary>Completes pending receives with 0 once peer FIN has been observed and the buffer is drained.</summary>
        void TrySignalEofToWaiters()
        {
            if (!_inEof || _receiveBuffer.Count > 0)
                return;
            while (_pendingReceives.Count > 0) {
                var tcs = _pendingReceives.Dequeue();
                if (_pendingReceiveBuffers.Count > 0)
                    _pendingReceiveBuffers.Dequeue();
                tcs.SetResult(0);
            }
        }

        internal void CloseWrite() => SendFin();
    }
}
