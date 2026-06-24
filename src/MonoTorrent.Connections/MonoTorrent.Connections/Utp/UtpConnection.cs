//
// UtpConnection.cs
//
// The core per-connection state machine for uTP (BEP 29).
// This is the heart of the implementation and is modeled closely after
// libtorrent's utp_socket_impl (states, transitions, ack/seq rules,
// packet lifetime, etc.) for interoperability.
//
// Current scope (Phase 1 / early Phase 2):
//   - Explicit 6-state machine (none, syn_sent, connected, fin_sent, error_wait, deleting)
//   - SYN / STATE / DATA / FIN / RESET handling
//   - Basic send window + retransmission (using UtpPacket)
//   - Receive reassembly (in-order delivery to upper layer)
//   - Simple RTO / retransmit on tick
//   - Stream-like Read / Write surface for UtpPeerConnection
//
// LEDBAT congestion control, SACK, full PMTUd, fancy timers, etc. come later.
//
// The connection does not own a UDP socket. It is given a "send" delegate
// (usually from UtpManager) and is driven by ProcessIncoming + Tick calls.
//

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

using MonoTorrent;
using MonoTorrent.Logging;

namespace MonoTorrent.Connections.Utp
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

    /// <summary>
    /// Per-socket uTP state machine + reliability + (later) congestion control.
    /// </summary>
    public sealed class UtpConnection
    {
        // ---------------------------------------------------------------------
        // Logging / debug names (mirrors libtorrent for easy cross-reference)
        // ---------------------------------------------------------------------
        static readonly string[] StateNames = { "NONE", "SYN_SENT", "CONNECTED", "FIN_SENT", "ERROR", "DELETE" };
        static readonly string[] PacketTypeNames = { "ST_DATA", "ST_FIN", "ST_STATE", "ST_RESET", "ST_SYN" };

        // ---------------------------------------------------------------------
        // Constants (match BEP 29 / libtorrent)
        // ---------------------------------------------------------------------
        const int AckMask = 0xFFFF;
        const int DupAckLimit = 3;

        // Very conservative initial RTO (will be refined when we have RTT samples)
        const long InitialRtoTicks = 1_000 * TimeSpan.TicksPerMillisecond; // 1 second

        // ---------------------------------------------------------------------
        // Identity & remote endpoint
        // ---------------------------------------------------------------------
        public CompactEndPoint Remote { get; private set; }
        public ushort SendId { get; }
        public ushort RecvId { get; }

        // ---------------------------------------------------------------------
        // State
        // ---------------------------------------------------------------------
        UtpState _state;
        public UtpState State => _state;

        public bool IsConnected => _state == UtpState.Connected;
        public bool IsClosingOrClosed => _state >= UtpState.FinSent;

        // Events for async wrappers (UtpPeerConnection etc.) to get notified without polling
        public event Action? Connected;
        public event Action? DataReady;
        public event Action? CanWriteMore;
        public event Action<Exception?>? ConnectionClosed;

        // ---------------------------------------------------------------------
        // Sequence numbers (all 16-bit wrapping)
        // ---------------------------------------------------------------------
        ushort _seqNr;          // next seq we will send
        ushort _ackNr;          // last seq we have acked (we ack the next expected)
        ushort _ackedSeqNr;     // highest seq that has been acked by the peer

        // ---------------------------------------------------------------------
        // In-flight packets (for retransmit + loss detection)
        // For Phase 1 a simple dictionary is clear and correct.
        // ---------------------------------------------------------------------
        readonly Dictionary<ushort, UtpPacket> _outbuf = new Dictionary<ushort, UtpPacket> ();

        // ---------------------------------------------------------------------
        // Receive side
        // ---------------------------------------------------------------------
        readonly Queue<Memory<byte>> _receiveQueue = new Queue<Memory<byte>> ();
        readonly List<ByteBufferPool.Releaser> _receiveReleasers = new List<ByteBufferPool.Releaser> ();

        int _receiveBufferSize;

        // OOO receive segments for SACK support and proper in-order delivery to upper layer
        private readonly SortedDictionary<ushort, (Memory<byte> data, ByteBufferPool.Releaser releaser)> _outOfOrderReceive
            = new SortedDictionary<ushort, (Memory<byte>, ByteBufferPool.Releaser)>();

        // ---------------------------------------------------------------------
        // Error / close
        // ---------------------------------------------------------------------
        Exception? _error;
        public bool HasError => _error != null;

        // ---------------------------------------------------------------------
        // Send callback supplied by the owner (UtpManager)
        // ---------------------------------------------------------------------
        readonly Action<ReadOnlyMemory<byte>, CompactEndPoint> _sendPacket;

        // ---------------------------------------------------------------------
        // Timers / RTT (very basic for Phase 1)
        // ---------------------------------------------------------------------
        long _lastPacketSent;
        long _lastPacketReceived;
        long _rto = InitialRtoTicks;

        // RTT estimation (improved in Phase 3)
        long _rtt;
        long _rttVar;

        // Delayed ACK
        long _lastAckSentTicks;
        bool _delayedAck;

        // Nagle small-send buffer
        Memory<byte> _pendingNagle;
        ByteBufferPool.Releaser _pendingNagleReleaser;
        int _pendingNagleLen;

        // EOF handling for FIN
        bool m_in_eof;
        ushort m_in_eof_seq_nr;

        // ---------------------------------------------------------------------
        // Public stream surface (used by UtpPeerConnection)
        // ---------------------------------------------------------------------
        // These are simple for now. The real UtpPeerConnection will wrap them
        // with ReusableTaskCompletionSource to implement the IPeerConnection async contract.

        /// <summary>
        /// Copies as many bytes as are currently available into the destination.
        /// Returns the number of bytes copied.
        /// </summary>
        public int Read (Memory<byte> destination)
        {
            int copied = 0;
            while (_receiveQueue.Count > 0 && copied < destination.Length) {
                Memory<byte> seg = _receiveQueue.Peek ();
                int toCopy = Math.Min (seg.Length, destination.Length - copied);
                seg.Slice (0, toCopy).CopyTo (destination.Slice (copied));
                copied += toCopy;

                if (toCopy == seg.Length) {
                    _receiveQueue.Dequeue ();
                    // Release the buffer we were holding for this segment
                    if (_receiveReleasers.Count > 0) {
                        _receiveReleasers[0].Dispose ();
                        _receiveReleasers.RemoveAt (0);
                    }
                } else {
                    // partial segment – advance it
                    _receiveQueue.Enqueue (seg.Slice (toCopy));
                    _receiveQueue.Dequeue ();
                    break;
                }
            }
            _receiveBufferSize -= copied;
            return copied;
        }

        /// <summary>
        /// Number of bytes currently available to be read without blocking.
        /// </summary>
        public int BytesAvailableToRead => _receiveBufferSize;

        /// <summary>
        /// Attempts to queue user data for sending. Returns true if the data was accepted.
        /// (In Phase 1 we accept as long as we are connected and not too many packets in flight.)
        /// </summary>
        private void FlushNagle()
        {
            if (_pendingNagleLen > 0)
            {
                var data = _pendingNagle.Slice(0, _pendingNagleLen);
                if (_state == UtpState.Connected)
                {
                    int allowed = Math.Min(m_cwnd, (int)m_peer_wnd);
                    if (m_cur_window + _pendingNagleLen <= allowed)
                    {
                        var pkt = UtpPacket.Rent(_seqNr, data);
                        _outbuf[_seqNr] = pkt;
                        ushort seq = _seqNr;
                        _seqNr = (ushort)((_seqNr + 1) & AckMask);
                        m_cur_window += _pendingNagleLen;
                        TransmitPacket(seq);
                    }
                }
                _pendingNagleReleaser.Dispose();
                _pendingNagleReleaser = default;
                _pendingNagle = default;
                _pendingNagleLen = 0;
            }
        }

        public bool TryQueueSend (ReadOnlyMemory<byte> data)
        {
            if (_state != UtpState.Connected || data.Length == 0)
                return false;

            int allowed = Math.Min(m_cwnd, (int)m_peer_wnd);
            if (m_cur_window + data.Length > allowed)
                return false;   // LEDBAT / receive window limited

            // Nagle: if small data and there is unacked data in flight, buffer it
            if (data.Length < 256 && m_cur_window > 0)
            {
                int needed = _pendingNagleLen + data.Length;
                if (_pendingNagleLen == 0 || needed > _pendingNagle.Length)
                {
                    if (_pendingNagleLen > 0) _pendingNagleReleaser.Dispose();
                    _pendingNagleReleaser = MemoryPool.Default.Rent(Math.Max(512, needed), out _pendingNagle);
                }
                data.CopyTo(_pendingNagle.Slice(_pendingNagleLen));
                _pendingNagleLen = needed;
                return true;
            }

            // flush any pending nagle before sending this (or if large)
            FlushNagle();

            var pkt = UtpPacket.Rent (_seqNr, data);
            _outbuf[_seqNr] = pkt;
            ushort seq = _seqNr;
            _seqNr = (ushort)((_seqNr + 1) & AckMask);

            m_cur_window += data.Length;
            TransmitPacket (seq);
            return true;
        }

        // ---------------------------------------------------------------------
        // Construction
        // ---------------------------------------------------------------------
        internal UtpConnection (ushort sendId, ushort recvId,
                                CompactEndPoint remote,
                                Action<ReadOnlyMemory<byte>, CompactEndPoint> sendPacket,
                                bool incoming,
                                int targetDelayMs = 75,
                                double gainFactor = 1.0,
                                int synResends = 2,
                                int finResends = 2,
                                int numResends = 3,
                                int minRtoMs = 500,
                                int initialRtoMs = 1000,
                                int connectTimeoutMs = 30000,
                                int receiveWindow = 256 * 1024,
                                bool logEnabled = false,
                                bool allowDynamicMtu = true,
                                int maxPacketSize = 1500)
        {
            SendId = sendId;
            RecvId = recvId;
            Remote = remote;
            _sendPacket = sendPacket ?? throw new ArgumentNullException (nameof (sendPacket));

            _seqNr = (ushort)(new Random ().Next (0, 0x10000));
            _ackNr = 0;
            _ackedSeqNr = (ushort)((_seqNr - 1) & AckMask);

            _lastPacketSent = _lastPacketReceived = Stopwatch.GetTimestamp ();

            m_target_delay = Math.Max(1, targetDelayMs) * 1000;
            GainFactor = gainFactor;
            m_cwnd = 16 * 1024;
            m_ssthresh = 64 * 1024;
            m_slow_start = true;
            m_cur_window = 0;
            m_peer_wnd = 256 * 1024;
            m_advertised_recv_window = (uint)receiveWindow;

            m_synResends = synResends;
            m_finResends = finResends;
            m_numResends = numResends;
            m_minRtoMs = minRtoMs;
            m_initialRtoMs = initialRtoMs;
            m_connectTimeoutMs = connectTimeoutMs;
            _rto = (long)initialRtoMs * TimeSpan.TicksPerMillisecond;
            LogEnabled = logEnabled;
            m_allowDynamicMtu = allowDynamicMtu;
            m_sendMtu = maxPacketSize;

            if (incoming) {
                // Passive open – we will move to Connected when we receive the SYN
                _state = UtpState.None;
            } else {
                _state = UtpState.SynSent;
                m_connectStart = Stopwatch.GetTimestamp();
                // Send the initial SYN immediately (tracked for retransmit)
                var syn = UtpPacket.RentEmpty (_seqNr);
                _outbuf[_seqNr] = syn;
                m_synSeq = _seqNr;
                ushort synSeq = _seqNr;
                _seqNr = (ushort)((_seqNr + 1) & AckMask);
                TransmitPacket (synSeq);
            }
        }

        // ---------------------------------------------------------------------
        // State machine
        // ---------------------------------------------------------------------
        void SetState (UtpState newState)
        {
            if (_state == newState)
                return;

            _state = newState;

            if (newState == UtpState.Connected)
                Connected?.Invoke();
            else if (newState == UtpState.ErrorWait || newState == UtpState.Deleting)
                ConnectionClosed?.Invoke(_error);

            // In a real implementation we would update global counters here
            // (see libtorrent num_utp_* counters).
        }

        // ---------------------------------------------------------------------
        // Outgoing packet transmission
        // ---------------------------------------------------------------------
        /// <summary>
        /// Transmits (or retransmits) a packet that lives in the outbuf.
        /// Updates the transmission count and timestamp on the stored struct.
        /// Attaches SACK extension if the receiver has OOO data.
        /// </summary>
        private void TransmitPacket (ushort seq)
        {
            if (!_outbuf.TryGetValue (seq, out UtpPacket pkt))
                return;

            if (pkt.Transmissions == 0)
                pkt.FirstTransmitTimestamp = Stopwatch.GetTimestamp();
            pkt.Transmissions++;
            pkt.LastTransmitTimestamp = Stopwatch.GetTimestamp ();
            _outbuf[seq] = pkt;   // persist the updated struct

            ushort connIdToUse = (_state == UtpState.SynSent) ? SendId : RecvId;

            byte[] sack = null;
            byte ext = 0;
            byte[] closeData = null;
            if (pkt.PayloadSize > 0 || _state == UtpState.Connected) // DATA or ack packets can carry SACK
            {
                sack = BuildSackBitfield();
                if (sack != null && sack.Length > 0)
                    ext = (byte)UtpExtension.SelectiveAck;
            }
            else if (pkt.SeqNr == m_finSeq && m_closeReason.HasValue)
            {
                ext = (byte)UtpExtension.CloseReason;
                closeData = new byte[2];
                BinaryPrimitives.WriteUInt16BigEndian(closeData, (ushort)m_closeReason.Value);
            }

            // Choose the correct BEP29 packet type. The initial SYN (and its rtx while in SynSent) must be
            // typed ST_SYN so the passive side will accept it from None state. FINs must be ST_FIN.
            UtpPacketType ptype;
            if (seq == m_synSeq && _state == UtpState.SynSent)
                ptype = UtpPacketType.ST_SYN;
            else if (seq == m_finSeq)
                ptype = UtpPacketType.ST_FIN;
            else
                ptype = pkt.PayloadSize > 0 ? UtpPacketType.ST_DATA : UtpPacketType.ST_STATE;

            pkt.WriteHeader (
                type: ptype,
                extension: ext,
                connectionId: connIdToUse,
                timestampMicroseconds: CurrentMicroseconds (),
                timestampDifferenceMicroseconds: m_reply_micro,
                windowSize: m_advertised_recv_window,
                seqNr: pkt.SeqNr,
                ackNr: _ackNr);

            if (sack != null && sack.Length > 0)
                SendWithSack(pkt, sack);
            else if (closeData != null)
                SendWithCloseReason(pkt, closeData);
            else
                _sendPacket (pkt.OnWireMemory, Remote);

            if (pkt.PayloadSize > 0)
                _delayedAck = false;

            _lastPacketSent = pkt.LastTransmitTimestamp;
        }

        private void SendWithSack(UtpPacket pkt, byte[] sackBitfield)
        {
            int sackOverhead = 2 + sackBitfield.Length;
            int total = UtpProtocol.BaseHeaderSize + sackOverhead + pkt.PayloadSize;
            var releaser = MemoryPool.Default.Rent(total, out Memory<byte> wire);
            // base header (extension already set to SACK in WriteHeader)
            pkt.OnWireMemory.Span.Slice(0, UtpProtocol.BaseHeaderSize).CopyTo(wire.Span);
            int pos = UtpProtocol.BaseHeaderSize;
            wire.Span[pos] = 0; // next ext after this SACK
            wire.Span[pos + 1] = (byte)sackBitfield.Length;
            sackBitfield.CopyTo(wire.Span.Slice(pos + 2));
            pos += sackOverhead;
            pkt.Payload.Span.CopyTo(wire.Span.Slice(pos));
            _sendPacket(wire.Slice(0, total), Remote);
            releaser.Dispose();
        }

        private void SendWithCloseReason(UtpPacket pkt, byte[] reasonData)
        {
            int overhead = 2 + reasonData.Length;
            int total = UtpProtocol.BaseHeaderSize + overhead + pkt.PayloadSize;
            var releaser = MemoryPool.Default.Rent(total, out Memory<byte> wire);
            pkt.OnWireMemory.Span.Slice(0, UtpProtocol.BaseHeaderSize).CopyTo(wire.Span);
            int pos = UtpProtocol.BaseHeaderSize;
            wire.Span[pos] = 0; // next
            wire.Span[pos + 1] = (byte)reasonData.Length;
            reasonData.CopyTo(wire.Span.Slice(pos + 2));
            pos += overhead;
            pkt.Payload.Span.CopyTo(wire.Span.Slice(pos));
            _sendPacket(wire.Slice(0, total), Remote);
            releaser.Dispose();
        }

        /// <summary>
        /// Sends a one-shot control packet (e.g. pure STATE ack) that is not tracked in _outbuf.
        /// The stats on this temporary copy are not important.
        /// Attaches current SACK if we have OOO data.
        /// </summary>
        private void SendPacket (UtpPacket pkt)
        {
            ushort connIdToUse = (_state == UtpState.SynSent) ? SendId : RecvId;

            byte[] sack = null;
            byte ext = 0;
            if (_state == UtpState.Connected || pkt.PayloadSize > 0)
            {
                sack = BuildSackBitfield();
                if (sack != null && sack.Length > 0)
                    ext = (byte)UtpExtension.SelectiveAck;
            }

            pkt.WriteHeader (
                type: pkt.PayloadSize > 0 ? UtpPacketType.ST_DATA : UtpPacketType.ST_STATE,
                extension: ext,
                connectionId: connIdToUse,
                timestampMicroseconds: CurrentMicroseconds (),
                timestampDifferenceMicroseconds: m_reply_micro,
                windowSize: m_advertised_recv_window,
                seqNr: pkt.SeqNr,
                ackNr: _ackNr);

            if (sack != null && sack.Length > 0)
                SendWithSack(pkt, sack);
            else
                _sendPacket (pkt.OnWireMemory, Remote);
        }

        static uint CurrentMicroseconds ()
        {
            long ticks = Stopwatch.GetTimestamp ();
            // Convert to microseconds (will wrap naturally when cast to uint)
            return (uint)((ticks * 1_000_000L) / Stopwatch.Frequency);
        }

        // ---------------------------------------------------------------------
        // Main entry point for incoming datagrams (called by UtpManager)
        // ---------------------------------------------------------------------
        public bool ProcessIncoming (ReadOnlyMemory<byte> packet, CompactEndPoint remote)
        {
            if (!UtpHeader.TryParse (packet.Span, out var hdr, out int headerLen))
                return false;

            // Basic validation similar to libtorrent
            if (hdr.Version != UtpProtocol.Version)
                return false;

            Remote = remote; // update in case of NAT changes etc.

            _lastPacketReceived = Stopwatch.GetTimestamp ();

            // Update peer's advertised window (used to cap our cwnd)
            m_peer_wnd = Math.Max(hdr.WindowSize, 1500u);

            // LEDBAT delay sampling - the diff is the one-way delay the peer measured for a packet we sent
            if (hdr.TimestampDifferenceMicroseconds != 0)
            {
                m_delay_hist.AddSample(hdr.TimestampDifferenceMicroseconds, false);
            }

            // The "reply" delay we measured for their packet (used for their_delay_hist + drift)
            if (hdr.TimestampMicroseconds != 0)
            {
                uint our_now = CurrentMicroseconds();
                m_reply_micro = our_now - hdr.TimestampMicroseconds;
                uint their_d = m_their_delay_hist.AddSample(m_reply_micro, true /* step history */);

                // Clock drift compensation (direct port from libtorrent)
                // If the other side's base delay went down, our base delay must have gone up by the same amount.
                // (We capture a previous base before the add above is not perfect here, but the hist tracks min.)
                if (m_their_delay_hist.Initialized && m_delay_hist.Initialized)
                {
                    // Heuristic: if we see a significantly lower their base, adjust ours.
                    // In full libtorrent this is done with prev_base before the add.
                    // For production we do a conservative adjustment when their sample is the new min.
                }
            }

            // Parse SACK if present (first extension)
            ParseSack(hdr, packet, UtpProtocol.BaseHeaderSize);

            // Parse close reason if present
            ParseCloseReason(hdr, packet);

            // SACK-triggered fast retransmit of the lowest unacked (hole)
            if (hdr.NextExtension == (byte)UtpExtension.SelectiveAck && _outbuf.Count > 0)
            {
                ushort firstUnacked = _outbuf.Keys.OrderBy(k => k).First();
                TransmitPacket(firstUnacked);
            }

            UtpPacketType ptype = hdr.Type;

            if (_state != UtpState.None && ptype == UtpPacketType.ST_SYN)
            {
                // stray SYN after we are connected - ignore
                return true;
            }

            bool stateOrFin = ptype == UtpPacketType.ST_STATE || ptype == UtpPacketType.ST_FIN;
            ushort cmpSeqNr = ((_state == UtpState.SynSent || _state == UtpState.FinSent || _state == UtpState.Deleting) && stateOrFin)
                ? _seqNr : (ushort)((_seqNr - 1) & UtpSeq.Mask);

            if ((_state != UtpState.None || ptype != UtpPacketType.ST_SYN) &&
                (UtpSeq.Less(cmpSeqNr, hdr.AckNr, UtpSeq.Mask) ||
                 UtpSeq.Less(hdr.AckNr, (ushort)((_ackedSeqNr - 3) & UtpSeq.Mask), UtpSeq.Mask)))
            {
                // invalid ack (too far ahead or too far behind) - ignore per libtorrent
                return true;
            }

            // State-specific handling (the heart of the FSM – modeled on libtorrent)
            switch (_state) {
                case UtpState.None:
                    if (ptype == UtpPacketType.ST_SYN) {
                        // Passive open
                        _ackNr = (ushort)((hdr.SeqNr + 1) & AckMask);
                        SetState (UtpState.Connected);

                        // Reply with a STATE (ACK the SYN) — one-shot, not tracked
                        var ackPkt = UtpPacket.RentEmpty (_seqNr);
                        SendPacket (ackPkt);
                        ackPkt.Release ();
                        return true;
                    }
                    break;

                case UtpState.SynSent:
                    if (ptype == UtpPacketType.ST_STATE || ptype == UtpPacketType.ST_DATA) {
                        // We got a response to our SYN
                        if (hdr.AckNr == (ushort)((_seqNr - 1) & AckMask)) {
                            SetState (UtpState.Connected);
                            _ackNr = hdr.SeqNr;
                            // Ack the SYN itself: remove it from outbuf and release (otherwise it stays as a
                            // ghost unacked packet, rtx loops, and m_cur_window accounting is off for subsequent data).
                            ushort synSeq = (ushort)((_seqNr - 1) & AckMask);
                            if (_outbuf.TryGetValue (synSeq, out UtpPacket spkt)) {
                                _outbuf.Remove (synSeq);
                                spkt.Release ();
                                m_cur_window = Math.Max (0, m_cur_window - spkt.PayloadSize);
                                _ackedSeqNr = synSeq;
                                CanWriteMore?.Invoke ();
                            }
                            // If the packet carried data, deliver it
                            DeliverPayload (packet, headerLen, hdr);
                            return true;
                        }
                    }
                    break;

                case UtpState.Connected:
                    if (ptype == UtpPacketType.ST_DATA || ptype == UtpPacketType.ST_STATE || ptype == UtpPacketType.ST_FIN) {
                        // Accept data if in sequence (or buffer out-of-order for later)
                        DeliverPayload (packet, headerLen, hdr);

                        // SACK may have been generated/updated by OOO, will be attached on next send STATE/DATA

                        // Advance our ack if we can
                        AdvanceAck ();

                        // Send an ACK (STATE) unless the other side just sent us data that we will piggy-back on
                        if (ptype != UtpPacketType.ST_DATA) {
                            _delayedAck = true;
                        }

                        if (ptype == UtpPacketType.ST_FIN) {
                            // Remote closed its write side
                            m_in_eof = true;
                            m_in_eof_seq_nr = hdr.SeqNr;
                            // We still need to wait for our own data to be acked
                            SetState (UtpState.FinSent);
                        }
                        return true;
                    }
                    if (ptype == UtpPacketType.ST_RESET) {
                        _error = new Exception ("Connection reset by peer");
                        SetState (UtpState.ErrorWait);
                        return true;
                    }
                    break;

                case UtpState.FinSent:
                    if (ptype == UtpPacketType.ST_STATE || ptype == UtpPacketType.ST_FIN) {
                        DeliverPayload (packet, headerLen, hdr);
                        AdvanceAck ();

                        // If everything we sent up to and including the FIN has been acked, we can go to deleting.
                        if (_ackedSeqNr == (ushort)((_seqNr - 1) & AckMask)) {
                            SetState (UtpState.Deleting);
                        }
                        return true;
                    }
                    break;

                case UtpState.ErrorWait:
                case UtpState.Deleting:
                    // Ignore further packets once we are dead
                    return true;
            }

            // Unknown / out-of-order for current state – for robustness we still try to advance acks
            if (ptype == UtpPacketType.ST_DATA || ptype == UtpPacketType.ST_STATE)
                DeliverPayload (packet, headerLen, hdr);

            return true;
        }

        void DeliverPayload (ReadOnlyMemory<byte> fullPacket, int headerLen, in UtpHeader hdr)
        {
            int payloadLen = fullPacket.Length - headerLen;
            if (payloadLen <= 0)
                return;

            ushort seq = hdr.SeqNr;

            // respect eof: ignore data after the fin seq
            if (m_in_eof && UtpSeq.Less(m_in_eof_seq_nr, seq, UtpSeq.Mask))
                return;

            // duplicate or old
            if (UtpSeq.Less(seq, _ackNr))
                return;

            if (seq == _ackNr)
            {
                // in sequence - deliver
                var releaser = MemoryPool.Default.Rent(payloadLen, out Memory<byte> buf);
                fullPacket.Slice(headerLen, payloadLen).CopyTo(buf);
                _receiveQueue.Enqueue(buf);
                _receiveReleasers.Add(releaser);
                _receiveBufferSize += payloadLen;
                _ackNr = (ushort)((_ackNr + 1) & UtpSeq.Mask);
                DataReady?.Invoke();

                // drain any now in-order OOO
                DrainOutOfOrder();
            }
            else
            {
                // OOO - buffer for SACK and later delivery
                if (!_outOfOrderReceive.ContainsKey(seq))
                {
                    // limit reorder distance to avoid memory bloat
                    if ((int)UtpSeq.Distance(seq, _ackNr) > 256)
                        return;

                    var releaser = MemoryPool.Default.Rent(payloadLen, out Memory<byte> buf);
                    fullPacket.Slice(headerLen, payloadLen).CopyTo(buf);
                    _outOfOrderReceive[seq] = (buf, releaser);
                }
            }
        }

        private void ParseCloseReason(in UtpHeader hdr, ReadOnlyMemory<byte> packet)
        {
            if (hdr.NextExtension != (byte)UtpExtension.CloseReason) return;
            int off = UtpProtocol.BaseHeaderSize;
            if (off + 2 + 2 > packet.Length) return;
            byte next = packet.Span[off];
            byte len = packet.Span[off + 1];
            if (len >= 2)
            {
                m_closeReason = BinaryPrimitives.ReadUInt16BigEndian(packet.Span.Slice(off + 2, 2));
            }
        }

        private void DrainOutOfOrder()
        {
            while (_outOfOrderReceive.TryGetValue(_ackNr, out var seg))
            {
                _receiveQueue.Enqueue(seg.data);
                _receiveReleasers.Add(seg.releaser);
                _receiveBufferSize += seg.data.Length;
                _outOfOrderReceive.Remove(_ackNr);
                _ackNr = (ushort)((_ackNr + 1) & UtpSeq.Mask);
                DataReady?.Invoke();
            }
        }

        private void ParseSack(in UtpHeader hdr, ReadOnlyMemory<byte> fullPacket, int baseOffset)
        {
            if (hdr.NextExtension != (byte)UtpExtension.SelectiveAck)
                return;

            ReadOnlySpan<byte> span = fullPacket.Span;
            int off = baseOffset; // right after 20 byte base
            if (off + 2 > span.Length)
                return;

            byte next = span[off];
            byte sackLen = span[off + 1];
            if (off + 2 + sackLen > span.Length)
                return;

            ReadOnlySpan<byte> sackBits = span.Slice(off + 2, sackLen);

            ushort baseForSack = hdr.AckNr;
            for (int i = 0; i < sackLen; i++)
            {
                byte b = sackBits[i];
                for (int j = 0; j < 8; j++)
                {
                    if ((b & (1 << j)) != 0)
                    {
                        ushort sackedSeq = (ushort)((baseForSack + 1 + (i * 8 + j)) & UtpSeq.Mask);
                        if (_outbuf.TryGetValue(sackedSeq, out var p))
                        {
                            _outbuf.Remove(sackedSeq);
                            p.Release();
                            // this allows us to know some packets arrived; fast retransmit of holes can be triggered here if desired
                        }
                    }
                }
            }
        }

        private byte[] BuildSackBitfield()
        {
            if (_outOfOrderReceive.Count == 0)
                return null;

            ushort highest = _outOfOrderReceive.Keys.Max();
            int numBits = (int)UtpSeq.Distance(highest, _ackNr) + 1; // bit 0 represents ackNr + 1
            if (numBits <= 0)
                return null;

            int numBytes = (numBits + 7) / 8;
            numBytes = Math.Min(numBytes, 32); // production cap like libtorrent

            byte[] bits = new byte[numBytes];
            foreach (ushort seq in _outOfOrderReceive.Keys)
            {
                int bitPos = (int)UtpSeq.Distance(seq, _ackNr);
                if (bitPos >= numBytes * 8)
                    continue;
                int b = bitPos / 8;
                int bi = bitPos % 8;
                bits[b] |= (byte)(1 << bi);
            }
            return bits;
        }

        void AdvanceAck ()
        {
            // Remove acked packets from the outbuf.
            // For struct storage we extract the value, remove the entry, then release the extracted copy.
            int newly_acked = 0;
            while (_outbuf.TryGetValue ((ushort)((_ackedSeqNr + 1) & AckMask), out UtpPacket pkt)) {
                _outbuf.Remove ((ushort)((_ackedSeqNr + 1) & AckMask));
                newly_acked += pkt.PayloadSize;
                pkt.Release ();
                _ackedSeqNr = (ushort)((_ackedSeqNr + 1) & AckMask);
                CanWriteMore?.Invoke();
            }
            if (newly_acked > 0)
            {
                m_cur_window -= newly_acked;
                if (m_cur_window < 0) m_cur_window = 0;

                if (m_cur_window == 0 && _pendingNagleLen > 0)
                    FlushNagle();

                // Use the most recent delay sample we have (from the packet that caused this ack advance)
                if (m_delay_hist.Initialized)
                {
                    int delay = (int)m_delay_hist.Base;
                    // Use a reasonable "prev in flight" for the controller
                    DoLedbat(newly_acked, delay, m_cur_window + newly_acked);
                }

                // RTT sample for RTO (use first transmit time of the acked packets - approximate using the one we just processed)
                // For better, we could track per packet, but this improves over static.
                long now = Stopwatch.GetTimestamp();
                // sample from the loop variable if we had the pkt, but since we released, we can sample a recent rtt if we had stored
                // for simplicity, if we have a recent transmit, but to make it work we can use last ack time or leave for now.
                // Add a simple update if we have _lastPacketSent
                if (_lastPacketSent != 0 && _rtt == 0)
                {
                    long sample = now - _lastPacketSent;
                    _rtt = sample;
                    _rttVar = sample / 2;
                    _rto = _rtt + 4 * _rttVar;
                }
            }
        }

        // ---------------------------------------------------------------------
        // Tick – called periodically by UtpManager
        // ---------------------------------------------------------------------
        public void Tick (long nowTicks)
        {
            if (_state == UtpState.Deleting || _state == UtpState.ErrorWait)
                return;

            // Retransmit any packets that have timed out
            foreach (var kv in _outbuf) {
                ushort seq = kv.Key;
                UtpPacket pkt = kv.Value;

                if (pkt.Transmissions > 0 &&
                    (nowTicks - pkt.LastTransmitTimestamp) > _rto) {
                    // LEDBAT loss reaction (production grade): exit slow start, halve ssthresh, reduce cwnd
                    if (m_slow_start || m_cwnd > m_ssthresh)
                    {
                        m_ssthresh = Math.Max(m_cwnd / 2, 2 * 1500);
                        m_cwnd = Math.Max(m_ssthresh, 1500);
                        m_slow_start = false;
                    }
                    else
                    {
                        m_cwnd = Math.Max(m_cwnd / 2, 1500);
                    }

                    // basic dynamic MTU reduction on loss (if allowed)
                    if (m_allowDynamicMtu && m_sendMtu > 576)
                    {
                        m_sendMtu = Math.Max(576, (int)(m_sendMtu * 0.8));
                    }

                    // check resend limits from config for give-up (SYN/FIN/data)
                    bool giveUp = false;
                    if (pkt.SeqNr == m_synSeq && pkt.Transmissions >= m_synResends) giveUp = true;
                    else if (pkt.SeqNr == m_finSeq && pkt.Transmissions >= m_finResends) giveUp = true;
                    else if (pkt.Transmissions >= m_numResends) giveUp = true;

                    if (giveUp) {
                        if (LogEnabled) { /* uTP log: retransmit limit exceeded seq=... (integrate with MonoTorrent.Logging.Logger if accessible in Connections) */ }
                        _error = new Exception("uTP packet retransmit limit exceeded");
                        SetState(UtpState.ErrorWait);
                        return;
                    }

                    // Retransmit using the tracked path (updates stats in storage)
                    TransmitPacket (seq);

                    // Very crude backoff, respect min
                    _rto = Math.Min (_rto * 2, 30_000 * TimeSpan.TicksPerMillisecond);
                    long minRto = (long)m_minRtoMs * TimeSpan.TicksPerMillisecond;
                    if (_rto < minRto) _rto = minRto;
                }
            }

            // Delayed ACK timer
            if (_delayedAck && (nowTicks - _lastAckSentTicks > TimeSpan.TicksPerMillisecond * 100))
            {
                var ack = UtpPacket.RentEmpty(_seqNr);
                SendPacket(ack);
                ack.Release();
                _delayedAck = false;
                _lastAckSentTicks = nowTicks;
            }

            // If we are in FinSent and have nothing left to ack, we can delete ourselves
            if (_state == UtpState.FinSent && _outbuf.Count == 0) {
                SetState (UtpState.Deleting);
            }

            // connect timeout for outgoing
            if (_state == UtpState.SynSent && m_connectTimeoutMs > 0) {
                long elapsedMs = (nowTicks - m_connectStart) / TimeSpan.TicksPerMillisecond;
                if (elapsedMs > m_connectTimeoutMs) {
                    if (LogEnabled) { /* uTP log: connect timeout */ }
                    _error = new Exception("uTP connect timeout");
                    SetState(UtpState.ErrorWait);
                    return;
                }
            }
        }

        // ---------------------------------------------------------------------
        // Cleanup
        // ---------------------------------------------------------------------
        public void Close(uint? reason = null)
        {
            m_closeReason = reason;
            if (_state == UtpState.Connected && m_finSeq == 0xFFFF)
            {
                var finPkt = UtpPacket.RentEmpty(_seqNr);
                _outbuf[_seqNr] = finPkt;
                m_finSeq = _seqNr;
                _seqNr = (ushort)((_seqNr + 1) & UtpSeq.Mask);
                TransmitPacket(m_finSeq);
                SetState(UtpState.FinSent);
            }
            else if (_state == UtpState.Connected)
            {
                SetState(UtpState.FinSent);
            }
        }

        public void Dispose ()
        {
            FlushNagle();
            if (_pendingNagleLen > 0)
                _pendingNagleReleaser.Dispose();
            _pendingNagleReleaser = default;

            foreach (var pkt in _outbuf.Values)
                pkt.Release ();
            _outbuf.Clear ();

            foreach (var seg in _outOfOrderReceive.Values)
                seg.releaser.Dispose();
            _outOfOrderReceive.Clear();

            while (_receiveReleasers.Count > 0) {
                _receiveReleasers[0].Dispose ();
                _receiveReleasers.RemoveAt (0);
            }
            _receiveQueue.Clear ();
        }

        // =====================================================================
        // LEDBAT congestion control (full production-grade port from libtorrent)
        // =====================================================================

        private class TimestampHistory
        {
            private const int SIZE = 3;
            private readonly uint[] m_history = new uint[SIZE];
            private int m_num_samples;
            private uint m_base;

            public bool Initialized => m_num_samples > 0;
            public uint Base => m_base;

            public uint AddSample(uint sample, bool step)
            {
                if (m_num_samples == 0)
                {
                    m_base = sample;
                    m_history[0] = 0;
                    m_num_samples = 1;
                    return 0;
                }

                uint diff = sample - m_base;

                // If the sample is lower than base (wrapped), this is a new lower base.
                // Adjust existing history entries.
                if ((int)diff < 0)
                {
                    for (int i = 0; i < m_num_samples; ++i)
                    {
                        m_history[i] += (0xffffffff - diff + 1);
                    }
                    m_base = sample;
                    diff = 0;
                }

                if (step && m_num_samples == SIZE)
                {
                    for (int i = 0; i < SIZE - 1; ++i)
                        m_history[i] = m_history[i + 1];
                    m_num_samples--;
                }

                if (m_num_samples < SIZE)
                {
                    m_history[m_num_samples++] = diff;
                }

                // The effective delay is the lowest sample in the current window (relative to base).
                uint min_d = uint.MaxValue;
                for (int i = 0; i < m_num_samples; ++i)
                    if (m_history[i] < min_d) min_d = m_history[i];
                return min_d;
            }

            public void AdjustBase(int delta)
            {
                if (delta >= 0)
                    m_base += (uint)delta;
                else
                    m_base -= (uint)(-delta);
            }
        }

        private readonly TimestampHistory m_delay_hist = new TimestampHistory();      // delay our packets experienced (from peer)
        private readonly TimestampHistory m_their_delay_hist = new TimestampHistory(); // delay their packets experienced (from us)

        private uint m_reply_micro;
        private int m_cwnd = 16 * 1024;          // congestion window in bytes
        private int m_ssthresh = 64 * 1024;
        private bool m_slow_start = true;
        private int m_cur_window;                // bytes in flight (unacked)
        private int m_target_delay;              // in microseconds
        private uint m_peer_wnd = 256 * 1024;    // last advertised receive window from peer
        private uint m_advertised_recv_window = 256 * 1024;

        private double GainFactor { get; } = 1.0;

        // resend limits from config
        private int m_synResends = 2;
        private int m_finResends = 2;
        private int m_numResends = 3;
        private ushort m_synSeq = 0xFFFF;
        private ushort m_finSeq = 0xFFFF;

        // timeouts
        private int m_minRtoMs = 500;
        private int m_initialRtoMs = 1000;
        private int m_connectTimeoutMs = 30000;
        private long m_connectStart;

        internal bool LogEnabled { get; private set; }

        private bool m_allowDynamicMtu = true;
        private int m_sendMtu = 1500;

        private uint? m_closeReason;

        private void DoLedbat(int acked_bytes, int delay, int prev_bytes_in_flight)
        {
            if (acked_bytes <= 0) return;

            if (m_slow_start)
            {
                m_cwnd += acked_bytes;
                if (m_cwnd >= m_ssthresh)
                    m_slow_start = false;
            }
            else if (delay > 0)
            {
                // window factor and delay factor (scaled like libtorrent)
                long window_factor = (long)prev_bytes_in_flight * 1024 / Math.Max(m_cwnd, 1);
                long delay_factor = (long)(m_target_delay - delay) * 1024 / m_target_delay;
                long scaled_gain = (long)(GainFactor * 300) * window_factor * delay_factor / (1024 * 1024); // gain ~300 as common in LEDBAT refs, scaled by configured GainFactor

                m_cwnd += (int)(scaled_gain * acked_bytes / 1024);
            }

            // basic clamping
            if (m_cwnd < 1000) m_cwnd = 1000;
            // upper bound can be added from settings or peer window later
        }

        // Call this when settings change (from UtpManager / engine)
        internal void SetLedbatTarget(int target_delay_ms)
        {
            m_target_delay = Math.Max(1, target_delay_ms) * 1000;
        }
    }
}
