//
// UtpPacket.cs
//
// Represents a single uTP packet (DATA, STATE, SYN, FIN, RESET) that may need
// to be retransmitted. Owns a buffer rented from MemoryPool for the payload
// (and optionally header space).
//
// Lifetime: created when we queue data or synthesize a control packet,
// released when it is acked or the socket decides to drop it after too many
// retransmissions.
//

using System;

using MonoTorrent;

namespace MonoTorrent.Connections.Utp
{
    /// <summary>
    /// Represents a single uTP packet that may need retransmission.
    /// Implemented as a struct to avoid per-packet object allocations on the send path.
    /// The expensive part (the payload buffer) is still rented from MemoryPool.
    ///
    /// Ownership rules:
    /// - Only release a packet after removing it from the authoritative storage (_outbuf in UtpConnection).
    /// - Do not keep long-lived copies of the struct and call Release on them.
    /// </summary>
    public struct UtpPacket
    {
        /// <summary>
        /// The sequence number of this packet.
        /// </summary>
        public ushort SeqNr { get; }

        /// <summary>
        /// Number of times we have transmitted (or retransmitted) this packet.
        /// </summary>
        public int Transmissions { get; set; }

        /// <summary>
        /// Timestamp (in Stopwatch ticks) of the last transmission.
        /// </summary>
        public long LastTransmitTimestamp { get; set; }

        /// <summary>
        /// Timestamp of the *first* transmission of this packet (for RTT sampling on ack).
        /// </summary>
        public long FirstTransmitTimestamp { get; set; }

        /// <summary>
        /// Size of the user payload in this packet (not including uTP header).
        /// </summary>
        public int PayloadSize { get; }

        private Memory<byte> _buffer;
        private ByteBufferPool.Releaser _releaser;

        /// <summary>
        /// The region containing the actual user data (after the header reservation).
        /// </summary>
        public Memory<byte> Payload => _buffer.Slice (HeaderReserveSize, PayloadSize);

        /// <summary>
        /// The full region we will send on the wire (header + payload).
        /// The caller must have written a valid 20-byte uTP header into the front before transmitting.
        /// </summary>
        public Memory<byte> OnWireMemory => _buffer.Slice (0, HeaderReserveSize + PayloadSize);

        public ReadOnlySpan<byte> OnWireSpan => OnWireMemory.Span;

        const int HeaderReserveSize = UtpProtocol.BaseHeaderSize;

        private UtpPacket (ushort seqNr, Memory<byte> buffer, ByteBufferPool.Releaser releaser, int payloadSize)
        {
            SeqNr = seqNr;
            _buffer = buffer;
            _releaser = releaser;
            PayloadSize = payloadSize;
            Transmissions = 0;
            LastTransmitTimestamp = 0;
        }

        /// <summary>
        /// Rents a packet buffer from the pool and copies the supplied user payload into it.
        /// Reserves space at the front for the uTP base header (20 bytes).
        /// </summary>
        public static UtpPacket Rent (ushort seqNr, ReadOnlyMemory<byte> payload)
        {
            int totalSize = HeaderReserveSize + payload.Length;
            var releaser = MemoryPool.Default.Rent (totalSize, out Memory<byte> rented);

            if (payload.Length > 0)
                payload.CopyTo (rented.Slice (HeaderReserveSize));

            return new UtpPacket (seqNr, rented, releaser, payload.Length);
        }

        /// <summary>
        /// Rents a zero-payload packet (pure control packets: STATE, SYN, FIN, RESET, etc.).
        /// </summary>
        public static UtpPacket RentEmpty (ushort seqNr)
            => Rent (seqNr, ReadOnlyMemory<byte>.Empty);

        /// <summary>
        /// Writes (or overwrites) the uTP base header at the front of the owned buffer.
        /// Called on every (re)transmit because timestamps and ack fields change.
        /// </summary>
        public void WriteHeader (UtpPacketType type,
                                 byte extension,
                                 ushort connectionId,
                                 uint timestampMicroseconds,
                                 uint timestampDifferenceMicroseconds,
                                 uint windowSize,
                                 ushort seqNr,
                                 ushort ackNr)
        {
            UtpHeader.Write (_buffer.Span,
                             type,
                             extension,
                             connectionId,
                             timestampMicroseconds,
                             timestampDifferenceMicroseconds,
                             windowSize,
                             seqNr,
                             ackNr);
        }

        /// <summary>
        /// Releases the rented buffer back to the pool.
        /// Must be called exactly once per packet, after it has been removed from
        /// the authoritative storage (e.g. _outbuf dictionary).
        /// </summary>
        public void Release ()
        {
            _releaser.Dispose ();
            _releaser = default;
            _buffer = default;
        }

        public override string ToString ()
            => $"UtpPacket seq={SeqNr} len={PayloadSize} tx={Transmissions}";
    }
}
