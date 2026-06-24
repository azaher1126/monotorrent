//
// UtpHeader.cs
//
// Authors:
//   Ayman (based on BEP 29 and libtorrent-rasterbar implementation)
//
// Copyright (C) 2026
//
// Permission is hereby granted, free of charge, to any person obtaining
// a copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to
// the following conditions:
//
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
// LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
// WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
//

using System;
using System.Buffers.Binary;
using System.Runtime.InteropServices;

namespace MonoTorrent.Connections.Peer.Utp
{
    /// <summary>
    /// uTP packet types (BEP 29).
    /// </summary>
    internal enum UtpPacketType : byte
    {
        ST_DATA = 0,
        ST_FIN = 1,
        ST_STATE = 2,
        ST_RESET = 3,
        ST_SYN = 4,
    }

    internal static class UtpConstants
    {
        public const byte Version = 1;
        public const int HeaderSize = 20; // 1+1+2 +4+4 +2+2+2
        public const int MaxPacketSize = 64 * 1024; // reasonable upper bound before MTU discovery
        public const ushort AckMask = 0xFFFF;

        // Extension field values
        public const byte NoExtension = 0;
        public const byte SelectiveAckExtension = 1;
    }

    /// <summary>
    /// Represents the fixed 20-byte uTP header (BEP 29). Provides helpers to read/write from spans.
    /// This struct is intentionally blittable-ish for fast handling; we parse manually for clarity and endian safety.
    /// </summary>
    [StructLayout (LayoutKind.Sequential, Pack = 1)]
    internal readonly struct UtpHeader
    {
        // Wire layout (big endian for multi-byte):
        // byte 0: type (high 4) | version (low 4)
        // byte 1: extension
        // bytes 2-3: connection_id (big endian)
        // bytes 4-7: timestamp_microseconds (big endian)
        // bytes 8-11: timestamp_difference_microseconds (big endian)
        // bytes 12-13: wnd_size (big endian)
        // bytes 14-15: seq_nr (big endian)
        // bytes 16-19: ack_nr (big endian)

        public readonly byte TypeVersion;
        public readonly byte Extension;
        public readonly ushort ConnectionId;
        public readonly uint TimestampMicroseconds;
        public readonly uint TimestampDifferenceMicroseconds;
        public readonly ushort WindowSize;
        public readonly ushort SeqNr;
        public readonly ushort AckNr;

        public UtpHeader (byte typeVersion, byte extension, ushort connectionId,
                          uint tsMicro, uint tsDiffMicro, ushort wndSize,
                          ushort seqNr, ushort ackNr)
        {
            TypeVersion = typeVersion;
            Extension = extension;
            ConnectionId = connectionId;
            TimestampMicroseconds = tsMicro;
            TimestampDifferenceMicroseconds = tsDiffMicro;
            WindowSize = wndSize;
            SeqNr = seqNr;
            AckNr = ackNr;
        }

        public UtpPacketType PacketType => (UtpPacketType) (TypeVersion >> 4);
        public byte Version => (byte) (TypeVersion & 0x0F);

        public static bool TryParse (ReadOnlySpan<byte> buffer, out UtpHeader header)
        {
            header = default;
            if (buffer.Length < UtpConstants.HeaderSize)
                return false;

            byte typeVer = buffer[0];
            byte ext = buffer[1];
            ushort connId = BinaryPrimitives.ReadUInt16BigEndian (buffer.Slice (2, 2));
            uint ts = BinaryPrimitives.ReadUInt32BigEndian (buffer.Slice (4, 4));
            uint tsdiff = BinaryPrimitives.ReadUInt32BigEndian (buffer.Slice (8, 4));
            ushort wnd = BinaryPrimitives.ReadUInt16BigEndian (buffer.Slice (12, 2));
            ushort seq = BinaryPrimitives.ReadUInt16BigEndian (buffer.Slice (14, 2));
            ushort ack = BinaryPrimitives.ReadUInt16BigEndian (buffer.Slice (16, 2));

            header = new UtpHeader (typeVer, ext, connId, ts, tsdiff, wnd, seq, ack);
            return true;
        }

        public void WriteTo (Span<byte> destination)
        {
            if (destination.Length < UtpConstants.HeaderSize)
                throw new ArgumentException ("Destination too small for uTP header", nameof (destination));

            destination[0] = TypeVersion;
            destination[1] = Extension;
            BinaryPrimitives.WriteUInt16BigEndian (destination.Slice (2, 2), ConnectionId);
            BinaryPrimitives.WriteUInt32BigEndian (destination.Slice (4, 4), TimestampMicroseconds);
            BinaryPrimitives.WriteUInt32BigEndian (destination.Slice (8, 4), TimestampDifferenceMicroseconds);
            BinaryPrimitives.WriteUInt16BigEndian (destination.Slice (12, 2), WindowSize);
            BinaryPrimitives.WriteUInt16BigEndian (destination.Slice (14, 2), SeqNr);
            BinaryPrimitives.WriteUInt16BigEndian (destination.Slice (16, 2), AckNr);
        }

        public override string ToString ()
            => $"uTP v{Version} {PacketType} conn={ConnectionId} seq={SeqNr} ack={AckNr} wnd={WindowSize} ts={TimestampMicroseconds} tsdiff={TimestampDifferenceMicroseconds} ext={Extension}";
    }
}
