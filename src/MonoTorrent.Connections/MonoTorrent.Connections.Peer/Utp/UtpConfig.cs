//
// UtpConfig.cs
//
// Tunable settings for uTP (BEP 29), combining the rich configurability from
// another-bep29-impl with the production-shaped socket manager from maybe-most-complete-bep29.
//

using System;

namespace MonoTorrent.Connections.Peer.Utp
{
    /// <summary>
    /// Configuration for <see cref="UtpSocketManager"/> and per-connection LEDBAT/reliability behaviour.
    /// </summary>
    public sealed class UtpConfig
    {
        public static UtpConfig Default { get; } = new UtpConfig ();

        /// <summary>
        /// LEDBAT target one-way delay in milliseconds (libtorrent <c>utp_target_delay</c> default: 100).
        /// Internally converted to microseconds when driving congestion control (matches libtorrent).
        /// </summary>
        public int TargetDelayMilliseconds { get; init; } = 100;

        /// <summary>
        /// LEDBAT gain factor: max bytes cwnd may grow per RTT when below target delay
        /// (libtorrent <c>utp_gain_factor</c> default: 3000).
        /// </summary>
        public int GainFactor { get; init; } = 3000;

        /// <summary>
        /// Percentage multiplier applied to cwnd on packet loss (libtorrent <c>utp_loss_multiplier</c> default: 50).
        /// </summary>
        public int LossMultiplier { get; init; } = 50;

        /// <summary>Advertised receive window in bytes.</summary>
        public int ReceiveWindow { get; init; } = 1024 * 1024;

        /// <summary>Initial/max packet size before path MTU discovery adjusts it.</summary>
        public int MaxPacketSize { get; init; } = 1500;

        /// <summary>Minimum RTO in milliseconds.</summary>
        public int MinTimeoutMilliseconds { get; init; } = 500;

        /// <summary>Initial RTO in milliseconds (before RTT samples).</summary>
        public int InitialTimeoutMilliseconds { get; init; } = 1000;

        /// <summary>SYN retransmission limit before giving up on connect.</summary>
        public int SynResends { get; init; } = 2;

        /// <summary>FIN retransmission limit.</summary>
        public int FinResends { get; init; } = 2;

        /// <summary>DATA retransmission limit before abort.</summary>
        public int NumResends { get; init; } = 3;

        /// <summary>Overall connect timeout in milliseconds.</summary>
        public int ConnectTimeoutMilliseconds { get; init; } = 30_000;

        /// <summary>Enable loss/probe driven MTU adjustment.</summary>
        public bool AllowDynamicMtu { get; init; } = true;

        /// <summary>How often the manager ticks all connections.</summary>
        public TimeSpan TickInterval { get; init; } = TimeSpan.FromMilliseconds (50);

        /// <summary>Hard limit on concurrent uTP sockets (hardening).</summary>
        public int MaxConnections { get; init; } = 200;
    }
}
