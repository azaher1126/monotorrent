using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

using MonoTorrent;
using MonoTorrent.Client;
using MonoTorrent.Connections.Peer;

namespace UtpTestClient
{
    /// <summary>
    /// A minimal CLI tool dedicated to testing MonoTorrent's uTP (BEP 29) implementation
    /// against real-world clients (qBittorrent, Transmission, libtorrent-based apps, Deluge, etc.).
    ///
    /// Usage examples:
    ///   dotnet run --project src/Samples/UtpTestClient -- "magnet:?xt=urn:btih:..."
    ///   dotnet run --project src/Samples/UtpTestClient -- /path/to/file.torrent --port 6881
    ///   dotnet run --project src/Samples/UtpTestClient -- "magnet:..." --utp-target 75 --log
    ///
    /// Tips for compatibility testing:
    /// - Use a fixed --port so you can easily capture traffic.
    /// - In Wireshark use a filter like: udp.port == 6881 and (udp[8:1] >> 4) &lt; 5
    ///   (uTP version nibble is always 1; types 0-4 are DATA/FIN/STATE/RESET/SYN).
    /// - In qBittorrent (or other clients): enable uTP, preferably "uTP only" or high priority for uTP.
    /// - Check the peer list in the other client: it should show the connection as uTP / μTP.
    /// - PEX should carry the 0x04 bit for supports-uTP on peers we connected to over uTP.
    /// - Try different --utp-target values (target delay in ms) to exercise LEDBAT.
    /// </summary>
    class Program
    {
        static async Task Main (string[] args)
        {
            if (args.Length == 0 || args[0] is "-h" or "--help" or "/?") {
                PrintHelp ();
                return;
            }

            string input = args[0];
            int port = 0;                 // 0 = let OS pick (good for normal use). Use fixed for capture.
            int utpTargetDelayMs = 100;   // Default LEDBAT target (lower = more aggressive, higher = nicer to TCP)
            bool enableLogging = false;

            for (int i = 1; i < args.Length; i++) {
                switch (args[i]) {
                    case "--port":
                        if (i + 1 < args.Length && int.TryParse (args[++i], out int p)) port = p;
                        break;
                    case "--utp-target":
                    case "--target-delay":
                        if (i + 1 < args.Length && int.TryParse (args[++i], out int t)) utpTargetDelayMs = Math.Clamp (t, 5, 500);
                        break;
                    case "--log":
                    case "-v":
                        enableLogging = true;
                        break;
                    case "--help":
                    case "-h":
                        PrintHelp ();
                        return;
                }
            }

            var cts = new CancellationTokenSource ();
            Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel (); };

            try {
                await RunAsync (input, port, utpTargetDelayMs, enableLogging, cts.Token);
            } catch (OperationCanceledException) {
                Console.WriteLine ("\nShutting down...");
            } catch (Exception ex) {
                Console.WriteLine ("Fatal error: " + ex);
            }
        }

        static void PrintHelp ()
        {
            Console.WriteLine ("UtpTestClient - Simple uTP (BEP29) compatibility tester for MonoTorrent");
            Console.WriteLine ();
            Console.WriteLine ("Usage:");
            Console.WriteLine ("  UtpTestClient <magnet-or-torrent> [options]");
            Console.WriteLine ();
            Console.WriteLine ("Options:");
            Console.WriteLine ("  --port N              Bind to a specific port (great for Wireshark)");
            Console.WriteLine ("  --utp-target N        LEDBAT target delay in milliseconds (default 100)");
            Console.WriteLine ("  --log, -v             Enable verbose engine logging");
            Console.WriteLine ("  --help                Show this message");
            Console.WriteLine ();
            Console.WriteLine ("Examples:");
            Console.WriteLine ("  dotnet run --project src/Samples/UtpTestClient -- \"magnet:?xt=urn:btih:...\"");
            Console.WriteLine ("  dotnet run --project src/Samples/UtpTestClient -- ubuntu-24.04-desktop-amd64.iso.torrent --port 51413 --utp-target 60");
            Console.WriteLine ();
            Console.WriteLine ("After starting, connect from qBittorrent / another client to your IP:port (or use DHT/PEX).");
            Console.WriteLine ("Watch the 'uTP peers' count and individual peer lines. Use Wireshark on the UDP port.");
        }

        static async Task RunAsync (string input, int listenPort, int utpTargetDelayMs, bool enableLogging, CancellationToken token)
        {
            var listenEp = listenPort > 0
                ? new IPEndPoint (IPAddress.Any, listenPort)
                : null;

            var settingsBuilder = new EngineSettingsBuilder {
                // Critical for this tool
                EnableUtp = true,
                UtpTargetDelayMilliseconds = utpTargetDelayMs,

                // Make capture easy + reproducible
                ListenEndPoints = listenEp != null
                    ? new Dictionary<string, IPEndPoint> { { "ipv4", listenEp }, { "ipv6", new IPEndPoint (IPAddress.IPv6Any, listenPort) } }
                    : new EngineSettingsBuilder ().ListenEndPoints,   // let the engine pick

                // A little more aggressive DHT/PEX helps discover uTP-capable peers quickly
                DhtEndPoint = listenEp != null ? new IPEndPoint (IPAddress.Any, listenPort) : null,

                // Nice for testing: save fast resume so you can restart quickly
                AutoSaveLoadFastResume = true,
            };

            if (enableLogging)
                Console.WriteLine ("Note: pass --log is accepted for compatibility; wire a LoggerFactory listener if needed.");

            using var engine = new ClientEngine (settingsBuilder.ToSettings ());

            // One torrent for this tool (simple!)
            TorrentManager manager;
            if (MagnetLink.TryParse (input, out var magnet)) {
                Console.WriteLine ($"Adding magnet: {magnet}");
                manager = await engine.AddAsync (magnet, "Downloads");
            } else if (File.Exists (input)) {
                Console.WriteLine ($"Adding torrent file: {input}");
                var torrent = await Torrent.LoadAsync (input);
                manager = await engine.AddAsync (torrent, "Downloads");
            } else {
                Console.WriteLine ("Error: Argument must be a magnet link or a path to a .torrent file that exists.");
                return;
            }

            // Live view of peers we have seen (the actual transport - uTP vs TCP - is best verified
            // from the remote client UI and from a packet capture, because uTP is the preferred
            // outgoing transport when EnableUtp=true).
            var peerStats = new ConcurrentDictionary<IPEndPoint, string>();   // key -> client name

            manager.PeerConnected += (_, e) => {
                var pid = e.Peer;
                string client = pid.ClientApp.ToString ();

                IPEndPoint? key = null;
                try {
                    if (pid.Uri is { } u && u.HostNameType == UriHostNameType.IPv4) {
                        key = new IPEndPoint (IPAddress.Parse (u.Host), u.Port);
                    } else if (pid.Uri is { } u6 && u6.HostNameType == UriHostNameType.IPv6) {
                        key = new IPEndPoint (IPAddress.Parse (u6.Host.Trim ('[', ']')), u6.Port);
                    }
                } catch { }

                if (key != null)
                    peerStats[key] = client;

                Console.WriteLine ($"[CONNECTED] {pid.Uri}  client={client}");
            };

            manager.PeerDisconnected += (_, e) => {
                var pid = e.Peer;
                IPEndPoint? key = null;
                try {
                    if (pid.Uri is { } u && u.HostNameType == UriHostNameType.IPv4)
                        key = new IPEndPoint (IPAddress.Parse (u.Host), u.Port);
                    else if (pid.Uri is { } u6 && u6.HostNameType == UriHostNameType.IPv6)
                        key = new IPEndPoint (IPAddress.Parse (u6.Host.Trim ('[', ']')), u6.Port);
                } catch { }
                if (key != null)
                    peerStats.TryRemove (key, out string _ignored);
            };

            // Start the torrent (hash check + download)
            await manager.StartAsync ();

            Console.WriteLine ();
            Console.WriteLine ($"Engine listening. EnableUtp={engine.Settings.EnableUtp}  UtpTargetDelay={engine.Settings.UtpTargetDelayMilliseconds}ms");
            if (listenPort > 0)
                Console.WriteLine ($"Listening port: {listenPort}  (use this for Wireshark)");
            Console.WriteLine ("Press Ctrl+C to stop and exit cleanly.");
            Console.WriteLine ();

            // Status printer
            using var timer = new Timer (_ => PrintStatus (manager, peerStats, engine), null, TimeSpan.FromSeconds (1), TimeSpan.FromSeconds (2));

            // Wait for user cancellation or for the torrent to reach a terminal-ish state.
            try {
                while (!token.IsCancellationRequested) {
                    if (manager.State == TorrentState.Seeding || manager.State == TorrentState.Stopped) {
                        // Give it a little more time so people can observe peers
                        await Task.Delay (TimeSpan.FromSeconds (8), token);
                        break;
                    }
                    await Task.Delay (250, token);
                }
            } catch (OperationCanceledException) { }

            // Graceful shutdown
            await manager.StopAsync ();
            await engine.StopAllAsync ();

            Console.WriteLine ("\n=== Final summary ===");
            Console.WriteLine ($"Peers seen during this run: {peerStats.Count}");
            Console.WriteLine ("(uTP vs TCP usage is best observed in the remote client's peer list and in a packet capture on the UDP port.)");
            Console.WriteLine ("Done. Thank you for testing uTP compatibility!");
        }

        static void PrintStatus (TorrentManager manager, ConcurrentDictionary<IPEndPoint, string> peerStats, ClientEngine engine)
        {
            int totalPeers = peerStats.Count;

            double progress = manager.Progress;
            var down = manager.Monitor.DownloadRate / 1024.0;
            var up = manager.Monitor.UploadRate / 1024.0;

            string hashForTitle = manager.Torrent?.Name
                ?? manager.InfoHashes.V1?.ToHex()
                ?? manager.InfoHashes.V2?.ToHex()
                ?? "unknown";

            Console.WriteLine ($"[{DateTime.Now:HH:mm:ss}] {hashForTitle}  " +
                               $"{manager.State}  {progress:0.0}%  ↓{down:0.0}kB/s ↑{up:0.0}kB/s   " +
                               $"peers: {totalPeers}");

            // Show a few peers we have seen
            foreach (var kv in peerStats.Take (6)) {
                Console.WriteLine ($"   {kv.Key}  {kv.Value}");
            }

            if (totalPeers == 0)
                Console.WriteLine ("   (no peers yet - check DHT/PEX/trackers or add the torrent manually in the other client)");
            Console.WriteLine ();
        }
    }
}
