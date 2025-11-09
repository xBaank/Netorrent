// See https://aka.ms/new-console-template for more information
using Microsoft.Extensions.Logging;
using Netorrent.TorrentFile;
using TimeSpanXt;

ILogger logger = LoggerFactory.Create(builder => builder.AddConsole()).CreateLogger("TorrentCLI");

var torrentClient = new TorrentClient(logger: logger);
var torrent = await torrentClient.ImportTorrentAsync(
    "C:\\Users\\elrob\\Downloads\\Clair Obscur - Expedition 33 [FitGirl Repack].torrent",
    "C:\\Users\\elrob\\Downloads\\output"
);

var task = Task.Run(async () =>
{
    while (true)
    {
        logger.LogInformation(
            "Downloaded {downloaded}/{total} at {speed}, from {active}/{total} active peers",
            torrent.DownloadInfo.DownloadedBytes,
            torrent.DownloadInfo.TotalBytes,
            torrent.DownloadInfo.DownloadSpeed,
            torrent.DownloadInfo.ActivePeers,
            torrent.DownloadInfo.TotalPeers
        );
        await Task.Delay(1.Seconds());
    }
});
torrent.Start();

var taskFinished = await Task.WhenAny(torrent.DownloadInfo.DownloadTask, task);
await taskFinished;
