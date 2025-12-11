// See https://aka.ms/new-console-template for more information
using Microsoft.Extensions.Logging;
using Netorrent.TorrentFile;

ILogger logger = LoggerFactory.Create(builder => builder.AddConsole()).CreateLogger("TorrentCLI");

var torrentClient = new TorrentClient(o => o with { Logger = logger });
await using var torrent = await torrentClient.ImportTorrentAsync(
    "C:\\Users\\elrob\\Downloads\\debian-13.2.0-amd64-netinst.iso.torrent",
    "D:\\output"
);

var task = Task.Run(async () =>
{
    while (true)
    {
        logger.LogInformation(
            "Downloaded {downloaded}/{total} at {speed}, from {active}/{total} active peers",
            torrent.Stadistics.DownloadedBytes,
            torrent.Stadistics.TotalBytes,
            torrent.Stadistics.DownloadSpeed,
            torrent.Stadistics.ActivePeers,
            torrent.Stadistics.TotalPeers
        );
        await Task.Delay(1000);
    }
});
await torrent.StartAsync();

var taskFinished = await Task.WhenAny(torrent.Stadistics.DownloadTask, task);
await taskFinished;
