// See https://aka.ms/new-console-template for more information
using Microsoft.Extensions.Logging;
using Netorrent.TorrentFile;
using TimeSpanXt;

Console.WriteLine("Hello, World!");
ILogger logger = LoggerFactory.Create(builder => builder.AddConsole()).CreateLogger("TorrentCLI");

var torrentClient = new TorrentClient(logger: logger);
var torrent = await torrentClient.AddTorrentAsync(
    "D:\\repos\\Netorrent\\Netorrent.Tests\\Data\\nosferatu.torrent",
    "C:\\Users\\elrob\\Downloads\\output"
);

var task = Task.Run(async () =>
{
    while (true)
    {
        logger.LogInformation("Donwload speed {speed}/kbps", torrent.TotalDownloadSpeedKbps);
        await Task.Delay(1.Seconds());
    }
});
await torrent.StartAsync();
await task;
