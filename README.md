# Netorrent 
![Build Status](https://github.com/xBaank/Netorrent/actions/workflows/dotnet.yml/badge.svg)
[![NuGet](https://img.shields.io/nuget/v/Netorrent?label=NuGet&color=blue)](https://www.nuget.org/packages/Netorrent)
[![NuGet Nightly](https://img.shields.io/nuget/vpre/Netorrent?label=NuGet%20Nightly&color=orange)](https://www.nuget.org/packages/Netorrent)



A async-first .NET 10.0 BitTorrent client library for downloading and seeding torrents.

## Installation

### Stable Release
```bash
dotnet add package Netorrent
```

**Requirements:**
- .NET 10.0 or higher

## Quick Start

```csharp
using Netorrent.TorrentFile;

await using var client = new TorrentClient();
await using var torrent = await client.LoadTorrentAsync(
    "path/to/file.torrent", 
    "output/directory"
);

await torrent.CheckAsync();  // Verify existing data
await torrent.StartAsync();  // Start downloading

await torrent.Completion;    // Wait for completion
```



## Usage Examples

### Statistics Monitoring

```csharp
using Netorrent.TorrentFile;

await using var client = new TorrentClient();
await using var torrent = await client.LoadTorrentAsync("file.torrent", "output");

await torrent.StartAsync();
var completionTask = torrent.Completion.AsTask();

// Monitor detailed statistics
while (!completionTask.IsCompleted)
{
    var data = torrent.Statistics.Data;
    var peers = torrent.Statistics.Peers;
    var check = torrent.Statistics.Check;
    
    Console.WriteLine($"Progress: {(double)data.Verified.Bytes / data.Total.Bytes:P1}");
    Console.WriteLine($"Download speed: {data.DownloadSpeed.BytesPerSecond} B/s");
    Console.WriteLine($"Upload speed: {data.UploadSpeed.BytesPerSecond} B/s");
    Console.WriteLine($"Connected peers: {peers.ConnectedCount}");
    Console.WriteLine($"Pieces checked: {check.CheckedPiecesCount} / {check.TotalPiecesCount}");
    
    await Task.Delay(1000);
}

await completionTask;
```

### Advanced Configuration

```csharp
using Microsoft.Extensions.Logging;
using Netorrent.TorrentFile;

var logger = LoggerFactory.Create(builder => builder.AddConsole()).CreateLogger<Netorrent.TorrentFile.TorrentClient>();

await using var client = new TorrentClient(options => options with
{
    Logger = logger,
    UsedAdressProtocol = UsedAddressProtocol.Ipv4,
    UsedTrackers = UsedTrackers.Http | UsedTrackers.Udp
});

await using var torrent = await client.LoadTorrentAsync("file.torrent", "output");

await torrent.CheckAsync();
await torrent.StartAsync();
await torrent.Completion;
```

### Torrent Creation

```csharp
using Netorrent.TorrentFile;

await using var client = new TorrentClient();

// Create torrent from single file
var torrent = await client.CreateTorrentAsync(
    "path/to/file.txt",
    "http://tracker.example.com/announce"
);

// Create torrent from directory with multiple trackers
var torrent = await client.CreateTorrentAsync(
    "path/to/directory",
    "http://main.tracker.com/announce",
    announceUrls: [["http://main.tracker.com/announce"], ["http://backup.tracker.com/announce"]],
    pieceLength: 512 * 1024  // 512KB pieces
);
```

### Stopping and Canceling Torrents

```csharp
using Netorrent.TorrentFile;

await using var client = new TorrentClient();
await using var torrent = await client.LoadTorrentAsync("file.torrent", "output");

await torrent.StartAsync();

// Request a stop (doesn't wait for ongoing operations to finish)
torrent.Stop();
Console.WriteLine("Stop requested");

// Or stop gracefully (waits for current operations to complete)
await torrent.StopAsync();
Console.WriteLine("Torrent stopped gracefully");
```

## BEP Implementation

Netorrent implements the following BitTorrent Enhancement Proposals (BEPs):

- **[BEP 3: The BitTorrent Protocol Specification](https://www.bittorrent.org/beps/bep_0003.html)** - Core BitTorrent protocol specification including metainfo files, tracker communication, and peer wire protocol
- **[BEP 7: IPv6 Tracker Extension](https://www.bittorrent.org/beps/bep_0007.html)** - IPv6 support for tracker communication and compact peer list handling
- **[BEP 12: Multitracker Metadata Extension](https://www.bittorrent.org/beps/bep_0012.html)** - Support for multiple tracker tiers and backup tracker fallback
- **[BEP 15: UDP Tracker Protocol](https://www.bittorrent.org/beps/bep_0015.html)** - UDP-based tracker protocol for reduced overhead and improved performance
- **[BEP 23: Tracker Returns Compact Peer Lists](https://www.bittorrent.org/beps/bep_0023.html)** - Compact peer list format to reduce tracker response size and improve efficiency
