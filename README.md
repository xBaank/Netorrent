# Netorrent

A high-performance, async-first .NET 10.0 BitTorrent client library for downloading and seeding torrents.

## Installation

### Stable Release
```bash
dotnet add package Netorrent
```

### Nightly Builds (for testing latest features)
```bash
dotnet add package Netorrent --version 1.0.0-nightly-*
```

### Preview Releases
```bash
dotnet add package Netorrent --prerelease
```

**Requirements:**
- .NET 10.0 or higher
- Dependencies automatically included:
  - Microsoft.Extensions.Logging.Abstractions
  - R3 (Reactive Extensions)
  - ZLinq (High-performance LINQ)

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

// Monitor detailed statistics
while (!torrent.Completion.IsCompleted)
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
    "http://primary.tracker.com/announce",
    announceUrls: ["http://backup1.tracker.com/announce", "http://backup2.tracker.com/announce"],
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

### Performance Considerations

The library is designed with async-first architecture for optimal performance:
- Channel-based communication between components
- Memory pooling for efficient buffer management
- Concurrent collections for thread-safe operations
- Minimal allocations in hot paths

## Package Versions

### 📦 Stable Releases
- **Purpose**: Production-ready versions with stable APIs
- **Versioning**: Semantic Versioning (e.g., 1.0.0, 1.1.0, 1.0.1)
- **Updates**: Bug fixes and new features
- **Installation**: `dotnet add package Netorrent`

### 🌙 Nightly Builds
- **Purpose**: Latest code from every commit to develop branch
- **Versioning**: Timestamped with commit hash (e.g., `1.0.0-nightly-20250114-1430-a1b2c3d`)
- **Updates**: Per-commit builds for immediate testing
- **Installation**: `dotnet add package Netorrent --version 1.0.0-nightly-*`
- **Warning**: May contain breaking changes or bugs

### 🚀 Preview Releases
- **Purpose**: Pre-release testing of upcoming features
- **Versioning**: Pre-release suffix (e.g., `1.0.0-preview-123`)
- **Updates**: Periodic builds from master branch
- **Installation**: `dotnet add package Netorrent --prerelease`

### Version Selection Strategies

**For Production Applications:**
```xml
<PackageReference Include="Netorrent" Version="[1.0.0,2.0.0)" />
```

**For Testing Latest Features:**
```xml
<PackageReference Include="Netorrent" Version="1.0.0-nightly-*" />
```

**For Early Adopters:**
```xml
<PackageReference Include="Netorrent" Version="1.0.0-*" />
```

## CI/CD Integration

The project uses GitHub Actions for automated publishing:

- **Per-commit nightly builds** from `develop` branch
- **Release builds** from git tags
- **Manual publishing** for custom versions
- **Comprehensive testing** before all publications

See [`.github/workflows/`](.github/workflows/) for complete workflow configurations.

## Features

- [x] **Torrent files** - Complete .torrent file support
- [x] **HTTP Trackers** - Full HTTP tracker protocol implementation
- [x] **Peer Wire Protocol** - TCP peer communication
- [x] **UDP Trackers** - High-performance UDP tracker support
- [x] **Piece Verification** - SHA-1 hash verification of downloaded pieces
- [x] **Multi-tracker Support** - Primary and backup tracker support
- [x] **Resume Downloads** - Support for partially downloaded torrents
- [x] **Real-time Statistics** - Comprehensive download/upload monitoring
- [x] **Async/Await Support** - Modern async-first API design
- [x] **Cancellation Support** - Full CancellationToken integration
- [x] **Torrent Creation** - Create torrents from files and directories
- [ ] **μTP Protocol** - Micro Transport Protocol (BEP 0029)
- [ ] **DHT Support** - Distributed Hash Table (BEP 0005)
- [ ] **Message Encryption** - Protocol encryption (BEP 0008)
- [ ] **UPnP/PMP** - NAT traversal for incoming connections
- [ ] **Magnet Links** - URI-based torrent identification
- [ ] **Endgame Mode** - Optimized piece downloading for completion
