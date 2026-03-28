# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Netorrent is an async-first .NET 10.0 BitTorrent client library. It implements BEP 3, 7, 12, 15, and 23 for peer-to-peer file downloading/seeding with tracker communication over HTTP and UDP.

## Build & Test Commands

```bash
dotnet build Netorrent.sln                          # Build all projects
dotnet test Netorrent.Tests                          # Run unit tests (TUnit framework)
dotnet test Netorrent.Tests.Integration              # Run integration tests (requires Docker for Testcontainers)
dotnet test Netorrent.Tests --filter "*.ClassName.*" # Run tests in a specific class
dotnet run --project Netorrent.Pipeline              # Run the full CI pipeline (build, test, pack, publish)
dotnet run --project Netorrent.Example               # Run the example console app
```

## Code Quality

- **Formatter**: CSharpier runs automatically on build via `CSharpier.MsBuild`. No separate format command needed.
- **Nullable warnings as errors**: CS8600, CS8602, CS8603, CS8604, CS8618, CS8625 are treated as errors.
- **AOT-compatible** and **trim-safe**: The library must remain compatible with Native AOT and trimming.
- **Public API approval tests**: Changes to the public API surface require updating the approved API file at `Netorrent.Tests/PublicApi/ApiTest.My_API_Has_No_Changes.approved.txt`. Run the unit tests to generate the `.received.txt` diff, then copy it over the `.approved.txt` if the change is intentional.

## Test Frameworks

- **TUnit** for test execution (not xUnit/NUnit) - uses `[Test]`, `[Arguments]`, `[Before(Test)]` attributes
- **Shouldly** for assertions (`result.ShouldBe(expected)`)
- **Testcontainers** for integration tests (Docker-based tracker infrastructure)
- Fakes live alongside their real counterparts in test projects (e.g., `FakePeerConnection`, `FakeMessageStream`, `FakePieceStorage`)

## Architecture

### Solution Structure

| Project | Purpose |
|---|---|
| `Netorrent` | Core library |
| `Netorrent.Tests` | Unit tests |
| `Netorrent.Tests.Integration` | Integration tests (Testcontainers) |
| `Netorrent.Example` | Console demo app (Spectre.Console TUI) |
| `Netorrent.Pipeline` | CI/CD pipeline (ModularPipelines) |
| `Netorrent.Benchmarks` | Performance benchmarks |

### Core Library Organization (`Netorrent/`)

The library is organized by domain, not by architecture layer:

- **`TorrentFile/`** - Public API entry points. `TorrentClient` creates/loads torrents, `Torrent` manages a single torrent lifecycle (Check, Start, Stop).
- **`P2P/`** - Peer wire protocol. `PeersClient` coordinates connections. `TcpPeersConnector`/`TcpPeersListener` handle outbound/inbound TCP. `Download/` has `RequestScheduler` (block request coordination) and `PiecePicker` (rarity-based piece selection). `Upload/` implements the choking algorithm.
- **`Tracker/`** - Tracker communication. `TrackerClient` manages announce loops across tiers. `Http/` and `Udp/` implement the respective tracker protocols.
- **`Bencoding/`** - Bencode codec. `BDecoder` uses `PipeReader` for streaming async decode. Structs: `BString`, `BInt`, `BList`, `BDictionary`.
- **`IO/`** - Disk I/O. `DiskStorage` implements `IPieceStorage` using `FileHandle` for async random access.
- **`Statistics/`** - Observable statistics via R3 reactive properties (`DataStatistics`, `PeerStatistics`, `CheckStatistics`).
- **`ActorSystem/`** - Proto.Actor wrapper used by `RequestSchedulerActor` for concurrent request scheduling.

### Key Patterns

- **Channel-based messaging**: Bounded `Channel<T>` for inter-component communication between peers and schedulers.
- **R3 Reactive**: `ReactiveProperty<T>` and `ReadOnlyReactiveProperty<T>` for observable state (statistics, completion tracking).
- **Interface-driven**: `IPeerConnection`, `IRequestScheduler`, `IUploadScheduler`, `IPieceStorage`, `IPiecePicker` - enables faking in tests.
- **Memory efficiency**: `RentedArray<T>` for pooled memory, `ReadOnlyMemory<T>` for zero-copy, stack-allocated spans for temporary buffers.
- **Async-first**: `ValueTask` throughout, `CancellationToken` support on all public async methods.

### Data Flow

```
TorrentClient (public API)
  -> TrackerClient (discovers peers via HTTP/UDP trackers)
  -> PeersClient (manages TCP connections to peers)
     -> RequestScheduler/RequestSchedulerActor (coordinates block downloads)
     -> PiecePicker (selects pieces by rarity)
     -> UploadScheduler (choking algorithm for uploads)
  -> DiskStorage (reads/writes/verifies pieces on disk)
  -> TorrentStatisticsClient (exposes download progress, speeds, peer counts)
```

## Git Workflow

- Main branch: `master`
- Development branch: `develop` (PR target)
- CI runs on push to `master`/`develop` and PRs to `develop`
- Pipeline is defined in code (`Netorrent.Pipeline` project), not YAML steps
