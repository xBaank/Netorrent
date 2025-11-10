namespace Netorrent.TorrentFile;

public record TorrentResumeData(string InfoHash, string OutputDirectory, byte[] Bitfield);

public interface ITorrentStateStorage
{
    ValueTask SaveAsync(TorrentResumeData state, CancellationToken ct = default);
    ValueTask<TorrentResumeData?> LoadAsync(string infoHash, CancellationToken ct = default);
}
