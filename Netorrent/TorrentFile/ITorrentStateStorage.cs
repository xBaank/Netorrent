using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Netorrent.TorrentFile;

public record TorrentResumeData(string InfoHash, string OutputDirectory, byte[] Bitfield);

public interface ITorrentStateStorage
{
    ValueTask SaveAsync(TorrentResumeData state, CancellationToken ct = default);
    ValueTask<TorrentResumeData?> LoadAsync(string infoHash, CancellationToken ct = default);
}
