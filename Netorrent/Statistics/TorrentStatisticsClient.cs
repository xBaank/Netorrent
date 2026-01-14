namespace Netorrent.Statistics;

public class TorrentStatisticsClient(
    DataStatistics data,
    PeerStatistics peers,
    CheckStatistics check
)
{
    public DataStatistics Data { get; } = data;
    public PeerStatistics Peers { get; } = peers;
    public CheckStatistics Check { get; } = check;
}
