namespace Netorrent.Statistics;

public class TorrentStatisticsClient(TransferStatistics transfer, PeerStatistics peers)
{
    public TransferStatistics Transfer { get; } = transfer;
    public PeerStatistics Peers { get; } = peers;
}
