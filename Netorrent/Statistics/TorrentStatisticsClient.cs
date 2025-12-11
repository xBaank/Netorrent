namespace Netorrent.Statistics;

public class TorrentStatisticsClient(
    TransferStatistics transfer,
    PeerStatistics peers,
    CompletionTracker completion
)
{
    public TransferStatistics Transfer { get; } = transfer;
    public PeerStatistics Peers { get; } = peers;
    public CompletionTracker Completion { get; } = completion;

    internal void Dispose()
    {
        Completion.Dispose();
    }
}
