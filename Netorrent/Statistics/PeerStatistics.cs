using Netorrent.P2P;
using Netorrent.P2P.Measurement;
using ZLinq;
using ZLinq.Linq;

namespace Netorrent.Statistics;

public class PeerStatistics
{
    private readonly IReadOnlyDictionary<PeerEndpoint, PeerConnection> _peers;

    internal PeerStatistics(IReadOnlyDictionary<PeerEndpoint, PeerConnection> peers)
    {
        _peers = peers;
    }

    private ValueEnumerable<
        Where<FromEnumerable<PeerConnection>, PeerConnection>,
        PeerConnection
    > PeersNotChocking =>
        _peers.Values.AsValueEnumerable().Where(i => !i.PeerChoking && i.AmInterested);

    public int ActivePeers => PeersNotChocking.Count();
    public int TotalPeers => _peers.Values.AsValueEnumerable().Count();

    public DownloadSpeed DownloadSpeed =>
        PeersNotChocking.Sum(p => p.DownloadSpeedTracker.CurrentBps.Bps);
}
