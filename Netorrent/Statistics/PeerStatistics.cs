using Netorrent.P2P;
using Netorrent.P2P.Measurement;
using ZLinq;
using ZLinq.Linq;

namespace Netorrent.Statistics;

public class PeerStatistics
{
    private readonly P2PClient _p2pClient;

    internal PeerStatistics(P2PClient p2PClient)
    {
        _p2pClient = p2PClient;
    }

    private ValueEnumerable<
        Where<FromEnumerable<PeerConnection>, PeerConnection>,
        PeerConnection
    > PeersNotChocking =>
        _p2pClient
            .ActivePeers.Values.AsValueEnumerable()
            .Where(i => !i.PeerChoking.Value && i.AmInterested.Value);

    public int ActivePeers => PeersNotChocking.Count();
    public int TotalPeers => _p2pClient.ActivePeers.Values.AsValueEnumerable().Count();

    public DownloadSpeed DownloadSpeed =>
        PeersNotChocking.Sum(p => p.DownloadSpeedTracker.CurrentBps.Bps);
}
