using Netorrent.P2P;
using Netorrent.P2P.Measurement;
using ZLinq;
using ZLinq.Linq;

namespace Netorrent.Statistics;

public class PeerStatistics
{
    private readonly PeersClient _peersClient;

    internal PeerStatistics(PeersClient peersClient)
    {
        _peersClient = peersClient;
    }

    private ValueEnumerable<
        Where<FromEnumerable<PeerConnection>, PeerConnection>,
        PeerConnection
    > PeersNotChocking =>
        _peersClient
            .ActivePeers.Values.AsValueEnumerable()
            .Where(i => !i.PeerChoking.Value && i.AmInterested.Value);

    public int ActivePeers => PeersNotChocking.Count();
    public int TotalPeers => _peersClient.ActivePeers.Values.AsValueEnumerable().Count();

    public DownloadSpeed DownloadSpeed => PeersNotChocking.Sum(p => p.DownloadTracker.Speed.Bps);
}
