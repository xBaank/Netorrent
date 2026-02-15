using Netorrent.P2P.Download;

internal class FakePeerRequestWindow : PeerRequestWindow
{
    public FakePeerRequestWindow()
        : base(16384) { }

    public new ulong MaxInFlightRequests => 16;
}
