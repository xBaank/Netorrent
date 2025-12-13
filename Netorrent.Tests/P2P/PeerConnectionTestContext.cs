using System.Net;
using System.Threading.Channels;
using Netorrent.P2P;
using Netorrent.P2P.Download;
using Netorrent.P2P.Messages;
using Netorrent.P2P.Upload;
using Netorrent.Tests.Fakes;
using NSubstitute;

namespace Netorrent.Tests.P2P;

internal sealed class PeerConnectionTestContext : IAsyncDisposable
{
    public Bitfield LocalBitfield { get; }
    public PeerId PeerId { get; } = new();
    public Channel<Message> Incoming { get; } = Channel.CreateUnbounded<Message>();
    public Channel<Message> Outgoing { get; } = Channel.CreateUnbounded<Message>();
    public FakeMessageStream Stream { get; }
    public IUploadScheduler UploadMock { get; }
    public IRequestScheduler RequestMock { get; }
    public PeerConnection Peer { get; }

    public PeerConnectionTestContext(
        Bitfield? bitfield = null,
        bool amChocking = true,
        bool amInterested = false,
        bool peerChocking = true,
        bool peerInterested = false
    )
    {
        LocalBitfield = bitfield ?? new Bitfield(5);
        UploadMock = Substitute.For<IUploadScheduler>();
        RequestMock = Substitute.For<IRequestScheduler>();

        Stream = new FakeMessageStream(PeerId, Incoming, Outgoing);

        Peer = new PeerConnection(
            new PeerEndpoint(new IPEndPoint(IPAddress.Loopback, 0), new PeerId()),
            LocalBitfield,
            UploadMock,
            RequestMock,
            Stream,
            peerChoking: peerChocking,
            amChoking: amChocking,
            peerInterested: peerInterested,
            amInterested: amInterested
        );
    }

    public Task StartAsync(CancellationToken ct) => Peer.StartAsync(ct);

    public ValueTask DisposeAsync() => Peer.DisposeAsync();
}
