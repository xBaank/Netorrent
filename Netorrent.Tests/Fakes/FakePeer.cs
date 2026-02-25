using System.Net;
using System.Threading.Channels;
using Netorrent.IO;
using Netorrent.P2P;
using Netorrent.P2P.Messages;

namespace Netorrent.Tests.Fakes;

internal class FakePeer(
    PeerId otherPeerId,
    IPEndPoint iPEndPoint,
    Channel<IMessage> incomming,
    Channel<IMessage> outgoing
) : IPeer
{
    public FakeMessageStream FakeMessageStream { get; } = new(otherPeerId, incomming, outgoing);
    public IPEndPoint PeerEndPoint { get; } = iPEndPoint;

    public async ValueTask<IMessageStream> ConnectAsync(CancellationToken cancellationToken) =>
        FakeMessageStream;

    public ValueTask DisconnectAsync(CancellationToken cancellationToken) =>
        ValueTask.CompletedTask;
}
