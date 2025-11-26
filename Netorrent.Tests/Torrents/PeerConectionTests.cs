using Imposter.Abstractions;
using Netorrent.IO;
using Netorrent.P2P;
using Netorrent.P2P.Messages;
using System.Net;

[assembly: GenerateImposter(typeof(IMessageStream))]
namespace Netorrent.Tests.Torrents;
public class PeerConectionTests
{
    [Test]
    public async Task Should_receive_unchocke() {
        var imposter = IMessageStream.Imposter();
        var bitfield = new Bitfield(5);
        var peerId = new PeerId();

        imposter.ReceiveHandshakeAsync(Arg<ReadOnlyMemory<byte>>.Any(), Arg<PeerId>.Any(), Arg<CancellationToken>.Any())
            .ReturnsAsync(peerId);

        var instance = imposter.Instance();
            

        var peerConnection = new PeerConnection(new IPEndPoint(IPAddress.Loopback,0),);
    
    }
}
