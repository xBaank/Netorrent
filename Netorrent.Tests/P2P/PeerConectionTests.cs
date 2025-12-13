using System.Buffers;
using System.Reactive.Linq;
using Netorrent.Extensions;
using Netorrent.IO;
using Netorrent.Other;
using Netorrent.P2P;
using Netorrent.P2P.Messages;
using Netorrent.Tests.Extensions;
using NSubstitute;
using NSubstitute.ReceivedExtensions;
using Shouldly;

namespace Netorrent.Tests.P2P;

/*
[Timeout(5_000)]
public class PeerConectionTests
{
    public async Task Peers_should_handshake()
    {
        var bitfield = new Bitfield(10, true);
        var receiver = await PeerConnection.CreatePeerConnectionAsync(bitfield,);
        var initiator = await PeerConnection.CreatePeerConnectionAsync();
    }
}
*/
