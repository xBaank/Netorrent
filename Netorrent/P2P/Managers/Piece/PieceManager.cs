using System.Collections.Concurrent;
using System.Net;
using System.Threading.Channels;
using Netorrent.P2P.Structs;

namespace Netorrent.P2P.Managers.Piece;

internal class PieceManager(
    Bitfield myBitfield,
    ConcurrentDictionary<IPEndPoint, PeerConnection> peersByIp
)
{
    private readonly Bitfield _myBitfield = myBitfield;
    private readonly ConcurrentDictionary<IPEndPoint, PeerConnection> _peersByIp = peersByIp;
    private readonly Channel<Block> _blocksToWrite = Channel.CreateBounded<Block>(
        new BoundedChannelOptions(50) { SingleWriter = true, SingleReader = true }
    );
    private readonly List<int>

    public async ValueTask AddBlockAsync(Block block)
    {
        await _blocksToWrite.Writer.WriteAsync(block);
    }

 
}
