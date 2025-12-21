using Netorrent.P2P;
using Netorrent.P2P.Messages;

namespace Netorrent.IO;

internal interface IHandshakeStream : IAsyncDisposable
{
    public ValueTask<Handshake> PerformHandshakeAsync(
        ReadOnlyMemory<byte> infoHash,
        PeerId peerId,
        CancellationToken cancellationToken
    );

    public ValueTask<Handshake> ReceiveHandshakeAsync(
        ICollection<ReadOnlyMemory<byte>> infoHashes,
        PeerId peerId,
        CancellationToken cancellationToken
    );
}
