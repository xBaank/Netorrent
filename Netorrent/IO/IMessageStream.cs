using Netorrent.P2P.Messages;

namespace Netorrent.IO;

internal delegate ValueTask MessageHandler(IMessage message, CancellationToken cancellationToken);

internal interface IMessageStream : IAsyncDisposable
{
    public Handshake Handshake { get; }
    public ValueTask SendAsync(IMessage message, CancellationToken cancellationToken);
    public bool TrySend(IMessage message);
    public Task StartAsync(MessageHandler messageHandler, CancellationToken cancellationToken);
}
