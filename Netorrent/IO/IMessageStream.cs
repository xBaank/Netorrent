using Netorrent.P2P.Messages;

namespace Netorrent.IO;

internal delegate ValueTask MessageHandler(Message message, CancellationToken cancellationToken);

internal interface IMessageStream : IAsyncDisposable
{
    public Handshake Handshake { get; }
    public ValueTask SendAsync(Message message, CancellationToken cancellationToken);
    public bool TrySend(Message message);
    public Task StartAsync(MessageHandler messageHandler, CancellationToken cancellationToken);
}
