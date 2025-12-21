using System.Threading.Channels;
using Netorrent.P2P.Messages;

namespace Netorrent.IO;

internal interface IMessageStream : IAsyncDisposable
{
    public ChannelReader<Message> IncomingMessages { get; }
    public ChannelWriter<Message> OutgoingMessages { get; }
    public Task StartAsync(CancellationToken cancellationToken);
}
