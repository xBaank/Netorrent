using System.Net;
using System.Threading.Channels;
using Netorrent.Dht;
using Netorrent.Dht.Krpc;

namespace Netorrent.Tests.Fakes;

internal sealed class FakeDhtHandler : IDhtHandler
{
    private readonly Dictionary<Type, Func<KrpcMessage, KrpcMessage>> _responseFactories = new();
    private readonly Channel<(KrpcMessage Message, IPEndPoint Remote)> _incoming =
        Channel.CreateUnbounded<(KrpcMessage, IPEndPoint)>();

    public List<(KrpcMessage Message, IPEndPoint Remote)> SentMessages { get; } = [];

    public ChannelReader<(KrpcMessage Message, IPEndPoint Remote)> IncomingMessages =>
        _incoming.Reader;

    public void SetupResponse<TQuery>(Func<TQuery, KrpcMessage> factory)
        where TQuery : KrpcMessage
    {
        _responseFactories[typeof(TQuery)] = msg => factory((TQuery)msg);
    }

    /// <summary>
    /// Simulates an incoming message from a remote peer (publishes to the inbound channel).
    /// </summary>
    public void SimulateIncoming(KrpcMessage message, IPEndPoint from) =>
        _incoming.Writer.TryWrite((message, from));

    public ValueTask SendAsync(
        KrpcMessage message,
        IPEndPoint remote,
        CancellationToken cancellationToken
    )
    {
        SentMessages.Add((message, remote));
        return ValueTask.CompletedTask;
    }

    public ValueTask<KrpcMessage> SendAndReceiveAsync(
        KrpcMessage query,
        IPEndPoint remote,
        CancellationToken cancellationToken
    )
    {
        SentMessages.Add((query, remote));

        if (_responseFactories.TryGetValue(query.GetType(), out var factory))
            return ValueTask.FromResult(factory(query));

        throw new InvalidOperationException(
            $"No response configured for query type {query.GetType().Name}"
        );
    }

    public ValueTask DisposeAsync()
    {
        _incoming.Writer.TryComplete();
        return ValueTask.CompletedTask;
    }
}
