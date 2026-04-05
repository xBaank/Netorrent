using System.Net;
using Netorrent.Dht;
using Netorrent.Dht.Krpc;

namespace Netorrent.Tests.Fakes;

internal sealed class FakeDhtHandler : IDhtHandler
{
    private readonly Dictionary<Type, Func<KrpcMessage, KrpcMessage>> _responseFactories = new();
    private readonly Queue<(KrpcMessage Message, IPEndPoint Remote)> _incomingQueue = new();

    public List<(KrpcMessage Message, IPEndPoint Remote)> SentMessages { get; } = [];
    public event Action<KrpcMessage, IPEndPoint>? MessageReceived;

    public void SetupResponse<TQuery>(Func<TQuery, KrpcMessage> factory)
        where TQuery : KrpcMessage
    {
        _responseFactories[typeof(TQuery)] = msg => factory((TQuery)msg);
    }

    /// <summary>
    /// Simulates an incoming message from a remote peer (fires the MessageReceived event).
    /// </summary>
    public void SimulateIncoming(KrpcMessage message, IPEndPoint from) =>
        MessageReceived?.Invoke(message, from);

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

    public IPEndPoint? StunResult { get; set; }

    public ValueTask<IPEndPoint?> DiscoverExternalEndPointAsync(
        IPEndPoint stunServer,
        CancellationToken cancellationToken
    ) => ValueTask.FromResult(StunResult);

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
