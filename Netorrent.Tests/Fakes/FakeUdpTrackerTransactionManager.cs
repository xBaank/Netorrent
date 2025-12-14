using System.Collections.Concurrent;
using System.Net;
using Netorrent.Tracker.Udp;
using Netorrent.Tracker.Udp.Request;
using Netorrent.Tracker.Udp.Response;

namespace Netorrent.Tests.Fakes;

internal sealed class FakeUdpTrackerTransactionManager(IPEndPoint[] peers)
    : IUdpTrackerTransactionManager
{
    private readonly ConcurrentDictionary<Guid, long> _connections = new();

    private int _transactionId;
    private long _nextConnectionId = 1;

    public Task? TrackerManagerTask { get; } = Task.CompletedTask;

    public void Start()
    {
        // No background work required for the fake
    }

    public Task<UdpTrackerConnectResponse> ConnectAsync(
        IPEndPoint endPoint,
        Guid trackerId,
        CancellationToken cancellationToken
    )
    {
        var connectionId = _connections.GetOrAdd(
            trackerId,
            _ => Interlocked.Increment(ref _nextConnectionId)
        );

        var response = new UdpTrackerConnectResponse(MakeTransactionId(), connectionId, 0);

        return Task.FromResult(response);
    }

    public long? GetConnectionIdOrNull(Guid trackerId)
    {
        return _connections.TryGetValue(trackerId, out var id) ? id : null;
    }

    public bool IsOutdated(long connectionId)
    {
        return false;
    }

    public int MakeTransactionId()
    {
        return Interlocked.Increment(ref _transactionId);
    }

    public Task<T> SendAsync<T>(
        IUdpTrackerSendPacket packet,
        Guid trackerId,
        CancellationToken cancellationToken
    )
        where T : IUdpTrackerReceivePacket
    {
        if (packet is UdpTrackerRequest)
        {
            if (!_connections.TryGetValue(trackerId, out _))
            {
                //TODO maybe instead of a throw we should generate a UDPErrorRespose
                throw new InvalidOperationException($"No connection id for tracker {trackerId}");
            }
        }

        IUdpTrackerReceivePacket receivePacket = new UdpTrackerResponse(
            Action: 1, // Announce
            TransactionId: packet.TransactionId,
            Interval: 1800,
            Leechers: 0,
            Seeders: peers.Length,
            Peers: peers
        );
        return Task.FromResult((T)receivePacket);
    }

    public ValueTask DisposeAsync()
    {
        _connections.Clear();
        return ValueTask.CompletedTask;
    }
}
