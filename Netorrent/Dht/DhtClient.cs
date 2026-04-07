using System.Net;
using System.Net.Sockets;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Netorrent.ActorSystem;
using Netorrent.Dht.Krpc;
using Netorrent.Dht.Routing;
using Netorrent.Extensions;
using Netorrent.TorrentFile.FileStructure;

namespace Netorrent.Dht;

internal sealed class DhtClient : IAsyncDisposable
{
    private readonly NodeId _selfId;
    private readonly InfoHash _infoHash;
    private readonly IDhtHandler _handler;
    private readonly ChannelWriter<IPEndPoint> _peersChannel;
    private readonly DhtClientOptions _options;
    private readonly int _tcpListenPort;
    private readonly ILogger _logger;
    private readonly Actor<DhtMessage> _actor = new();
    private readonly RoutingTable _routingTable;
    private readonly KrpcTokenStore _tokenStore = new();

    public DhtClient(
        NodeId selfId,
        InfoHash infoHash,
        IDhtHandler handler,
        ChannelWriter<IPEndPoint> peersChannel,
        DhtClientOptions options,
        int tcpListenPort,
        ILogger logger
    )
    {
        _selfId = selfId;
        _infoHash = infoHash;
        _handler = handler;
        _peersChannel = peersChannel;
        _options = options;
        _tcpListenPort = tcpListenPort;
        _logger = logger;
        _routingTable = new RoutingTable(selfId);
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _handler.MessageReceived += OnMessageReceived;

        _actor.Tell(new DhtMessage.BootstrapMessage());

        // Periodic get_peers (delay + interval configurable, e.g. for testing)
        _actor.ScheduleRepeatedly(
            _options.GetPeersDelay,
            _options.GetPeersInterval,
            new DhtMessage.GetPeersMessage()
        );

        // Bucket refresh
        _actor.ScheduleRepeatedly(
            _options.RefreshInterval,
            _options.RefreshInterval,
            new DhtMessage.RefreshBucketsMessage()
        );

        // Token rotation every 5 minutes
        _actor.ScheduleRepeatedly(5.Minutes, 5.Minutes, new DhtMessage.RotateTokensMessage());

        await _actor.StartAsync(OnReceiveAsync, cancellationToken).ConfigureAwait(false);
    }

    private void OnMessageReceived(KrpcMessage message, IPEndPoint remote) =>
        _actor.Tell(new DhtMessage.IncomingMessage(message, remote));

    private async ValueTask OnReceiveAsync(DhtMessage message, CancellationToken ct)
    {
        switch (message)
        {
            case DhtMessage.BootstrapMessage:
                await BootstrapAsync(ct).ConfigureAwait(false);
                break;
            case DhtMessage.GetPeersMessage:
                await GetPeersAsync(ct).ConfigureAwait(false);
                break;
            case DhtMessage.RefreshBucketsMessage:
                await RefreshBucketsAsync(ct).ConfigureAwait(false);
                break;
            case DhtMessage.RotateTokensMessage:
                _tokenStore.RotateSecret();
                break;
            case DhtMessage.PingNodeMessage pingMsg:
                await HandleEvictionPingAsync(pingMsg.Node, ct).ConfigureAwait(false);
                break;
            case DhtMessage.IncomingMessage incoming:
                await HandleIncomingAsync(incoming.Message, incoming.Remote, ct)
                    .ConfigureAwait(false);
                break;
        }
    }

    // ── Bootstrap ─────────────────────────────────────────────────────────────

    private async ValueTask BootstrapAsync(CancellationToken ct)
    {
        foreach (var bootstrapNode in _options.BootstrapNodes)
        {
            try
            {
                var addresses = await ResolveIpv4Async(bootstrapNode.Host, ct)
                    .ConfigureAwait(false);
                foreach (var address in addresses)
                {
                    var ep = new IPEndPoint(address, bootstrapNode.Port);
                    var query = new KrpcMessage.FindNodeQuery(
                        TransactionId.Generate(),
                        _selfId,
                        _selfId
                    );
                    try
                    {
                        var response = await _handler
                            .SendAndReceiveAsync(query, ep, ct)
                            .ConfigureAwait(false);
                        if (response is KrpcMessage.FindNodeResponse findResp)
                        {
                            foreach (var node in findResp.Nodes)
                                InsertNode(node);
                        }
                    }
                    catch (Exception ex) when (ex is not OperationCanceledException)
                    {
                        if (_logger.IsEnabled(LogLevel.Debug))
                            _logger.LogDebug(
                                ex,
                                "Bootstrap node {host}:{port} did not respond",
                                bootstrapNode.Host,
                                bootstrapNode.Port
                            );
                    }
                }
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }

        _logger.LogInformation(
            "DHT bootstrap: seeded routing table to {n} nodes, starting iterative find_node",
            _routingTable.Count
        );
        await IterativeFindNodeAsync(_selfId, ct).ConfigureAwait(false);
        _logger.LogInformation(
            "DHT bootstrap: complete, routing table now has {n} nodes",
            _routingTable.Count
        );
    }

    // ── Iterative Kademlia lookups ────────────────────────────────────────────

    private const int Alpha = 3;
    private const int K = 8;

    // Hard cap on lookup rounds. Each round queries Alpha nodes in parallel; with the handler
    // configured for ~6s worst-case-per-query timeouts, this caps a single iterative lookup at
    // ~MaxRounds * 6s ≈ 1 minute, after which we yield the actor back to its mailbox.
    private const int MaxRounds = 10;

    private static readonly IComparer<NodeId> _nodeIdByteComparer = Comparer<NodeId>.Create(
        static (a, b) => a.CompareTo(b)
    );

    private async ValueTask IterativeFindNodeAsync(NodeId target, CancellationToken ct)
    {
        var queried = new HashSet<NodeId>();
        var shortlist = new Dictionary<NodeId, DhtNode>();
        foreach (var n in _routingTable.GetClosest(target, K))
            shortlist[n.Id] = n;

        for (int round = 0; round < MaxRounds && !ct.IsCancellationRequested; round++)
        {
            var toQuery = shortlist
                .Values.Where(n => !queried.Contains(n.Id))
                .OrderBy(n => n.Id.Xor(target), _nodeIdByteComparer)
                .Take(Alpha)
                .ToList();
            if (toQuery.Count == 0)
                break;

            foreach (var n in toQuery)
                queried.Add(n.Id);

            var responses = await Task.WhenAll(
                    toQuery.Select(async n =>
                    {
                        try
                        {
                            var query = new KrpcMessage.FindNodeQuery(
                                TransactionId.Generate(),
                                _selfId,
                                target
                            );
                            return await _handler
                                    .SendAndReceiveAsync(query, n.EndPoint, ct)
                                    .ConfigureAwait(false) as KrpcMessage.FindNodeResponse;
                        }
                        catch (Exception ex) when (ex is not OperationCanceledException)
                        {
                            if (_logger.IsEnabled(LogLevel.Debug))
                                _logger.LogDebug(ex, "find_node failed for {ep}", n.EndPoint);
                            return null;
                        }
                    })
                )
                .ConfigureAwait(false);

            foreach (var resp in responses)
            {
                if (resp is null)
                    continue;
                foreach (var newNode in resp.Nodes)
                {
                    if (newNode.Id == _selfId)
                        continue;
                    InsertNode(newNode);
                    shortlist.TryAdd(newNode.Id, newNode);
                }
            }

            // Trim shortlist to the 2K closest known nodes so it doesn't grow unbounded
            if (shortlist.Count > K * 2)
            {
                shortlist = shortlist
                    .Values.OrderBy(n => n.Id.Xor(target), _nodeIdByteComparer)
                    .Take(K * 2)
                    .ToDictionary(n => n.Id);
            }
        }
    }

    // ── Periodic get_peers (iterative Kademlia lookup for the info hash) ─────

    private async ValueTask GetPeersAsync(CancellationToken ct)
    {
        var target = NodeId.FromInfoHash(_infoHash);
        var seeds = _routingTable.GetClosest(target, K);
        _logger.LogInformation(
            "DHT get_peers: starting iterative lookup ({seeds} seeds, routing table {rt})",
            seeds.Count,
            _routingTable.Count
        );

        if (seeds.Count == 0)
        {
            // Routing table empty: re-bootstrap and retry
            await BootstrapAsync(ct).ConfigureAwait(false);
            seeds = _routingTable.GetClosest(target, K);
            if (seeds.Count == 0)
                return;
        }

        var queried = new HashSet<NodeId>();
        var shortlist = new Dictionary<NodeId, DhtNode>();
        foreach (var n in seeds)
            shortlist[n.Id] = n;

        var seenPeers = new HashSet<long>();
        // Nodes that returned a valid token, ordered by proximity — announce to the K closest after convergence
        var announceTargets = new List<(DhtNode Node, byte[] Token)>();

        for (int round = 0; round < MaxRounds && !ct.IsCancellationRequested; round++)
        {
            var toQuery = shortlist
                .Values.Where(n => !queried.Contains(n.Id))
                .OrderBy(n => n.Id.Xor(target), _nodeIdByteComparer)
                .Take(Alpha)
                .ToList();
            if (toQuery.Count == 0)
                break;

            foreach (var n in toQuery)
                queried.Add(n.Id);

            var responses = await Task.WhenAll(
                    toQuery.Select(async n =>
                    {
                        try
                        {
                            var query = new KrpcMessage.GetPeersQuery(
                                TransactionId.Generate(),
                                _selfId,
                                _infoHash
                            );
                            var resp = await _handler
                                .SendAndReceiveAsync(query, n.EndPoint, ct)
                                .ConfigureAwait(false);
                            return (Node: n, Response: (KrpcMessage?)resp);
                        }
                        catch (Exception ex) when (ex is not OperationCanceledException)
                        {
                            if (_logger.IsEnabled(LogLevel.Debug))
                                _logger.LogDebug(ex, "get_peers failed for {ep}", n.EndPoint);
                            return (Node: n, Response: (KrpcMessage?)null);
                        }
                    })
                )
                .ConfigureAwait(false);

            foreach (var (node, resp) in responses)
            {
                switch (resp)
                {
                    case KrpcMessage.GetPeersWithPeersResponse peersResp:
                        _routingTable.RefreshNode(peersResp.ResponderId);
                        foreach (var ep in peersResp.Peers)
                        {
                            if (ep.Port == 0)
                                continue;
                            var key = PeerKey(ep);
                            if (seenPeers.Add(key))
                                _peersChannel.TryWrite(ep);
                        }
                        if (peersResp.Token.Length > 0)
                            announceTargets.Add((node, peersResp.Token));
                        break;

                    case KrpcMessage.GetPeersWithNodesResponse nodesResp:
                        _routingTable.RefreshNode(nodesResp.ResponderId);
                        if (nodesResp.Token.Length > 0)
                            announceTargets.Add((node, nodesResp.Token));
                        foreach (var newNode in nodesResp.Nodes)
                        {
                            if (newNode.Id == _selfId)
                                continue;
                            InsertNode(newNode);
                            shortlist.TryAdd(newNode.Id, newNode);
                        }
                        break;
                }
            }

            if (shortlist.Count > K * 2)
            {
                shortlist = shortlist
                    .Values.OrderBy(n => n.Id.Xor(target), _nodeIdByteComparer)
                    .Take(K * 2)
                    .ToDictionary(n => n.Id);
            }
        }

        _logger.LogInformation(
            "DHT get_peers: complete, found {peers} unique peers, queried {q} nodes",
            seenPeers.Count,
            queried.Count
        );

        // Announce to the K closest responsive nodes that handed us a token
        var closestAnnounce = announceTargets
            .OrderBy(t => t.Node.Id.Xor(target), _nodeIdByteComparer)
            .Take(K)
            .ToList();
        foreach (var (node, token) in closestAnnounce)
        {
            if (ct.IsCancellationRequested)
                break;
            await AnnounceAsync(node.EndPoint, token, ct).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Resolves a host to its IPv4 addresses with a hard 5-second timeout. We restrict to IPv4
    /// because <see cref="Dns.GetHostAddressesAsync(string, CancellationToken)"/> requests both
    /// A and AAAA records, and the AAAA query can hang indefinitely on networks without
    /// functional IPv6 DNS, ignoring the cancellation token (a known .NET / OS-level limitation).
    /// </summary>
    private async ValueTask<IPAddress[]> ResolveIpv4Async(string host, CancellationToken ct)
    {
        try
        {
            return await Dns.GetHostAddressesAsync(host, AddressFamily.InterNetwork, ct)
                .WaitAsync(TimeSpan.FromSeconds(5), ct)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            if (_logger.IsEnabled(LogLevel.Debug))
                _logger.LogDebug(ex, "DNS resolution failed for {host}", host);
            return [];
        }
    }

    private static long PeerKey(IPEndPoint ep)
    {
        // Pack IPv4 + port into a single long for O(1) dedup
        Span<byte> buf = stackalloc byte[4];
        var addr = ep.Address.GetAddressBytes();
        if (addr.Length == 4)
            addr.CopyTo(buf);
        return ((long)System.Buffers.Binary.BinaryPrimitives.ReadUInt32BigEndian(buf) << 16)
            | (uint)ep.Port;
    }

    // ── Announce ──────────────────────────────────────────────────────────────

    private async ValueTask AnnounceAsync(IPEndPoint remote, byte[] token, CancellationToken ct)
    {
        if (token.Length == 0)
            return;
        try
        {
            var query = new KrpcMessage.AnnouncePeerQuery(
                TransactionId.Generate(),
                _selfId,
                _infoHash,
                _tcpListenPort,
                token,
                ImpliedPort: false
            );
            await _handler.SendAsync(query, remote, ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            if (_logger.IsEnabled(LogLevel.Debug))
                _logger.LogDebug(ex, "announce_peer failed for {ep}", remote);
        }
    }

    // ── Incoming query handler ─────────────────────────────────────────────────

    private async ValueTask HandleIncomingAsync(
        KrpcMessage message,
        IPEndPoint remote,
        CancellationToken ct
    )
    {
        switch (message)
        {
            case KrpcMessage.PingQuery ping:
                var pongResp = new KrpcMessage.PingResponse(ping.TransactionId, _selfId);
                await _handler.SendAsync(pongResp, remote, ct).ConfigureAwait(false);
                InsertNode(new DhtNode(ping.SenderId, remote));
                break;

            case KrpcMessage.FindNodeQuery findNode:
                var closest = _routingTable.GetClosest(findNode.Target);
                var findResp = new KrpcMessage.FindNodeResponse(
                    findNode.TransactionId,
                    _selfId,
                    closest
                );
                await _handler.SendAsync(findResp, remote, ct).ConfigureAwait(false);
                InsertNode(new DhtNode(findNode.SenderId, remote));
                break;

            case KrpcMessage.GetPeersQuery getPeers:
                var token = _tokenStore.GenerateToken(remote.Address);
                var nearNodes = _routingTable.GetClosest(NodeId.FromInfoHash(getPeers.InfoHash));
                var getPeersResp = new KrpcMessage.GetPeersWithNodesResponse(
                    getPeers.TransactionId,
                    _selfId,
                    token,
                    nearNodes
                );
                await _handler.SendAsync(getPeersResp, remote, ct).ConfigureAwait(false);
                InsertNode(new DhtNode(getPeers.SenderId, remote));
                break;

            case KrpcMessage.AnnouncePeerQuery announce:
                if (_tokenStore.ValidateToken(remote.Address, announce.Token))
                {
                    var announceResp = new KrpcMessage.AnnouncePeerResponse(
                        announce.TransactionId,
                        _selfId
                    );
                    await _handler.SendAsync(announceResp, remote, ct).ConfigureAwait(false);
                    // Optionally write the peer to the channel
                    var port = announce.ImpliedPort ? remote.Port : announce.Port;
                    _peersChannel.TryWrite(new IPEndPoint(remote.Address, port));
                }
                break;

            // Responses — routing table refresh only (TCS already resolved in DhtHandler)
            case KrpcMessage.PingResponse pingResp:
                _routingTable.RefreshNode(pingResp.ResponderId);
                break;
            case KrpcMessage.FindNodeResponse fnResp:
                _routingTable.RefreshNode(fnResp.ResponderId);
                break;
            case KrpcMessage.GetPeersWithPeersResponse gpPeers:
                _routingTable.RefreshNode(gpPeers.ResponderId);
                break;
            case KrpcMessage.GetPeersWithNodesResponse gpNodes:
                _routingTable.RefreshNode(gpNodes.ResponderId);
                break;
        }
    }

    // ── Eviction ping ─────────────────────────────────────────────────────────

    private async ValueTask HandleEvictionPingAsync(DhtNode candidate, CancellationToken ct)
    {
        try
        {
            var query = new KrpcMessage.PingQuery(TransactionId.Generate(), _selfId);
            var response = await _handler
                .SendAndReceiveAsync(query, candidate.EndPoint, ct)
                .ConfigureAwait(false);
            if (response is KrpcMessage.PingResponse)
            {
                candidate.LastSeen = DateTime.UtcNow;
                _routingTable.GetBucketFor(candidate.Id).ConfirmAlive(candidate.Id);
            }
        }
        catch (TimeoutException)
        {
            candidate.IsBad = true;
            _routingTable.GetBucketFor(candidate.Id).Evict(candidate.Id);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            candidate.IsBad = true;
            _routingTable.GetBucketFor(candidate.Id).Evict(candidate.Id);
            if (_logger.IsEnabled(LogLevel.Debug))
                _logger.LogDebug(ex, "Eviction ping failed for {ep}", candidate.EndPoint);
        }
    }

    // ── Bucket refresh ────────────────────────────────────────────────────────

    private async ValueTask RefreshBucketsAsync(CancellationToken ct)
    {
        foreach (var (_, target) in _routingTable.GetStaleRefreshTargets())
        {
            if (ct.IsCancellationRequested)
                return;
            await IterativeFindNodeAsync(target, ct).ConfigureAwait(false);
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private void InsertNode(DhtNode node)
    {
        var evictCandidate = _routingTable.TryInsert(node);
        if (evictCandidate is not null)
            _actor.Tell(new DhtMessage.PingNodeMessage(evictCandidate));
    }

    public async ValueTask DisposeAsync()
    {
        _handler.MessageReceived -= OnMessageReceived;
        await _actor.DisposeAsync().ConfigureAwait(false);
        await _handler.DisposeAsync().ConfigureAwait(false);
    }
}
