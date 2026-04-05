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
    private IPEndPoint? _externalEndPoint;

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
        // Discover external endpoint via STUN before contacting bootstrap nodes
        _externalEndPoint = await DiscoverExternalEndPointViaNatAsync(ct).ConfigureAwait(false);
        if (_externalEndPoint is not null && _logger.IsEnabled(LogLevel.Information))
            _logger.LogInformation("DHT STUN: external endpoint is {ep}", _externalEndPoint);

        foreach (var bootstrapNode in _options.BootstrapNodes)
        {
            try
            {
                var addresses = await Dns.GetHostAddressesAsync(bootstrapNode.Host, ct)
                    .ConfigureAwait(false);
                foreach (var address in addresses)
                {
                    if (address.AddressFamily != AddressFamily.InterNetwork)
                        continue;
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
            catch (Exception ex)
            {
                if (_logger.IsEnabled(LogLevel.Debug))
                    _logger.LogDebug(
                        ex,
                        "DNS resolution failed for bootstrap node {host}",
                        bootstrapNode.Host
                    );
            }
        }

        await IterativeFindNodeAsync(_selfId, ct).ConfigureAwait(false);
    }

    private async ValueTask<IPEndPoint?> DiscoverExternalEndPointViaNatAsync(CancellationToken ct)
    {
        foreach (var stunNode in _options.StunServers)
        {
            try
            {
                var addresses = await Dns.GetHostAddressesAsync(stunNode.Host, ct)
                    .ConfigureAwait(false);
                foreach (var address in addresses)
                {
                    if (address.AddressFamily != AddressFamily.InterNetwork)
                        continue;
                    var ep = new IPEndPoint(address, stunNode.Port);
                    var result = await _handler
                        .DiscoverExternalEndPointAsync(ep, ct)
                        .ConfigureAwait(false);
                    if (result is not null)
                        return result;
                }
            }
            catch (OperationCanceledException)
            {
                return null;
            }
            catch (Exception ex)
            {
                if (_logger.IsEnabled(LogLevel.Debug))
                    _logger.LogDebug(ex, "STUN DNS resolution failed for {host}", stunNode.Host);
            }
        }
        return null;
    }

    // ── Iterative find_node (Kademlia lookup) ─────────────────────────────────

    private async ValueTask IterativeFindNodeAsync(NodeId target, CancellationToken ct)
    {
        const int Alpha = 3;
        var queried = new HashSet<NodeId>();
        var candidates = new List<DhtNode>(_routingTable.GetClosest(target));

        bool progress = true;
        while (progress && !ct.IsCancellationRequested)
        {
            progress = false;
            var toQuery = candidates.Where(n => !queried.Contains(n.Id)).Take(Alpha).ToList();
            if (toQuery.Count == 0)
                break;

            foreach (var node in toQuery)
            {
                queried.Add(node.Id);
                try
                {
                    var query = new KrpcMessage.FindNodeQuery(
                        TransactionId.Generate(),
                        _selfId,
                        target
                    );
                    var response = await _handler
                        .SendAndReceiveAsync(query, node.EndPoint, ct)
                        .ConfigureAwait(false);
                    if (response is KrpcMessage.FindNodeResponse findResp)
                    {
                        foreach (var newNode in findResp.Nodes)
                        {
                            if (
                                !queried.Contains(newNode.Id)
                                && !candidates.Any(c => c.Id == newNode.Id)
                            )
                            {
                                candidates.Add(newNode);
                                InsertNode(newNode);
                                progress = true;
                            }
                        }
                    }
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    if (_logger.IsEnabled(LogLevel.Debug))
                        _logger.LogDebug(ex, "find_node failed for {ep}", node.EndPoint);
                }
            }
        }
    }

    // ── Periodic get_peers ────────────────────────────────────────────────────

    private async ValueTask GetPeersAsync(CancellationToken ct)
    {
        var target = NodeId.FromInfoHash(_infoHash);
        var closest = _routingTable.GetClosest(target);

        if (closest.Count == 0)
        {
            // Routing table empty: re-bootstrap
            await BootstrapAsync(ct).ConfigureAwait(false);
            return;
        }

        foreach (var node in closest)
        {
            if (ct.IsCancellationRequested)
                return;
            try
            {
                var query = new KrpcMessage.GetPeersQuery(
                    TransactionId.Generate(),
                    _selfId,
                    _infoHash
                );
                var response = await _handler
                    .SendAndReceiveAsync(query, node.EndPoint, ct)
                    .ConfigureAwait(false);
                switch (response)
                {
                    case KrpcMessage.GetPeersWithPeersResponse peersResp:
                        foreach (var ep in peersResp.Peers)
                            _peersChannel.TryWrite(ep);
                        await AnnounceAsync(node.EndPoint, peersResp.Token, ct)
                            .ConfigureAwait(false);
                        break;
                    case KrpcMessage.GetPeersWithNodesResponse nodesResp:
                        foreach (var n in nodesResp.Nodes)
                            InsertNode(n);
                        await AnnounceAsync(node.EndPoint, nodesResp.Token, ct)
                            .ConfigureAwait(false);
                        break;
                }
                _routingTable.RefreshNode(node.Id);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                if (_logger.IsEnabled(LogLevel.Debug))
                    _logger.LogDebug(ex, "get_peers failed for {ep}", node.EndPoint);
            }
        }
    }

    // ── Announce ──────────────────────────────────────────────────────────────

    private async ValueTask AnnounceAsync(IPEndPoint remote, byte[] token, CancellationToken ct)
    {
        if (token.Length == 0)
            return;
        try
        {
            // When STUN succeeded we know our external IP, so peers can reach us at our
            // TCP listen port.  When STUN failed we set implied_port=true and let the
            // remote node use the source address/port of this UDP packet instead.
            var query = new KrpcMessage.AnnouncePeerQuery(
                TransactionId.Generate(),
                _selfId,
                _infoHash,
                _tcpListenPort,
                token,
                ImpliedPort: _externalEndPoint is null
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
