using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using Netorrent.IO;
using Netorrent.Other;
using Netorrent.P2P.Managers.Piece;
using Netorrent.P2P.Managers.Request;
using Netorrent.P2P.Structs;
using Netorrent.TorrentFile.FileStructure;

namespace Netorrent.P2P;

internal class P2PClient(
    MetaInfo metaInfo,
    string peerId,
    FileManager fileManager,
    Bitfield bitField
) : IDisposable
{
    private readonly TcpListener _listener = Tcp.GetFreeTcpListenerInRange(6881, 6899);
    private readonly MetaInfo _metaInfo = metaInfo;
    private readonly Random _rng = new();
    private readonly ConcurrentDictionary<IPEndPoint, PeerConnection> _knowPeers = [];
    private readonly List<(
        Task peerTask,
        CancellationTokenSource cancellationTokenSource
    )> peerTasks = [];

    public FileManager FileManager { get; } = fileManager;

    private IEnumerable<Bitfield> Bitfields => _knowPeers.Values.Select(i => i.PeerBitField);
    public IPEndPoint EndPoint => (IPEndPoint)_listener.LocalEndpoint;

    public async Task ConnectToPeerAsync(
        IPEndPoint iPEndPoint,
        CancellationToken cancellationToken = default
    )
    {
        if (_knowPeers.ContainsKey(iPEndPoint))
            return;

        var client = new TcpClient();
        await client.ConnectAsync(iPEndPoint, cancellationToken);

        var peerConnection = new PeerConnection(
            client,
            iPEndPoint,
            bitField,
            FileManager,
            new RequestManager(),
            new PieceManager()
        );

        _knowPeers[iPEndPoint] = peerConnection;

        await peerConnection.PerformHandshakeAsync(
            _metaInfo.Info.InfoHash,
            peerId,
            cancellationToken
        );
        await peerConnection.SendBitfieldAsync(bitField, cancellationToken);
        var cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken
        );
        SetPieceToDownload(peerConnection);
        peerConnection.OnPieceDownloadedAsync += PieceDownloadedAsync;
        var peerTask = HandlePeer(peerConnection, cancellationTokenSource.Token);
        peerTasks.Add((peerTask, cancellationTokenSource));
    }

    public async Task ListenForPeersAsync(CancellationToken cancellationToken = default)
    {
        _listener.Start();
        while (!cancellationToken.IsCancellationRequested)
        {
            var tcpClient = await _listener.AcceptTcpClientAsync(cancellationToken);
            var remoteEndPoint = (IPEndPoint)tcpClient.Client.RemoteEndPoint!;
            var peerConnection = new PeerConnection(
                tcpClient,
                remoteEndPoint,
                bitField,
                FileManager,
                new RequestManager(),
                new PieceManager()
            );

            _knowPeers[remoteEndPoint] = peerConnection;

            await peerConnection.ReceiveHandshakeAsync(
                _metaInfo.Info.InfoHash,
                peerId,
                cancellationToken
            );
            var cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(
                cancellationToken
            );
            SetPieceToDownload(peerConnection);
            peerConnection.OnPieceDownloadedAsync += PieceDownloadedAsync;
            var peerTask = HandlePeer(peerConnection, cancellationTokenSource.Token);
            peerTasks.Add((peerTask, cancellationTokenSource));
        }
    }

    private void SetPieceToDownload(PeerConnection peerConnection)
    {
        var excluded = _knowPeers
            .Values.Where(i => i != peerConnection)
            .Select(pc => pc.CurrentPieceDownloading)
            .Where(piece => piece.HasValue)
            .Select(piece => piece!.Value)
            .ToHashSet();
        peerConnection.SetCurrentPieceToDownload(GetNextRarestPiece(excluded));
    }

    private Task PieceDownloadedAsync(int pieceIndex, PeerConnection peerConnection)
    {
        SetPieceToDownload(peerConnection);
        return Task.CompletedTask;
    }

    private static async Task HandlePeer(
        PeerConnection peerConnection,
        CancellationToken cancellationToken
    )
    {
        var outgoing = peerConnection.WriteLoop(cancellationToken);
        var incoming = peerConnection.ReadLoop(cancellationToken);
        await Task.WhenAll(outgoing, incoming);
    }

    public Dictionary<int, int> GetPieceAvailability()
    {
        int pieceCount = bitField.Length;
        var availability = new int[pieceCount];

        foreach (var peerBits in Bitfields)
        {
            for (int i = 0; i < pieceCount; i++)
            {
                if (peerBits[i])
                    availability[i]++;
            }
        }

        var dict = new Dictionary<int, int>(pieceCount);
        for (int i = 0; i < pieceCount; i++)
        {
            if (availability[i] > 0 && !bitField[i])
                dict[i] = availability[i];
        }

        return dict;
    }

    public int? GetNextRarestPiece(HashSet<int>? excluded = null, int rarestSampleSize = 5)
    {
        var availability = GetPieceAvailability();

        if (excluded is not null)
        {
            foreach (var ex in excluded)
                availability.Remove(ex);
        }

        if (availability.Count == 0)
            return null;

        var sorted = availability.OrderBy(kv => kv.Value).ToList();
        var subset = sorted.Take(rarestSampleSize).ToList();
        int choiceIndex = _rng.Next(subset.Count);
        return subset[choiceIndex].Key;
    }

    public void Dispose()
    {
        _listener.Stop();
        foreach (var (_, cancellationTokenSource) in peerTasks)
        {
            cancellationTokenSource.Cancel();
        }
        foreach (var item in _knowPeers)
        {
            item.Value.OnPieceDownloadedAsync -= PieceDownloadedAsync;
            item.Value.Dispose();
        }
    }
}
