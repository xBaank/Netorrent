using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using Netorrent.Bencoding;
using Netorrent.Bencoding.Structs;
using Netorrent.P2P;
using Netorrent.TorrentFile.FileStructure;

namespace Netorrent.Tracker;

internal class TrackerClient(HttpClient client, PeerIdService peerIdService, MetaInfo metaInfo)
{
    private readonly P2PClient _p2pClient = new(GetFreeTcpListenerInRange(6881, 6899));

    public static byte[] ComputeInfoHash(BDictionary info)
    {
        var encoder = new BEncoder();

        var infoBytes = encoder.Encode(info);
        return SHA1.HashData(infoBytes);
    }

    public async ValueTask Start(CancellationToken cancellationToken = default)
    {
        var infoHash = ComputeInfoHash(metaInfo.Info.RawInfo);
        var toDownload = (ulong)(
            metaInfo.Info.Type == InfoType.Single
                ? metaInfo.Info.Length ?? 0
                : metaInfo.Info.Files?.Sum(f => f.Length) ?? 0
        );
        var request = new HttpTrackerRequest(
            infoHash,
            peerIdService.PeerId,
            _p2pClient.Port,
            toDownload,
            0,
            toDownload,
            true,
            false,
            Events.Started
        );

        var response = await client.SendAsync(
            request.GenerateRequest(metaInfo.Announce),
            cancellationToken
        );
        var httpTrackerResponse = await HttpTrackerResponse.FromHttpResponseAsync(
            response,
            cancellationToken
        );
        Console.WriteLine(response);
    }

    static TcpListener GetFreeTcpListenerInRange(int start, int end)
    {
        for (int port = start; port <= end; port++)
        {
            try
            {
                var listener = new TcpListener(IPAddress.Any, port);
                listener.Start(); // Try to bind — this reserves the port
                return listener;
            }
            catch (SocketException)
            {
                // Port already in use — try next one
            }
        }

        throw new Exception("No free port found in the specified range.");
    }
}
