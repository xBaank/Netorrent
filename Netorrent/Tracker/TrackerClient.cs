using System.Net;
using System.Security.Cryptography;
using System.Text;
using Netorrent.Bencoding;
using Netorrent.Bencoding.Structs;
using Netorrent.Extensions;
using Netorrent.P2P;
using Netorrent.TorrentFile.FileStructure;

namespace Netorrent.Tracker;

internal class TrackerClient(HttpClient client, PeerIdService peerIdService, MetaInfo metaInfo)
{
    public static byte[] ComputeInfoHash(BDictionary info)
    {
        var encoder = new BEncoder();

        var infoBytes = encoder.Encode(info);
        return SHA1.HashData(infoBytes);
    }

    public static List<Peer> DecodeCompactPeers(byte[] peers)
    {
        if (peers.Length % 6 != 0)
            throw new ArgumentException("Invalid peers length: must be a multiple of 6 bytes.");

        var list = new List<Peer>();
        for (int i = 0; i < peers.Length; i += 6)
        {
            // IP: first 4 bytes
            var ipBytes = new byte[4];
            Array.Copy(peers, i, ipBytes, 0, 4);
            var ip = new IPAddress(ipBytes);

            // Port: last 2 bytes, big-endian
            int port = (peers[i + 4] << 8) | peers[i + 5];

            list.Add(new Peer(ip, port));
        }

        return list;
    }

    public async ValueTask GetPeers(CancellationToken cancellationToken = default)
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
            6899,
            toDownload,
            0,
            toDownload,
            true,
            false,
            "started"
        );

        var response = await client.SendAsync(
            request.GenerateRequest(metaInfo.Announce),
            cancellationToken
        );
        var content = await response.Content.ReadAsByteArrayAsync(cancellationToken);
        var decoder = new BDecoder(content);
        var decoded = decoder.Decode();
        //TODO parse response
        var peersRaw =
            decoded.As<BDictionary>()?.Elements["peers"]?.As<BString>()
            ?? throw new InvalidDataException();

        var peers = DecodeCompactPeers(Encoding.ASCII.GetBytes(peersRaw));

        Console.WriteLine(peers);
    }
}
