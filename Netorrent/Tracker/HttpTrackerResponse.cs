using System.Net;
using System.Text;
using Netorrent.Bencoding;
using Netorrent.Bencoding.Structs;
using Netorrent.Extensions;

namespace Netorrent.Tracker;

internal class HttpTrackerResponse
{
    public int Interval { get; private set; }
    public int MinInterval { get; private set; }
    public int Complete { get; private set; }
    public int Incomplete { get; private set; }
    public List<IPEndPoint> Peers { get; } = [];

    public static async Task<HttpTrackerResponse> FromHttpResponseAsync(
        HttpResponseMessage response,
        CancellationToken cancellationToken = default
    )
    {
        response.EnsureSuccessStatusCode();

        var bytes = await response.Content.ReadAsByteArrayAsync(cancellationToken);
        var decoder = new BDecoder(bytes.AsSpan());
        var root = decoder.Decode();

        if (root is not BDictionary dict)
            throw new InvalidDataException("Tracker response is not a dictionary.");

        var trackerResponse = new HttpTrackerResponse();

        if (dict.Elements.TryGetValue("failure reason", out var failureReason))
            throw new Exception(failureReason.As<BString>()?.Data);

        // Try to extract basic fields
        if (dict.Elements.TryGetValue("interval", out var interval))
            trackerResponse.Interval = (int)((BInt)interval).Data;
        else
            throw new Exception();

        if (dict.Elements.TryGetValue("min interval", out var minInt))
            trackerResponse.MinInterval = (int)((BInt)minInt).Data;

        if (dict.Elements.TryGetValue("complete", out var comp))
            trackerResponse.Complete = (int)((BInt)comp).Data;
        else
            throw new Exception();

        if (dict.Elements.TryGetValue("incomplete", out var incomp))
            trackerResponse.Incomplete = (int)((BInt)incomp).Data;
        else
            throw new Exception();

        // Parse peers
        if (dict.Elements.TryGetValue(new BString("peers"), out var peersVal))
        {
            if (peersVal is BString peersString)
            {
                // Compact mode (binary)
                trackerResponse.Peers.AddRange(ParseCompactPeers(peersString.RawData));
            }
            else if (peersVal is BList peersList)
            {
                // Non-compact mode (list of dictionaries)
                foreach (var peerObj in peersList.Elements)
                {
                    if (
                        peerObj is BDictionary peerDict
                        && peerDict.Elements.TryGetValue(new BString("ip"), out var ipVal)
                        && peerDict.Elements.TryGetValue(new BString("port"), out var portVal)
                    )
                    {
                        var ip = IPAddress.Parse(((BString)ipVal).Data);
                        var port = (int)((BInt)portVal).Data;
                        trackerResponse.Peers.Add(new IPEndPoint(ip, port));
                    }
                }
            }
        }

        return trackerResponse;
    }

    private static List<IPEndPoint> ParseCompactPeers(byte[] bytes)
    {
        if (bytes.Length % 6 != 0 && bytes.Length != 0)
            throw new InvalidDataException("Invalid compact peer list length.");

        var peers = new List<IPEndPoint>();
        for (int i = 0; i < bytes.Length; i += 6)
        {
            var ip = new IPAddress([bytes[i], bytes[i + 1], bytes[i + 2], bytes[i + 3]]);
            var port = (bytes[i + 4] << 8) | bytes[i + 5];
            peers.Add(new IPEndPoint(ip, port));
        }

        return peers;
    }
}
