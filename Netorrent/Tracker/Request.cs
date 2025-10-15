using System.Net;
using System.Text;

namespace Netorrent.Tracker;

internal class Request(
    string InfoHash,
    string PeerId,
    int Port,
    ulong Downloaded,
    ulong Uploaded,
    ulong Left,
    bool Compact,
    bool NoPeerId,
    string Event,
    string? IpAddress = null,
    int? NumWant = null,
    string? Key = null,
    string? TrackerId = null
)
{
    private static string UrlEncode(byte[] bytes)
    {
        // Percent-encode bytes per BitTorrent spec
        var sb = new StringBuilder(bytes.Length * 3);
        foreach (var b in bytes)
            sb.Append('%').Append(b.ToString("X2"));
        return sb.ToString();
    }

    public HttpRequestMessage GenerateRequest(string trackerUrl)
    {
        var uriBuilder = new StringBuilder();
        uriBuilder.Append(trackerUrl);
        uriBuilder.Append(trackerUrl.Contains('?') ? '&' : '?');

        uriBuilder.Append($"info_hash={UrlEncode(Encoding.ASCII.GetBytes(InfoHash))}");
        uriBuilder.Append($"&peer_id={WebUtility.UrlEncode(PeerId)}");
        uriBuilder.Append($"&port={Port}");
        uriBuilder.Append($"&uploaded={Uploaded}");
        uriBuilder.Append($"&downloaded={Downloaded}");
        uriBuilder.Append($"&left={Left}");
        uriBuilder.Append($"&compact={(Compact ? 1 : 0)}");
        uriBuilder.Append($"&no_peer_id={(NoPeerId ? 1 : 0)}");

        if (!string.IsNullOrEmpty(Event))
            uriBuilder.Append($"&event={WebUtility.UrlEncode(Event)}");

        if (!string.IsNullOrEmpty(IpAddress))
            uriBuilder.Append($"&ip={WebUtility.UrlEncode(IpAddress)}");

        if (NumWant.HasValue)
            uriBuilder.Append($"&numwant={NumWant.Value}");

        if (!string.IsNullOrEmpty(Key))
            uriBuilder.Append($"&key={WebUtility.UrlEncode(Key)}");

        if (!string.IsNullOrEmpty(TrackerId))
            uriBuilder.Append($"&trackerid={WebUtility.UrlEncode(TrackerId)}");

        return new HttpRequestMessage(HttpMethod.Get, uriBuilder.ToString());
    }
}
