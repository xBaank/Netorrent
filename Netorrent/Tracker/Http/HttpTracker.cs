using System.Net;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Netorrent.Extensions;
using Netorrent.P2P;

namespace Netorrent.Tracker.Http;

internal class HttpTracker(
    P2PClient p2PClient,
    HttpClient client,
    PeerId peerId,
    byte[] infoHash,
    string announceUrl,
    ILogger logger,
    ChannelWriter<IPEndPoint> channelWriter,
    IPAddress? forcedIp
) : ITracker
{
    public async ValueTask StartAsync(CancellationToken cancellationToken)
    {
        var response = await TryAnnounceAsync(Events.Started, cancellationToken)
            .ConfigureAwait(false);

        if (response is null)
            return;

        foreach (var iPEndPoint in response.Peers)
        {
            await channelWriter.WriteAsync(iPEndPoint, cancellationToken).ConfigureAwait(false);
        }

        while (!cancellationToken.IsCancellationRequested)
        {
            var interval = response.Interval.Seconds;

            if (logger.IsEnabled(LogLevel.Information))
                logger.LogInformation("Waiting {seconds} seconds", interval.TotalSeconds);

            await Task.Delay(interval, cancellationToken).ConfigureAwait(false);

            var newResponse = await TryAnnounceAsync(cancellationToken: cancellationToken)
                .ConfigureAwait(false);

            if (newResponse is null)
                continue;

            response = newResponse;

            foreach (var iPEndPoint in response.Peers)
            {
                await channelWriter.WriteAsync(iPEndPoint, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private async Task<HttpTrackerResponse?> TryAnnounceAsync(
        string? @event = null,
        CancellationToken cancellationToken = default
    )
    {
        if (logger.IsEnabled(LogLevel.Information))
            logger.LogInformation("Announcing to {url}", announceUrl);

        try
        {
            var request = new HttpTrackerRequest(
                infoHash,
                peerId,
                p2PClient.EndPoint.Port,
                p2PClient.FileManager.GetWrittenBytes(),
                0, //TODO Implement this in p2pclient
                p2PClient.FileManager.GetMissingBytes(),
                true,
                false,
                @event,
                forcedIp?.ToString(),
                50
            );

            var response = await client
                .SendAsync(request.GenerateRequest(announceUrl), cancellationToken)
                .ConfigureAwait(false);
            var httpTrackerResponse = await HttpTrackerResponse
                .FromHttpResponseAsync(response, cancellationToken)
                .ConfigureAwait(false);
            return httpTrackerResponse;
        }
        catch (Exception ex)
        {
            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebug(ex, "Couldn't announce to {trackerUrl}", announceUrl);
            return null;
        }
    }

    public async ValueTask DisposeAsync()
    {
        await TryAnnounceAsync(Events.Stopped).ConfigureAwait(false);
    }
}
