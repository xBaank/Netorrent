using System.Net;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Netorrent.P2P;
using TimeSpanXt;

namespace Netorrent.Tracker.Http;

internal class HttpTracker(
    P2PClient p2PClient,
    HttpClient client,
    string peerId,
    byte[] infoHash,
    string announceUrl,
    ILogger logger,
    ChannelWriter<IPEndPoint> channelWriter
) : ITracker
{
    private Task? _trackerTask;
    private readonly ILogger _logger = logger;

    public Task? TrackerTask => _trackerTask;
    private CancellationTokenSource? _cancellationTokenSource;

    public void Start(CancellationToken cancellationToken = default) =>
        _trackerTask ??= AnnounceLoopTask(cancellationToken);

    private async Task AnnounceLoopTask(CancellationToken cancellationToken)
    {
        _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken
        );
        var httpTrackerResponse = await TryAnnounce(Events.Started, _cancellationTokenSource.Token);

        if (httpTrackerResponse is null)
            return;

        foreach (var iPEndPoint in httpTrackerResponse.Peers)
        {
            await channelWriter.WriteAsync(iPEndPoint, cancellationToken);
        }

        while (!_cancellationTokenSource.Token.IsCancellationRequested)
        {
            var interval = httpTrackerResponse.Interval.Seconds();
            _logger.LogTrace("Waiting {seconds} seconds", interval.TotalSeconds);

            await Task.Delay(interval, _cancellationTokenSource.Token);

            var response = await TryAnnounce(cancellationToken: _cancellationTokenSource.Token);

            if (response is null)
                return;

            foreach (var iPEndPoint in httpTrackerResponse.Peers)
            {
                await channelWriter.WriteAsync(iPEndPoint, cancellationToken);
            }
        }
    }

    private async Task<HttpTrackerResponse?> TryAnnounce(
        string? @event = null,
        CancellationToken cancellationToken = default
    )
    {
        if (_logger.IsEnabled(LogLevel.Information))
            _logger.LogInformation("Announcing to {url}", announceUrl);

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
                IPAddress.Loopback.ToString(),
                50
            );

            var response = await client.SendAsync(
                request.GenerateRequest(announceUrl),
                cancellationToken
            );
            var httpTrackerResponse = await HttpTrackerResponse.FromHttpResponseAsync(
                response,
                cancellationToken
            );
            return httpTrackerResponse;
        }
        catch (Exception ex)
        {
            if (_logger.IsEnabled(LogLevel.Debug))
                _logger.LogDebug(ex, "Couldn't announce to {trackerUrl}", announceUrl);
            return null;
        }
    }

    public async ValueTask DisposeAsync()
    {
        _cancellationTokenSource?.Cancel();
        await TryAnnounce(Events.Stopped);
    }
}
