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
    ChannelWriter<HttpTrackerResponse> channelWriter
) : IAsyncDisposable
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
        var httpTrackerResponse = await Announce(Events.Started, _cancellationTokenSource.Token);

        if (httpTrackerResponse is null)
            return;

        await channelWriter.WriteAsync(httpTrackerResponse, cancellationToken);

        while (!_cancellationTokenSource.Token.IsCancellationRequested)
        {
            var interval = httpTrackerResponse.Interval.Seconds();
            _logger.LogTrace("Waiting {seconds} seconds", interval.TotalSeconds);

            await Task.Delay(interval, _cancellationTokenSource.Token);

            var response = await Announce(cancellationToken: _cancellationTokenSource.Token);

            if (response is null)
                return;

            await channelWriter.WriteAsync(httpTrackerResponse, cancellationToken);
        }
    }

    private async Task<HttpTrackerResponse?> Announce(
        string? @event = null,
        CancellationToken cancellationToken = default
    )
    {
        _logger.LogInformation("Announcing to {url}", announceUrl);

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
            NumWant: 50
        );

        try
        {
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
            _logger.LogDebug(ex, "Couldn't announce to {trackerUrl}", announceUrl);
            return null;
        }
    }

    public async ValueTask DisposeAsync()
    {
        _cancellationTokenSource?.Cancel();
        try
        {
            await Announce(Events.Stopped);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Error stopping the tracker");
        }
    }
}
