using Netorrent.Tracker.Http;
using Netorrent.Tracker.Udp;

namespace Netorrent.Tracker;

internal record TrackerHandlers(
    IHttpTrackerHandler? HttpTrackerHandlerIpv4,
    IUdpTrackerHandler? UdpTrackerHandlerIpv4,
    IHttpTrackerHandler? HttpTrackerHandlerIpv6,
    IUdpTrackerHandler? UdpTrackerHandlerIpv6
) : IAsyncDisposable
{
    public async ValueTask DisposeAsync()
    {
        HttpTrackerHandlerIpv4?.Dispose();
        HttpTrackerHandlerIpv6?.Dispose();

        if (UdpTrackerHandlerIpv4 is not null)
        {
            await UdpTrackerHandlerIpv4.DisposeAsync().ConfigureAwait(false);
        }
        if (UdpTrackerHandlerIpv6 is not null)
        {
            await UdpTrackerHandlerIpv6.DisposeAsync().ConfigureAwait(false);
        }
    }
}
