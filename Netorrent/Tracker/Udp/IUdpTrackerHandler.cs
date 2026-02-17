using System.Net;
using Netorrent.Tracker.Udp.Response;

namespace Netorrent.Tracker.Udp
{
    internal interface IUdpTrackerHandler : IAsyncDisposable
    {
        Task<UdpTrackerConnectResponse> ConnectAsync(
            IPEndPoint endPoint,
            Guid trackerId,
            CancellationToken cancellationToken
        );
        long? GetConnectionIdOrNull(Guid trackerId);
        bool IsOutdated(long connectionId);
        int MakeTransactionId();
        Task SendAsync(
            IUdpTrackerSendPacket packet,
            Guid trackerId,
            CancellationToken cancellationToken
        );
        Task<T> SendAndReceiveAsync<T>(
            IUdpTrackerSendPacket packet,
            Guid trackerId,
            CancellationToken cancellationToken
        )
            where T : IUdpTrackerReceivePacket;
    }
}
