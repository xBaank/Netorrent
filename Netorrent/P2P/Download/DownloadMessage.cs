using Netorrent.P2P.Messages;

namespace Netorrent.P2P.Download;

//TODO pool these messages
internal record DownloadMessage
{
    public record BlockMessage(Block Block) : DownloadMessage;

    public record CheckTimeoutMessage : DownloadMessage;

    public record ScheduleMessage(IPeerConnection PeerConnection) : DownloadMessage;
}
