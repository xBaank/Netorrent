using Netorrent.P2P.Messages;

namespace Netorrent.P2P.Download;

//TODO pool these messages
internal record DownloadMessage
{
    public record BlockMessage(Block Block) : DownloadMessage, IDisposable
    {
        public void Dispose() => Block.Dispose();
    }

    public record CheckTimeoutMessage : DownloadMessage;

    public record ScheduleMessage(IPeerConnection PeerConnection) : DownloadMessage;
}
