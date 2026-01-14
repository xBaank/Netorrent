using Netorrent.P2P.Messages;

namespace Netorrent.P2P.Upload;

internal record UploadMessage
{
    public record CheckRoundMessage : UploadMessage;

    public record RequestBlockMessage(RequestBlock RequestBlock) : UploadMessage;
}
