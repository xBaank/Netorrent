namespace Netorrent.Tracker;

internal abstract record TrackerMessage
{
    internal record AnnounceMessage(string? Event) : TrackerMessage;

    internal record CompletedMessage : TrackerMessage;
}
