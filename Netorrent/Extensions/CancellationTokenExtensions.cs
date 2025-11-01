namespace Netorrent.Extensions;

internal static class CancellationTokenExtensions
{
    public static CancellationTokenSource WithTimeout(
        this CancellationToken cancellationToken,
        TimeSpan timeout
    )
    {
        var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(timeout);
        return cts;
    }
}
