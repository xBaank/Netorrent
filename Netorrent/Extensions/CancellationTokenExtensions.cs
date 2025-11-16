namespace Netorrent.Extensions;

internal static class CancellationTokenExtensions
{
    extension(CancellationToken cancellationToken)
    {
        public CancellationTokenSource WithTimeout(TimeSpan timeout)
        {
            var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            cts.CancelAfter(timeout);
            return cts;
        }
    }
}
