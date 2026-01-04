using Netorrent.Other;

namespace Netorrent.Extensions;

public static class SemaphoreSlimExtensions
{
    extension(SemaphoreSlim semaphoreSlim)
    {
        public async Task<SemaphoreSlimDisposable> LockAsync(CancellationToken cancellationToken)
        {
            await semaphoreSlim.WaitAsync(cancellationToken).ConfigureAwait(false);
            return new SemaphoreSlimDisposable(semaphoreSlim);
        }
    }
}
