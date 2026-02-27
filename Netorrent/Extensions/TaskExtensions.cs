using ZLinq;

namespace Netorrent.Extensions;

internal static class TaskExtensions
{
    extension(Task)
    {
        public static async Task RunUntilFirstCompletesAsync(
            IEnumerable<Func<CancellationToken, Task>> taskFactories,
            CancellationTokenSource cancellationTokenSource
        )
        {
            var tasks = taskFactories
                .AsValueEnumerable()
                .Select(factory => factory(cancellationTokenSource.Token))
                .ToArray();

            var firstCompleted = await Task.WhenAny(tasks).ConfigureAwait(false);

            // Cancel the rest
            cancellationTokenSource.Cancel();

            try
            {
                await Task.WhenAll(tasks).ConfigureAwait(false);
            }
            catch
            {
                // Ignore here — we rethrow the original task's exception below
            }

            // Propagate original result/exception
            await firstCompleted.ConfigureAwait(false);
        }
    }
}
