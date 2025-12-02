namespace Netorrent.Extensions;

internal static class CancellationTokenSourceExtensions
{
    extension(CancellationTokenSource cancellationTokenSource)
    {
        public async Task CancelOnFirstCompletionAndAwaitAllAsync(IEnumerable<Task> tasks)
        {
            var finishedTask = await Task.WhenAny(tasks);
            cancellationTokenSource.Cancel();
            try
            {
                await Task.WhenAll(tasks);
            }
            catch { }

            //Throw the initial exception if there was
            await finishedTask;
        }
    }
}
