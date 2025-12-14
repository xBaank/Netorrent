namespace Netorrent.Extensions;

internal static class TaskUtils
{
    public static async Task WhenAllOrOneThrows(params Task[] tasks)
    {
        var taskList = tasks.ToList();
        var firstFailure = new TaskCompletionSource<Exception>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );

        foreach (var task in taskList)
        {
            _ = task.ContinueWith(
                t =>
                {
                    if (t.IsFaulted)
                        firstFailure.TrySetResult(t.Exception!);
                },
                TaskContinuationOptions.ExecuteSynchronously
            );
        }

        var completed = await Task.WhenAny(Task.WhenAll(taskList), firstFailure.Task)
            .ConfigureAwait(false);

        if (completed == firstFailure.Task)
        {
            var ex = await firstFailure.Task.ConfigureAwait(false);

            // Unwrap if only one inner exception
            if (ex is AggregateException agg && agg.InnerExceptions.Count == 1)
                throw agg.InnerExceptions[0];

            throw ex;
        }
    }
}
