using System.Reactive.Linq;
using Netorrent.Tests.Extensions;
using Netorrent.Tests.P2P;
using NSubstitute;

namespace Netorrent.Tests.Extensions;

public static class NSubstituteExtensions
{
    extension<T>(T substitute)
        where T : class
    {
        public async Task WaitForCallAsync(
            Action<T> expression,
            CancellationToken cancellationToken
        )
        {
            var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            cancellationToken.Register(() => tcs.TrySetCanceled());

            substitute.When(expression).Do(_ => tcs.TrySetResult());

            await tcs.Task;
        }

        public async Task WaitForCallAsync(
            Func<T, Task> expression,
            CancellationToken cancellationToken
        )
        {
            var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            cancellationToken.Register(() => tcs.TrySetCanceled());

            substitute.When(expression).Do(_ => tcs.TrySetResult());

            await tcs.Task;
        }
    }
}
