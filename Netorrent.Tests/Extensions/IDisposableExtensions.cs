using System.Runtime.CompilerServices;

namespace Netorrent.Tests.Extensions;

public static class IDisposableExtensions
{
    extension<T>(T[] disposables)
        where T : IAsyncDisposable
    {
        public async ValueTask DisposeAsync()
        {
            foreach (var item in disposables)
            {
                await item.DisposeAsync();
            }
        }

        public TaskAwaiter GetAwaiter() => Task.CompletedTask.GetAwaiter();
    }
}
