using System.Threading.Channels;

namespace Netorrent.Extensions;

internal static class ChannelReaderExtensions
{
    extension<T>(ChannelReader<T> source)
        where T : IDisposable
    {
        public IAsyncEnumerable<T> ReadAllWithDispose => source.ReadAndDisposeLeftovers();

        public async IAsyncEnumerable<T> ReadAndDisposeLeftovers()
        {
            try
            {
                await foreach (var item in source.ReadAllAsync())
                {
                    yield return item;
                }
            }
            finally
            {
                // Dispose anything left in the channel when enumeration ends
                while (source.TryRead(out var leftover))
                    leftover.Dispose();
            }
        }
    }
}
