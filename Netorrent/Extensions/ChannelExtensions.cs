using System.Threading.Channels;

namespace Netorrent.Extensions;

internal static class ChannelExtensions
{
    extension<T>(ChannelWriter<T> source)
        where T : IDisposable
    {
        public async ValueTask WriteOrDisposeAsync(
            T item,
            CancellationToken cancellationToken = default
        )
        {
            try
            {
                await source.WriteAsync(item, cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                item.Dispose();
                throw;
            }
        }

        public bool TryWriteOrDispose(T item)
        {
            if (source.TryWrite(item))
            {
                return true;
            }

            item.Dispose();
            return false;
        }
    }
}
