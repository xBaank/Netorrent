using System.Threading.Channels;

namespace Netorrent.Extensions;

internal static class ChannelExtensions
{
    extension<T>(ChannelWriter<T> source)
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
                if (item is IDisposable disposable)
                {
                    disposable.Dispose();
                }

                throw;
            }
        }

        public bool TryWriteOrDispose(T item)
        {
            if (source.TryWrite(item))
            {
                return true;
            }
            if (item is IDisposable disposable)
            {
                disposable.Dispose();
            }
            return false;
        }
    }
}
