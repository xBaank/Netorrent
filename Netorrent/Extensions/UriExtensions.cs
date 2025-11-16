namespace Netorrent.Extensions;

internal static class UriExtensions
{
    extension(Uri)
    {
        public static Uri? CreateOrNull(string url)
        {
            try
            {
                return new Uri(url);
            }
            catch
            {
                return null;
            }
        }
    }
}
