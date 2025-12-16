namespace Netorrent.Extensions;

internal static class TimeSpanExtensions
{
    extension(int time)
    {
        public TimeSpan Milliseconds => TimeSpan.FromMilliseconds(time);
        public TimeSpan Seconds => TimeSpan.FromSeconds(time);
        public TimeSpan Minutes => TimeSpan.FromMinutes(time);
        public TimeSpan Hours => TimeSpan.FromHours(time);
    }

    extension(double time)
    {
        public TimeSpan Milliseconds => TimeSpan.FromMilliseconds(time);
        public TimeSpan Seconds => TimeSpan.FromSeconds(time);
        public TimeSpan Minutes => TimeSpan.FromMinutes(time);
        public TimeSpan Hours => TimeSpan.FromHours(time);
    }
}
