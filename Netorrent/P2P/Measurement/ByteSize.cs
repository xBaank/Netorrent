namespace Netorrent.P2P.Measurement;

public readonly struct ByteSize(long bytes)
{
    public long Bytes { get; } = bytes;
    public double Kilobytes => Bytes / 1024.0;
    public double Megabytes => Bytes / (1024.0 * 1024);
    public double Gigabytes => Bytes / (1024.0 * 1024 * 1024);
    public double Terabytes => Bytes / (1024.0 * 1024 * 1024 * 1024);

    public static implicit operator ByteSize(long bytes) => new(bytes);

    public static ByteSize operator +(ByteSize a, ByteSize b) => new(a.Bytes + b.Bytes);

    public static ByteSize operator -(ByteSize a, ByteSize b) => new(a.Bytes - b.Bytes);

    public static bool operator >(ByteSize a, ByteSize b) => a.Bytes > b.Bytes;

    public static bool operator <(ByteSize a, ByteSize b) => a.Bytes < b.Bytes;

    public static bool operator >=(ByteSize a, ByteSize b) => a.Bytes >= b.Bytes;

    public static bool operator <=(ByteSize a, ByteSize b) => a.Bytes <= b.Bytes;

    public static bool operator ==(ByteSize a, ByteSize b) => a.Bytes == b.Bytes;

    public static bool operator !=(ByteSize a, ByteSize b) => a.Bytes != b.Bytes;

    public override string ToString()
    {
        double value = Bytes;
        string unit = "B";

        if (Math.Abs(value) >= 1024)
        {
            value /= 1024;
            unit = "KB";
        }
        if (Math.Abs(value) >= 1024)
        {
            value /= 1024;
            unit = "MB";
        }
        if (Math.Abs(value) >= 1024)
        {
            value /= 1024;
            unit = "GB";
        }
        if (Math.Abs(value) >= 1024)
        {
            value /= 1024;
            unit = "TB";
        }

        return $"{value:0.##} {unit}";
    }

    public override bool Equals(object? obj) => obj is ByteSize size && Bytes == size.Bytes;

    public override int GetHashCode() => HashCode.Combine(Bytes);
}
