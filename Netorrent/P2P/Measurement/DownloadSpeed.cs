namespace Netorrent.P2P.Measurement;

/// <summary>
/// Represents a download speed measurement in bits per second (bps) and provides conversions to common units.
/// </summary>
/// <remarks>Use this struct to represent and manipulate download speeds in various units, such as bps, Kbps,
/// Mbps, and Gbps. The struct supports arithmetic operations and implicit conversion from a double value representing
/// bits per second.</remarks>
/// <param name="bps">The download speed in bits per second (bps).</param>
public readonly struct DownloadSpeed(double bps)
{
    public double Bps => bps;
    public double Kbps => bps / 1_000d;
    public double Mbps => bps / 1_000_000d;
    public double Gbps => bps / 1_000_000_000d;

    public static implicit operator DownloadSpeed(double bps) => new(bps);

    public static DownloadSpeed operator +(DownloadSpeed left, DownloadSpeed right) =>
        new(left.Bps + right.Bps);

    public static DownloadSpeed operator -(DownloadSpeed left, DownloadSpeed right) =>
        new(left.Bps - right.Bps);

    public override string ToString()
    {
        if (bps >= 1_000_000_000)
            return $"{Gbps:F2}/Gbps";
        if (bps >= 1_000_000)
            return $"{Mbps:F2}/Mbps";
        if (bps >= 1_000)
            return $"{Kbps:F2}/Kbps";
        return $"{bps:F2}/bps";
    }
}
