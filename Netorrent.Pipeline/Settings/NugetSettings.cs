using ModularPipelines.Attributes;

namespace Netorrent.Pipeline.Settings;

public record NuGetSettings
{
    [SecretValue]
    public string? ApiKey { get; init; }

    public string FeedUrl { get; init; } = "https://api.nuget.org/v3/index.json";
}
