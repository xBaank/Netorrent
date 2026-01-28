using Microsoft.Extensions.Options;
using ModularPipelines.Attributes;
using ModularPipelines.Configuration;
using ModularPipelines.Context;
using ModularPipelines.DotNet.Extensions;
using ModularPipelines.DotNet.Options;
using ModularPipelines.Git.Extensions;
using ModularPipelines.Models;
using ModularPipelines.Modules;
using Netorrent.Pipeline.Settings;

namespace Netorrent.Pipeline.PublishModules;

[DependsOn<PackModule>]
public class NugetPublishModule(IOptions<NuGetSettings> nugetSettings) : Module<CommandResult>
{
    protected override ModuleConfiguration Configure() =>
        ModuleConfiguration
            .Create()
            .WithSkipWhen(ctx =>
            {
                if (!ctx.IsRunningInCI() && !OperatingSystem.IsLinux())
                {
                    return SkipDecision.Skip("Not running on Linux CI");
                }

                if (string.IsNullOrWhiteSpace(nugetSettings.Value.ApiKey))
                {
                    return SkipDecision.Skip("Not ApiKey specified");
                }

                return SkipDecision.DoNotSkip;
            })
            .Build();

    protected override async Task<CommandResult?> ExecuteAsync(
        IModuleContext context,
        CancellationToken cancellationToken
    )
    {
        return await context
            .DotNet()
            .Nuget.Push(
                new DotNetNugetPushOptions
                {
                    Path = Path.Combine(context.Git().RootDirectory.Path, "artifacts", "*.nupkg"),
                    Source = nugetSettings.Value.FeedUrl,
                    ApiKey = nugetSettings.Value.ApiKey,
                    SkipDuplicate = true,
                },
                cancellationToken: cancellationToken
            );
    }
}
