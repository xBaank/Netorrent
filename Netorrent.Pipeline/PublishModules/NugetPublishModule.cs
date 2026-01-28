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
                if (ctx.IsRunningInCI() && OperatingSystem.IsLinux())
                {
                    return SkipDecision.DoNotSkip;
                }

                if (ctx.IsRunningLocally())
                {
                    return SkipDecision.DoNotSkip;
                }

                return SkipDecision.Skip("Running on CI other than Linux");
            })
            .Build();

    protected override async Task<CommandResult?> ExecuteAsync(
        IModuleContext context,
        CancellationToken cancellationToken
    )
    {
        var localSource = Path.GetFullPath(Path.Combine("artifacts", "local-nuget"));

        if (!context.IsRunningInCI())
        {
            Directory.CreateDirectory(localSource);
        }

        return await context
            .DotNet()
            .Nuget.Push(
                new DotNetNugetPushOptions
                {
                    Path = Path.Combine(context.Git().RootDirectory.Path, "artifacts", "*.nupkg"),
                    Source = context.IsRunningInCI() ? nugetSettings.Value.FeedUrl : localSource,
                    ApiKey = nugetSettings.Value.ApiKey,
                    SkipDuplicate = true,
                },
                cancellationToken: cancellationToken
            );
    }
}
