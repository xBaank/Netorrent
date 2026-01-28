using ModularPipelines.Attributes;
using ModularPipelines.Configuration;
using ModularPipelines.Context;
using ModularPipelines.Git.Attributes;
using ModularPipelines.GitHub.Extensions;
using ModularPipelines.Models;
using ModularPipelines.Modules;
using Octokit;

namespace Netorrent.Pipeline.PublishModules;

[DependsOn<PackModule>]
[RunOnlyOnBranch("main")]
[RunOnLinuxOnly]
public class ReleaseModule : Module<Release>
{
    protected override ModuleConfiguration Configure() =>
        ModuleConfiguration
            .Create()
            .WithSkipWhen(
                async (ctx) =>
                {
                    var gitRef = ctx.GitHub().EnvironmentVariables.Ref;

                    if (gitRef is null || !gitRef.StartsWith("refs/tags/"))
                    {
                        return SkipDecision.Skip("No tag provided");
                    }

                    return SkipDecision.DoNotSkip;
                }
            )
            .Build();

    protected override async Task<Release?> ExecuteAsync(
        IModuleContext context,
        CancellationToken cancellationToken
    )
    {
        var version = (await context.GetModule<SetupVersion>()).ValueOrDefault!;

        return await context
            .GitHub()
            .Client.Repository.Release.Create(
                "xBaank",
                "Netorrent",
                new NewRelease(version) { GenerateReleaseNotes = true, Name = version }
            );
    }
}
