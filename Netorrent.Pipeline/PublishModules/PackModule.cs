using ModularPipelines.Attributes;
using ModularPipelines.Context;
using ModularPipelines.DotNet.Extensions;
using ModularPipelines.DotNet.Options;
using ModularPipelines.Git.Extensions;
using ModularPipelines.Models;
using ModularPipelines.Modules;

namespace Netorrent.Pipeline.PublishModules;

[DependsOn<SetupVersion>]
public class PackModule : Module<CommandResult>
{
    protected override async Task<CommandResult?> ExecuteAsync(
        IModuleContext context,
        CancellationToken cancellationToken
    )
    {
        var result = await context.GetModule<SetupVersion>();

        return await context
            .DotNet()
            .Pack(
                new DotNetPackOptions
                {
                    ProjectSolution = Path.Combine(context.Git().RootDirectory.Path, "Netorrent"),
                    Configuration = "Release",
                    Output = Path.Combine(context.Git().RootDirectory.Path, "artifacts"),
                    NoBuild = true,
                    Arguments =
                    [
                        $"-p:PackageVersion={result.ValueOrDefault!}",
                        "-p:ContinuousIntegrationBuild=true",
                        "-p:IncludeSymbols=true",
                    ],
                },
                cancellationToken: cancellationToken
            );
    }
}
