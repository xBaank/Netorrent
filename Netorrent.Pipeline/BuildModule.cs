using ModularPipelines.Context;
using ModularPipelines.DotNet.Extensions;
using ModularPipelines.DotNet.Options;
using ModularPipelines.Git.Extensions;
using ModularPipelines.Models;
using ModularPipelines.Modules;

namespace Netorrent.Pipeline;

public class BuildModule : Module<CommandResult>
{
    protected override async Task<CommandResult?> ExecuteAsync(
        IModuleContext context,
        CancellationToken cancellationToken
    ) =>
        await context
            .DotNet()
            .Build(
                new DotNetBuildOptions
                {
                    ProjectSolution = Path.Combine(
                        context.Git().RootDirectory.Path,
                        "Netorrent.sln"
                    ),
                    Configuration = "Release",
                },
                cancellationToken: cancellationToken
            );
}
