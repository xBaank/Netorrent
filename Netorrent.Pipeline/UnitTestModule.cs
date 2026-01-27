using ModularPipelines.Attributes;
using ModularPipelines.Context;
using ModularPipelines.DotNet.Extensions;
using ModularPipelines.DotNet.Options;
using ModularPipelines.Models;
using ModularPipelines.Modules;

namespace Netorrent.Pipeline;

[DependsOn<BuildModule>]
public class UnitTestModule : Module<CommandResult>
{
    protected override async Task<CommandResult?> ExecuteAsync(
        IModuleContext context,
        CancellationToken cancellationToken
    ) =>
        await context
            .DotNet()
            .Test(
                new DotNetTestOptions { Project = "./Netorrent.Tests", Configuration = "Release" },
                cancellationToken: cancellationToken
            );
}
