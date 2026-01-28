using ModularPipelines.Attributes;
using ModularPipelines.Context;
using ModularPipelines.Git.Extensions;
using ModularPipelines.Git.Options;
using ModularPipelines.GitHub.Extensions;
using ModularPipelines.Modules;

namespace Netorrent.Pipeline.PublishModules;

[DependsOn<BuildModule>]
public partial class SetupVersion : Module<string>
{
    protected override async Task<string?> ExecuteAsync(
        IModuleContext context,
        CancellationToken cancellationToken
    )
    {
        var gitRef = context.GitHub().EnvironmentVariables.Ref;

        var gitTagsOutput = (
            await context
                .Git()
                .Commands.Tag(
                    new GitTagOptions { Sort = "-version:refname" },
                    token: cancellationToken
                )
        ).StandardOutput;

        var gitTags = gitTagsOutput
            .Split(["\r\n", "\n"], StringSplitOptions.RemoveEmptyEntries)
            .ToArray();

        var version = gitTags.FirstOrDefault() ?? "0.0.1";

        if (gitRef is null || gitRef == "refs/heads/develop")
        {
            var commitSha = context.Git().Information.LastCommitSha[..7];
            var time = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            version = $"{version}-nightly-{time}-{commitSha}";
        }
        else if (gitRef.StartsWith("refs/tags/"))
        {
            version = gitRef.Replace("refs/tags/", "");
        }

        return version;
    }
}
