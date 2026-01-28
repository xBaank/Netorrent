using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using ModularPipelines;
using ModularPipelines.Extensions;
using Netorrent.Pipeline;
using Netorrent.Pipeline.PublishModules;
using Netorrent.Pipeline.Settings;
using Netorrent.Pipeline.TestsModules;

var builder = Pipeline.CreateBuilder(args);

builder.Configuration.AddUserSecrets<Program>().AddEnvironmentVariables();

builder.Services.Configure<NuGetSettings>(builder.Configuration.GetSection("Nuget"));

await builder
    .AddModule<BuildModule>()
    .AddModule<UnitTestModule>()
    .AddModule<IntegrationTestModule>()
    .AddModule<SetupVersion>()
    .AddModule<PackModule>()
    .AddModule<NugetPublishModule>()
    .AddModule<ReleaseModule>()
    .ExecutePipelineAsync();
