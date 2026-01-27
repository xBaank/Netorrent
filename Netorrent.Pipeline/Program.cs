// Program.cs
using ModularPipelines;
using ModularPipelines.Extensions;
using Netorrent.Pipeline;

await Pipeline
    .CreateBuilder()
    .AddModule<BuildModule>()
    .AddModule<UnitTestModule>()
    .AddModule<IntegrationTestModule>()
    .ExecutePipelineAsync();
