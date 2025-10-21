using DotNet.Testcontainers.Builders;
using IContainer = DotNet.Testcontainers.Containers.IContainer;

namespace Netorrent.Tests.Fixtures;

public class OpenTrackerFixture : IAsyncLifetime
{
    private IContainer? _container;
    public string AnnounceUrl { get; private set; } = "";

    public async Task InitializeAsync()
    {
        _container = new ContainerBuilder()
            .WithImage("lednerb/opentracker-docker")
            .WithName("opentracker-test")
            .WithPortBinding(6969, 6969)
            .Build();

        await _container.StartAsync();

        AnnounceUrl = $"http://localhost:{_container.GetMappedPublicPort(6969)}/announce";
    }

    public async Task DisposeAsync()
    {
        if (_container is null)
            return;

        await _container.StopAsync();
        await _container.DisposeAsync();
    }
}
