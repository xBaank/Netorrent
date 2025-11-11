using DotNet.Testcontainers.Builders;
using IContainer = DotNet.Testcontainers.Containers.IContainer;

namespace Netorrent.Tests.Fixtures;

public class OpenTrackerFixture : IAsyncLifetime
{
    private IContainer? _container;
    public string AnnounceUrl { get; private set; } = "";
    public string UdpAnnounceUrl { get; private set; } = "";

    public async ValueTask InitializeAsync()
    {
        _container = new ContainerBuilder()
            .WithImage("xbaank/opentracker")
            .WithName("opentracker-test")
            .WithCreateParameterModifier(p => p.HostConfig.NetworkMode = "host")
            .Build();

        await _container.StartAsync();

        AnnounceUrl = $"http://127.0.0.1:6969/announce";
        UdpAnnounceUrl = $"udp://127.0.0.1:6969/announce";
    }

    public async ValueTask DisposeAsync()
    {
        if (_container is null)
            return;

        await _container.StopAsync();
        await _container.DisposeAsync();
    }
}
