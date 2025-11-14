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
            .WithPortBinding("6969/tcp", true)
            .WithPortBinding("6969/udp", true)
            .Build();

        await _container.StartAsync();

        var tcpPort = _container.GetMappedPublicPort("6969");
        var udpPort = _container.GetMappedPublicPort("6969/udp");

        AnnounceUrl = $"http://127.0.0.1:{tcpPort}/announce";
        UdpAnnounceUrl = $"udp://127.0.0.1:{udpPort}/announce";
    }

    public async ValueTask DisposeAsync()
    {
        if (_container is null)
            return;

        await _container.StopAsync();
        await _container.DisposeAsync();
    }
}
