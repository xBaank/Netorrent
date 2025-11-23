using DotNet.Testcontainers.Builders;
using TUnit.Core.Interfaces;
using IContainer = DotNet.Testcontainers.Containers.IContainer;

namespace Netorrent.Tests.Fixtures;

public class OpenTrackerFixture : IAsyncInitializer, IAsyncDisposable
{
    private IContainer? _container;
    public string AnnounceUrl { get; private set; } = "";
    public string UdpAnnounceUrl { get; private set; } = "";

    public string[] AnnounceUrls => [AnnounceUrl, UdpAnnounceUrl];

    public async Task InitializeAsync()
    {
        _container = new ContainerBuilder()
            .WithImage("xbank/opentracker-docker")
            .WithName("opentracker-test")
            .WithPortBinding("6969/tcp", true)
            .WithPortBinding("6969/udp", true)
            .Build();

        await _container.StartAsync();

        var tcpPort = _container.GetMappedPublicPort("6969");
        var udpPort = _container.GetMappedPublicPort("6969/udp");

        AnnounceUrl = $"http://localhost:{tcpPort}/announce";
        UdpAnnounceUrl = $"udp://localhost:{udpPort}/announce";
    }

    public async ValueTask DisposeAsync()
    {
        if (_container is null)
            return;

        await _container.StopAsync();
        await _container.DisposeAsync();
    }
}
