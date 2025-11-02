using Netorrent.P2P;
using Netorrent.Tests.Fixtures;
using Netorrent.Tracker;
using Shouldly;

namespace Netorrent.Tests.Tracker;

/*
public class HttpAnounceTests(OpenTrackerFixture fixture) : IClassFixture<OpenTrackerFixture>
{
    private readonly OpenTrackerFixture _fixture = fixture;

    [Fact]
    public async Task Can_Announce_With_No_Data_To_Tracker()
    {
        // Arrange
        var httpClient = new HttpClient();
        var peerIdService = new PeerIdService();
        var metaInfo = TestMetaInfoFactory.CreateSingleFileMetaInfo(
            _fixture.AnnounceUrl,
            "test.a",
            "ONE;TWO;THREEEE"
        );
        var p2pClient = new P2PClient(metaInfo); // test port
        await using var trackerClient = new TrackerClient(
            p2pClient,
            httpClient,
            peerIdService,
            metaInfo.Info.InfoHash,
            _fixture.AnnounceUrl,
            new FileHandlerFake(metaInfo.Info.GetAllFilesSize(), 0, 0)
        );

        // Act
        var response = await trackerClient.Announce(
            Events.Started,
            new CancellationTokenSource(TimeSpan.FromSeconds(5)).Token
        );

        response.Incomplete.ShouldBe(1);
        response.Complete.ShouldBe(0);
        response.Peers.ShouldNotBeEmpty();
    }

    [Fact]
    public async Task Can_Announce_With_Data_To_Tracker()
    {
        // Arrange
        var httpClient = new HttpClient();
        var peerIdService = new PeerIdService();
        var metaInfo = TestMetaInfoFactory.CreateSingleFileMetaInfo(
            _fixture.AnnounceUrl,
            "test.a",
            "ONE;TWO;THREEEE"
        );
        var p2pClient = new P2PClient(metaInfo); // test port
        await using var trackerClient = new TrackerClient(
            p2pClient,
            httpClient,
            peerIdService,
            metaInfo.Info.InfoHash,
            _fixture.AnnounceUrl,
            new FileHandlerFake(0, metaInfo.Info.GetAllFilesSize(), 0)
        );

        // Act
        var response = await trackerClient.Announce(
            Events.Started,
            new CancellationTokenSource(TimeSpan.FromSeconds(5)).Token
        );

        response.Incomplete.ShouldBe(0);
        response.Complete.ShouldBe(1);
        response.Peers.ShouldNotBeEmpty();
    }
}
*/
