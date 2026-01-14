using Netorrent.TorrentFile;
using PublicApiGenerator;
using Shouldly;

namespace Netorrent.Tests.PublicApi;

public class ApiTest
{
    [Test]
    public void My_API_Has_No_Changes()
    {
        var publicApi = typeof(TorrentClient).Assembly.GeneratePublicApi();

        //Shouldly
        publicApi.ShouldMatchApproved();
    }
}
