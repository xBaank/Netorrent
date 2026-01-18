using System.Net;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Netorrent.Extensions;
using Netorrent.P2P.Messages;
using Netorrent.TorrentFile.FileStructure;

namespace Netorrent.P2P.Tcp;

internal class TcpPeersConnector(
    PeersClient peersClient,
    InfoHash infoHash,
    PeerId peerId,
    ChannelReader<IPEndPoint> peersEndpoints,
    Func<IPAddress, IPAddress>? peerIpProxy,
    ILogger logger
)
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        List<Task> tasks = [];
        try
        {
            await foreach (
                var iPEndPoint in peersEndpoints
                    .ReadAllAsync(cancellationToken)
                    .ConfigureAwait(false)
            )
            {
                var targetEndPoint = new IPEndPoint(
                    peerIpProxy?.Invoke(iPEndPoint.Address) ?? iPEndPoint.Address,
                    iPEndPoint.Port
                );

                try
                {
                    tasks.Add(
                        peersClient
                            .AddPeerAsync(
                                new TcpPeer(null, targetEndPoint, peerId, infoHash),
                                cancellationToken
                            )
                            .AsTask()
                    );

                    if (tasks.Count >= 500)
                    {
                        try
                        {
                            await Task.WhenAll(tasks).ConfigureAwait(false);
                        }
                        finally
                        {
                            tasks.Clear();
                        }
                    }
                }
                catch (Exception ex)
                {
                    if (logger.IsEnabled(LogLevel.Debug))
                    {
                        logger.LogDebug(ex, "Error connecting to {ip}", targetEndPoint);
                    }
                }
            }
        }
        finally
        {
            try
            {
                await Task.WhenAll(tasks).ConfigureAwait(false);
            }
            catch { }
        }
    }
}
