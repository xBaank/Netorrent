using System.Net;
using System.Net.Sockets;

namespace Netorrent.P2P;

internal class P2PClient(TcpListener listener)
{
    public int Port => ((IPEndPoint)listener.LocalEndpoint).Port;
    private readonly List<TcpClient> _peers = [];

    public void Start()
    {
        listener.Start();
        _ = AcceptLoopAsync();
    }

    private async Task AcceptLoopAsync()
    {
        while (true)
        {
            var client = await listener.AcceptTcpClientAsync();
            lock (_peers)
                _peers.Add(client);
            _ = HandlePeerAsync(client);
        }
    }

    private async Task HandlePeerAsync(TcpClient client)
    {
        try
        {
            // Perform BitTorrent handshake, then message loop
        }
        finally
        {
            lock (_peers)
                _peers.Remove(client);
            client.Close();
        }
    }
}
