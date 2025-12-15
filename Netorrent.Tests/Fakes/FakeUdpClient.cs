using System.Net;
using System.Net.Sockets;
using System.Threading.Channels;
using Netorrent.Tracker.Udp.Client;

internal sealed class FakeUdpClient : IUdpClient
{
    private readonly Channel<UdpReceiveResult> _incoming =
        Channel.CreateUnbounded<UdpReceiveResult>();

    public List<(byte[] Payload, IPEndPoint Endpoint)> SentPackets { get; } = [];

    public ValueTask<int> SendAsync(
        ReadOnlyMemory<byte> buffer,
        IPEndPoint endPoint,
        CancellationToken cancellationToken
    )
    {
        SentPackets.Add((buffer.ToArray(), endPoint));
        return ValueTask.FromResult(buffer.Length);
    }

    public async ValueTask<UdpReceiveResult> ReceiveAsync(CancellationToken cancellationToken)
    {
        return await _incoming.Reader.ReadAsync(cancellationToken);
    }

    // Test helper
    public void EnqueueIncoming(byte[] payload, IPEndPoint remote)
    {
        _incoming.Writer.TryWrite(new UdpReceiveResult(payload, remote));
    }
}
