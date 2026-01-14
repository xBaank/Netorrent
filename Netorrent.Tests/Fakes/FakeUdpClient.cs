using System.Net;
using System.Net.Sockets;
using System.Threading.Channels;
using Netorrent.Tracker.Udp.Client;
using R3;

internal sealed class FakeUdpClient : IUdpClient
{
    private readonly Channel<UdpReceiveResult> _incoming =
        Channel.CreateUnbounded<UdpReceiveResult>();

    public List<(Memory<byte> Payload, IPEndPoint Endpoint)> SentPackets { get; } = [];

    public Subject<Memory<byte>> OnSent { get; } = new();
    public Subject<Memory<byte>> OnReceived { get; } = new();

    public ValueTask<int> SendAsync(
        ReadOnlyMemory<byte> buffer,
        IPEndPoint endPoint,
        CancellationToken cancellationToken
    )
    {
        var copied = buffer.ToArray();
        SentPackets.Add((copied, endPoint));
        OnSent.OnNext(copied);
        return ValueTask.FromResult(buffer.Length);
    }

    public async ValueTask<UdpReceiveResult> ReceiveAsync(CancellationToken cancellationToken)
    {
        var received = await _incoming.Reader.ReadAsync(cancellationToken);
        OnReceived.OnNext(received.Buffer.ToArray());
        return received;
    }

    // Test helper
    public void EnqueueIncoming(byte[] payload, IPEndPoint remote)
    {
        _incoming.Writer.TryWrite(new UdpReceiveResult(payload, remote));
    }
}
