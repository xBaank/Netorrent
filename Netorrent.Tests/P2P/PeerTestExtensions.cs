using System.Reactive.Linq;
using System.Reactive.Threading.Tasks;
using Netorrent.P2P;
using Netorrent.P2P.Messages;

namespace Netorrent.Tests.P2P;

internal static class PeerTestExtensions
{
    extension(PeerConnectionTestContext ctx)
    {
        public Task WriteAsync(Message msg, CancellationToken ct) =>
            ctx.Incoming.Writer.WriteAsync(msg, ct).AsTask();

        public Task<Message> ReadAsync(CancellationToken ct) =>
            ctx.Outgoing.Reader.ReadAsync(ct).AsTask();
    }

    extension(PeerConnection peer)
    {
        public Task<PeerConnection> NextStateAsync(CancellationToken ct) =>
            peer.StateChanged.FirstAsync().ToTask(ct);
    }
}
