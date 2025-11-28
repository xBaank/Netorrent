using System.Reactive.Linq;
using System.Reactive.Threading.Tasks;
using Netorrent.P2P;
using Netorrent.P2P.Messages;
using Netorrent.Tests.Extensions;
using Netorrent.Tests.P2P;

namespace Netorrent.Tests.Extensions;

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

        public Task<PeerConnection> NextStateAsync(int count, CancellationToken ct) =>
            peer.StateChanged.Take(count).ToTask(ct);
    }
}
