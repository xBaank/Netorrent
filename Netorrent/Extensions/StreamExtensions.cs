using System.Buffers;
using Netorrent.P2P;
using Netorrent.P2P.Messages;

namespace Netorrent.Extensions;

internal static class StreamExtensions
{
    extension(Stream stream)
    {
        public async ValueTask<Handshake> PerformHandshakeAsync(
            ReadOnlyMemory<byte> infoHash,
            PeerId peerId,
            CancellationToken cancellationToken
        )
        {
            using var timeoutCts = cancellationToken.WithTimeout(10.Seconds);
            await stream
                .SendHandshakeInternalAsync(infoHash, peerId, timeoutCts.Token)
                .ConfigureAwait(false);
            var receivedHandshake = await stream
                .ReceiveHandshakeInternalAsync(timeoutCts.Token)
                .ConfigureAwait(false);

            return receivedHandshake.InfoHash.SequenceEqual(infoHash.Span)
                ? receivedHandshake
                : throw new InvalidOperationException("InfoHash do not match");
        }

        public async ValueTask<Handshake> ReceiveHandshakeAsync(
            ICollection<ReadOnlyMemory<byte>> infoHashes,
            PeerId peerId,
            CancellationToken cancellationToken
        )
        {
            if (infoHashes.Count == 0)
            {
                throw new ArgumentException(
                    "InfoHashes collection cannot be empty.",
                    nameof(infoHashes)
                );
            }

            using var timeoutCts = cancellationToken.WithTimeout(10.Seconds);

            var receivedHandshake = await stream
                .ReceiveHandshakeInternalAsync(timeoutCts.Token)
                .ConfigureAwait(false);

            ReadOnlyMemory<byte>? selectedInfoHash = null;
            foreach (var infoHash in infoHashes)
            {
                if (receivedHandshake.InfoHash.SequenceEqual(infoHash.Span))
                {
                    selectedInfoHash = infoHash;
                    break;
                }
            }

            if (selectedInfoHash is null)
            {
                throw new InvalidOperationException(
                    "Received handshake contains an unknown info hash."
                );
            }

            await stream
                .SendHandshakeInternalAsync(selectedInfoHash.Value, peerId, timeoutCts.Token)
                .ConfigureAwait(false);

            return receivedHandshake;
        }

        private async ValueTask<Handshake> ReceiveHandshakeInternalAsync(
            CancellationToken cancellationToken
        )
        {
            using var pool = MemoryPool<byte>.Shared.Rent(Handshake.TotalLength);
            var buffer = pool.Memory[..Handshake.TotalLength];
            await stream.ReadExactlyAsync(buffer, cancellationToken).ConfigureAwait(false);
            var receivedHandshake = Handshake.FromBytes(buffer.Span);
            return receivedHandshake;
        }

        private async ValueTask SendHandshakeInternalAsync(
            ReadOnlyMemory<byte> infoHash,
            PeerId peerId,
            CancellationToken cancellationToken
        )
        {
            var handshake = Handshake.Create(infoHash.ToArray(), peerId.ToBytes());
            using var bytesRented = handshake.ToBytes();
            await stream.WriteAsync(bytesRented.Memory, cancellationToken).ConfigureAwait(false);
            await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}
