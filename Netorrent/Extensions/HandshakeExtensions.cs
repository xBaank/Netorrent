using System.Buffers;
using Netorrent.P2P;
using Netorrent.P2P.Messages;

namespace Netorrent.Extensions;

internal static class HandshakeExtensions
{
    extension(Handshake)
    {
        public static async ValueTask<Handshake> PerformHandshakeAsync(
            Stream stream,
            ReadOnlyMemory<byte> infoHash,
            PeerId peerId,
            CancellationToken cancellationToken
        )
        {
            using var timeoutCts = cancellationToken.WithTimeout(10.Seconds);
            await SendHandshakeInternalAsync(stream, infoHash, peerId, timeoutCts.Token)
                .ConfigureAwait(false);
            var receivedHandshake = await ReceiveHandshakeInternalAsync(stream, timeoutCts.Token)
                .ConfigureAwait(false);

            return receivedHandshake.InfoHash.SequenceEqual(infoHash.Span)
                ? receivedHandshake
                : throw new InvalidOperationException("InfoHash do not match");
        }

        public static async ValueTask<Handshake> ReceiveHandshakeAsync(
            Stream stream,
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

            var receivedHandshake = await ReceiveHandshakeInternalAsync(stream, timeoutCts.Token)
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

            await SendHandshakeInternalAsync(
                    stream,
                    selectedInfoHash.Value,
                    peerId,
                    timeoutCts.Token
                )
                .ConfigureAwait(false);

            return receivedHandshake;
        }

        private static async ValueTask<Handshake> ReceiveHandshakeInternalAsync(
            Stream stream,
            CancellationToken cancellationToken
        )
        {
            using var pool = MemoryPool<byte>.Shared.Rent(Handshake.TotalLength);
            var buffer = pool.Memory[..Handshake.TotalLength];
            await stream.ReadExactlyAsync(buffer, cancellationToken).ConfigureAwait(false);
            var receivedHandshake = Handshake.FromBytes(buffer.Span);
            return receivedHandshake;
        }

        private static async ValueTask SendHandshakeInternalAsync(
            Stream stream,
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
