using System.Buffers;
using System.IO;
using Netorrent.Other;
using ZLinq;

namespace Netorrent.Extensions;

internal static class MemoryExtensions
{
    extension(IList<Memory<byte>> chunks)
    {
        public MemoryRented<byte> Combine()
        {
            var totalLength = chunks.AsValueEnumerable().Sum(i => i.Length);
            var memoryPool = MemoryPool<byte>.Shared.Rent(totalLength);
            var destination = memoryPool.Memory[..totalLength];
            int offset = 0;
            foreach (var chunk in chunks)
            {
                chunk.CopyTo(destination[offset..]);
                offset += chunk.Length;
            }
            return new MemoryRented<byte>(memoryPool, totalLength);
        }
    }
}
