using System.Buffers;
using System.IO;
using Netorrent.Other;
using ZLinq;

namespace Netorrent.Extensions;

internal static class MemoryExtensions
{
    extension(IList<Memory<byte>> chunks)
    {
        public RentedArray<byte> Combine()
        {
            var totalLength = chunks.AsValueEnumerable().Sum(i => i.Length);
            var array = ArrayPool<byte>.Shared.Rent(totalLength);
            var destination = array.AsMemory()[..totalLength];
            int offset = 0;
            foreach (var chunk in chunks)
            {
                chunk.CopyTo(destination[offset..]);
                offset += chunk.Length;
            }
            return new RentedArray<byte>(array, totalLength);
        }
    }
}
