using System.Runtime.CompilerServices;
using Netorrent.Bencoding.Structs;

namespace Netorrent.Extensions;

internal static class IBencodingNodeExtensions
{
    extension(IBencodingNode? value)
    {
        public T? As<T>([CallerArgumentExpression(nameof(value))] string? expression = null)
            where T : struct, IBencodingNode
        {
            if (value is null)
            {
                return null;
            }

            if (value is not T bString)
            {
                throw new InvalidCastException(
                    $"{expression ?? "the value"} is not a valid bencoded value."
                );
            }

            return bString;
        }
    }
}
