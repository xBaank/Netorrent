using System.Runtime.CompilerServices;
using Netorrent.Bencoding.Structs;

namespace Netorrent.Extensions;

public static class IBencodingNodeExtensions
{
    public static T? As<T>(
        this IBencodingNode? value,
        [CallerArgumentExpression(nameof(value))] string? expression = null
    )
        where T : struct, IBencodingNode
    {
        if (value is null)
            return null;

        if (value is not T bString)
            throw new InvalidCastException(
                $"{expression ?? "the value"} is not a valid bencoded value."
            );

        return bString;
    }
}
