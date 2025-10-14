using System.Runtime.CompilerServices;
using Netorrent.Bencoding.Structs;

namespace Netorrent.Extensions;

public static class IBencodingTypeExtensions
{
    public static T? As<T>(
        this IBencodingType? value,
        [CallerArgumentExpression(nameof(value))] string? expression = null
    )
        where T : struct, IBencodingType
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
