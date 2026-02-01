using System.Text;

namespace Netorrent.Tests.Bencoding.Data;

public static class InvalidBencodingData
{
    public static IEnumerable<Func<(string, string)>> GetMalformedStringData()
    {
        yield return () => ("4spam", "Missing colon separator");
        yield return () => ("a:spam", "Non-numeric length prefix");
        yield return () => ("1.5:spam", "Decimal length prefix");
        yield return () => ("+4:spam", "Positive sign in length");
        yield return () => ("-1:spam", "Negative string length");
        yield return () => ("-5:hello", "Negative string length with content");
        yield return () => ("10:short", "Length exceeds actual data");
        yield return () => ("5:x", "Length greatly exceeds data");
        yield return () => ("3:", "Length specified but no data");
        yield return () => ("1", "Missing colon and data");
        yield return () => (":data", "Empty length prefix");
        yield return () => (" :", "Space as length");
    }

    public static IEnumerable<Func<(string, string)>> GetCorruptedIntegerData()
    {
        yield return () => ("i123", "Missing integer terminator");
        yield return () => ("i", "Missing value and terminator");
        yield return () => ("i-", "Negative sign without digits");

        yield return () => ("iabc e", "Non-numeric integer content");
        yield return () => ("i12.34e", "Decimal in integer");
        yield return () => ("i1a2e", "Mixed alphanumeric");
        yield return () => ("i 123 e", "Spaces in integer");

        yield return () => ("ie", "Empty integer");
        yield return () => ("i-e", "Empty negative integer");

        yield return () => ("i+123e", "Positive sign in integer");
        yield return () => ("i0123e", "Leading zeros");
        yield return () => ("i-0123e", "Leading zeros with negative");

        yield return () => ("ii123e", "Multiple integer starters");
        yield return () => ("i123ee", "Multiple integer terminators");
    }

    public static IEnumerable<Func<(string, string)>> GetStructureValidationData()
    {
        yield return () => ("l4:test", "Unclosed list");
        yield return () => ("d3:foo3:bar", "Unclosed dictionary");
        yield return () => ("ld4:test4:item", "Nested unclosed containers");

        yield return () => ("l4:teste3:foo", "Extra data after closed container");
        yield return () => ("d3:foo3:baree", "Extra terminator");

        yield return () => ("lx", "Invalid character in list");
        yield return () => ("dx", "Invalid character in dictionary");
        yield return () => ("4:test3:foo", "Multiple top-level elements");

        yield return () => ("l", "List starter only");
        yield return () => ("d", "Dictionary starter only");
        yield return () => ("e", "Terminator without starter");

        yield return () => ("d3:foo", "Dictionary key without value");
        yield return () => ("di42e4:spame", "Dictionary key not a string");
        yield return () => ("di42ei42ee", "Dictionary keys are integers");
    }

    public static IEnumerable<Func<(string, string)>> GetBufferSafetyData()
    {
        yield return () => ("", "Empty input");

        yield return () => ("4", "Truncated at length start");
        yield return () => ("4:", "Truncated after colon");
        yield return () => ("i", "Truncated at integer start");
        yield return () => ("i12", "Truncated integer");

        yield return () => ("l", "Single list starter");
        yield return () => ("d", "Single dictionary starter");
        yield return () => ("e", "Single terminator");
        yield return () => (":", "Single colon");

        yield return () => ("9999999999:", "Huge length with no data");
        yield return () => ("i999999999999999999999", "Very long integer without terminator");
    }

    public static IEnumerable<Func<(string, string)>> GetInvalidTypeData()
    {
        yield return () => ("x:data", "Unknown type character");
        yield return () => ("123:test", "Starts with digits but not a valid string");
        yield return () => ("@data", "At symbol as type");
        yield return () => ("#data", "Hash as type");
        yield return () => ("$data", "Dollar sign as type");

        yield return () => ("\n4:test", "Leading newline");
        yield return () => ("\r4:test", "Leading carriage return");
        yield return () => ("\t4:test", "Leading tab");
        yield return () => (" 4:test", "Leading space");
        yield return () => ("4:test\n", "Trailing newline");

        yield return () => ("\0:data", "Leading null byte");
        yield return () => ("4:te\0st", "Null in string data");
        yield return () => ("i\0e", "Null in integer");
    }
}
