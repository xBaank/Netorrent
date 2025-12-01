using System.Text;
using Netorrent.Bencoding;
using Netorrent.Bencoding.Structs;
using Netorrent.Tests.Bencoding.Data;
using Shouldly;

namespace Netorrent.Tests.Bencoding;

public class BEncoderTests
{
    [Test]
    [Arguments(0, "i0e")]
    [Arguments(1, "i1e")]
    [Arguments(42, "i42e")]
    [Arguments(-1, "i-1e")]
    [Arguments(999, "i999e")]
    [Arguments(123456789, "i123456789e")]
    [Arguments(-99999, "i-99999e")]
    [Arguments(2147483647, "i2147483647e")] // max 32-bit int
    [Arguments(-2147483648, "i-2147483648e")] // min 32-bit int
    [Arguments(9223372036854775807, "i9223372036854775807e")] // max 64-bit
    [Arguments(-9223372036854775808, "i-9223372036854775808e")] // min 64-bit
    [Arguments(7, "i7e")] // normalized leading zeros
    public async Task Should_Encode_BInt(long value, string expected)
    {
        await using var encoder = new BEncoder();
        var bint = new BInt(value);

        byte[] encoded = encoder.Encode(bint);
        string result = Encoding.UTF8.GetString(encoded);

        result.ShouldBe(expected);
    }

    [Test]
    [Arguments("4:spam", "spam")]
    [Arguments("12:hello world!", "hello world!")]
    [Arguments("0:", "")]
    [Arguments("5:12345", "12345")]
    [Arguments("3:foo", "foo")]
    [Arguments("9:test data", "test data")]
    [Arguments("11:hello_world", "hello_world")]
    [Arguments("6:!@#$%^", "!@#$%^")]
    [Arguments("15:multi\nline\ndata", "multi\nline\ndata")]
    [Arguments("12:áéíóúñ", "áéíóúñ")]
    [Arguments("9:🙂emoji", "🙂emoji")]
    [Arguments("1:a", "a")]
    public async Task Should_Encode_BString(string expectedEncoded, string value)
    {
        await using var encoder = new BEncoder();
        var bstring = new BString(value);

        byte[] encoded = encoder.Encode(bstring);
        string result = Encoding.UTF8.GetString(encoded);

        result.ShouldBe(expectedEncoded);
    }

    [Test]
    [MethodDataSource(typeof(BDictionaryData), nameof(BDictionaryData.GetTestData))]
    [MethodDataSource(typeof(BlistData), nameof(BlistData.GetTestData))]
    public async Task Shoud_Encode_Collections(string expectedEncoded, IBencodingNode node)
    {
        await using var encoder = new BEncoder();

        byte[] encoded = encoder.Encode(node);
        string result = Encoding.UTF8.GetString(encoded);

        result.ShouldBe(expectedEncoded);
    }
}
