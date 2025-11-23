using System.Text;
using Netorrent.Bencoding;
using Netorrent.Bencoding.Structs;
using Netorrent.Tests.Bencoding.Data;
using Shouldly;

namespace Netorrent.Tests.Bencoding;

public class BDecoderTests
{
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
    public void Should_decode_BString(string input, string actual)
    {
        var bytes = Encoding.UTF8.GetBytes(input);
        var decoder = new BDecoder(bytes);

        var decoded = (BString)decoder.Decode();

        decoded.Data.ShouldBeEquivalentTo(actual);
    }

    [Test]
    [Arguments("i0e", 0)]
    [Arguments("i1e", 1)]
    [Arguments("i42e", 42)]
    [Arguments("i-1e", -1)]
    [Arguments("i999e", 999)]
    [Arguments("i123456789e", 123456789)]
    [Arguments("i-99999e", -99999)]
    [Arguments("i2147483647e", 2147483647)] // max 32-bit int
    [Arguments("i-2147483648e", -2147483648)] // min 32-bit int
    [Arguments("i9223372036854775807e", 9223372036854775807L)] // max 64-bit
    [Arguments("i-9223372036854775808e", -9223372036854775808L)] // min 64-bit
    [Arguments("i007e", 7)] // technically invalid in strict bencoding (leading zeros), but useful for tests
    [Arguments("i-0e", 0)] // another edge case: negative zero normalization
    [Arguments("i000000e", 0)] // leading zeros case
    public void Should_decode_BInt(string input, long actual)
    {
        var bytes = Encoding.UTF8.GetBytes(input);
        var decoder = new BDecoder(bytes);

        var decoded = (BInt)decoder.Decode();

        decoded.Data.ShouldBeEquivalentTo(actual);
    }

    [Test]
    [MethodDataSource(typeof(BDictionaryData), nameof(BDictionaryData.GetTestData))]
    [MethodDataSource(typeof(BlistData), nameof(BlistData.GetTestData))]
    public void Should_decode_collections(string input, IBencodingNode actual)
    {
        var bytes = Encoding.UTF8.GetBytes(input);
        var decoder = new BDecoder(bytes);

        var decoded = decoder.Decode();

        decoded.ShouldBeEquivalentTo(actual);
    }
}
