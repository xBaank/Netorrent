using System.Text;
using Netorrent.Bencoding;
using Netorrent.Bencoding.Structs;
using Shouldly;

namespace Netorrent.Tests.Bencoding;

public class BencoderTests
{
    [Theory]
    [InlineData(0, "i0e")]
    [InlineData(1, "i1e")]
    [InlineData(42, "i42e")]
    [InlineData(-1, "i-1e")]
    [InlineData(999, "i999e")]
    [InlineData(123456789, "i123456789e")]
    [InlineData(-99999, "i-99999e")]
    [InlineData(2147483647, "i2147483647e")] // max 32-bit int
    [InlineData(-2147483648, "i-2147483648e")] // min 32-bit int
    [InlineData(9223372036854775807, "i9223372036854775807e")] // max 64-bit
    [InlineData(-9223372036854775808, "i-9223372036854775808e")] // min 64-bit
    [InlineData(7, "i7e")] // normalized leading zeros
    public void CanEncodeBInt(long value, string expected)
    {
        var encoder = new BEncoder();
        var bint = new BInt(value);

        byte[] encoded = encoder.Encode(bint);
        string result = Encoding.UTF8.GetString(encoded);

        result.ShouldBe(expected);
    }

    [Theory]
    [InlineData("4:spam", "spam")]
    [InlineData("12:hello world!", "hello world!")]
    [InlineData("0:", "")]
    [InlineData("5:12345", "12345")]
    [InlineData("3:foo", "foo")]
    [InlineData("9:test data", "test data")]
    [InlineData("11:hello_world", "hello_world")]
    [InlineData("6:!@#$%^", "!@#$%^")]
    [InlineData("15:multi\nline\ndata", "multi\nline\ndata")]
    [InlineData("12:áéíóúñ", "áéíóúñ")]
    [InlineData("9:🙂emoji", "🙂emoji")]
    [InlineData("1:a", "a")]
    public void CanEncodeBString(string expectedEncoded, string value)
    {
        var encoder = new BEncoder();
        var bstring = new BString(value);

        byte[] encoded = encoder.Encode(bstring);
        string result = Encoding.UTF8.GetString(encoded);

        result.ShouldBe(expectedEncoded);
    }

    [Theory]
    [ClassData(typeof(BDictionaryData))]
    [ClassData(typeof(BlistData))]
    public void CanEncodeNonPrimitives(string expectedEncoded, IBencodingType dictionary)
    {
        var encoder = new BEncoder();

        byte[] encoded = encoder.Encode(dictionary);
        string result = Encoding.UTF8.GetString(encoded);

        result.ShouldBe(expectedEncoded);
    }
}
