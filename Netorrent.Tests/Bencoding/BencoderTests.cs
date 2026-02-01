using System.Text;
using Netorrent.Bencoding;
using Netorrent.Bencoding.Structs;
using Netorrent.Tests.Bencoding.Data;
using Shouldly;

namespace Netorrent.Tests.Bencoding;

public class BEncoderTests
{
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
    [Arguments("i9223372036854775807e", 9223372036854775807)] // max 64-bit
    [Arguments("i-9223372036854775808e", -9223372036854775808)] // min 64-bit
    [Arguments("i7e", 7)] // normalized leading zeros
    public async Task Should_Encode_BInt(
        string expected,
        long value,
        CancellationToken cancellationToken
    )
    {
        var memoryStream = new MemoryStream();
        await using var encoder = new BEncoder(memoryStream);
        var bint = new BInt(value);

        await encoder.EncodeAsync(bint, cancellationToken);
        string result = Encoding.UTF8.GetString(memoryStream.ToArray());

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
    public async Task Should_Encode_BString(
        string expected,
        string value,
        CancellationToken cancellationToken
    )
    {
        var memoryStream = new MemoryStream();
        await using var encoder = new BEncoder(memoryStream);
        var bint = new BString(value);

        await encoder.EncodeAsync(bint, cancellationToken);
        string result = Encoding.UTF8.GetString(memoryStream.ToArray());

        result.ShouldBe(expected);
    }

    [Test]
    [MethodDataSource(typeof(BDictionaryData), nameof(BDictionaryData.GetTestData))]
    [MethodDataSource(typeof(BlistData), nameof(BlistData.GetTestData))]
    public async Task Shoud_Encode_Collections(
        string expected,
        IBencodingNode node,
        CancellationToken cancellationToken
    )
    {
        var memoryStream = new MemoryStream();
        await using var encoder = new BEncoder(memoryStream);

        await encoder.EncodeAsync(node, cancellationToken);
        string result = Encoding.UTF8.GetString(memoryStream.ToArray());

        result.ShouldBe(expected);
    }
}
