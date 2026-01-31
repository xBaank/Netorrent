using System.Text;
using Netorrent.Bencoding;
using Netorrent.Bencoding.Structs;
using Netorrent.Tests.Bencoding.Data;
using Shouldly;

namespace Netorrent.Tests.Bencoding;

[Timeout(5_000)]
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
    public async Task Should_Decode_BString(
        string input,
        string actual,
        CancellationToken cancellationToken
    )
    {
        var bytes = Encoding.UTF8.GetBytes(input);
        var stream = new MemoryStream(bytes);
        await using var decoder = new BDecoder(stream);

        var decoded = (BString)await decoder.DecodeAsync(cancellationToken);

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
    [Arguments("i2147483647e", 2147483647)]
    [Arguments("i-2147483648e", -2147483648)]
    [Arguments("i9223372036854775807e", 9223372036854775807L)]
    [Arguments("i-9223372036854775808e", -9223372036854775808L)]
    public async Task Should_Decode_BInt(
        string input,
        long actual,
        CancellationToken cancellationToken
    )
    {
        var bytes = Encoding.UTF8.GetBytes(input);
        var stream = new MemoryStream(bytes);
        await using var decoder = new BDecoder(stream);

        var decoded = (BInt)await decoder.DecodeAsync(cancellationToken);

        decoded.Data.ShouldBeEquivalentTo(actual);
    }

    [Test]
    [MethodDataSource(typeof(BDictionaryData), nameof(BDictionaryData.GetTestData))]
    [MethodDataSource(typeof(BlistData), nameof(BlistData.GetTestData))]
    public async Task Should_Decode_Collections(
        string input,
        IBencodingNode actual,
        CancellationToken cancellationToken
    )
    {
        var bytes = Encoding.UTF8.GetBytes(input);
        var stream = new MemoryStream(bytes);
        await using var decoder = new BDecoder(stream);

        var decoded = await decoder.DecodeAsync(cancellationToken);

        decoded.ShouldBeEquivalentTo(actual);
    }

    [Test]
    [MethodDataSource(
        typeof(InvalidBencodingData),
        nameof(InvalidBencodingData.GetMalformedStringData)
    )]
    [MethodDataSource(
        typeof(InvalidBencodingData),
        nameof(InvalidBencodingData.GetCorruptedIntegerData)
    )]
    [MethodDataSource(
        typeof(InvalidBencodingData),
        nameof(InvalidBencodingData.GetStructureValidationData)
    )]
    [MethodDataSource(
        typeof(InvalidBencodingData),
        nameof(InvalidBencodingData.GetBufferSafetyData)
    )]
    [MethodDataSource(
        typeof(InvalidBencodingData),
        nameof(InvalidBencodingData.GetInvalidTypeData)
    )]
    public async Task Should_Throw_Exception_For_Invalid_Data(
        string input,
        string description,
        CancellationToken cancellationToken
    )
    {
        var bytes = Encoding.UTF8.GetBytes(input);
        var stream = new MemoryStream(bytes);
        await using var decoder = new BDecoder(stream);

        await Should.ThrowAsync<Exception>(
            async () => await decoder.DecodeAsync(cancellationToken),
            description
        );
    }
}
