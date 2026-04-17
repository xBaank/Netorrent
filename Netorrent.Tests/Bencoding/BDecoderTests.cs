using System.Text;
using Netorrent.Bencoding;
using Netorrent.Bencoding.Structs;
using Netorrent.Exceptions;
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
    [Arguments("i2147483647e", int.MaxValue)]
    [Arguments("i-2147483648e", int.MinValue)]
    [Arguments("i9223372036854775807e", long.MaxValue)]
    [Arguments("i-9223372036854775808e", long.MinValue)]
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
    [Arguments(1)]
    [Arguments(5)]
    [Arguments(64)]
    public async Task Should_Decode_Structure_At_Exact_Depth_Limit(
        int maxDepth,
        CancellationToken cancellationToken
    )
    {
        // N nested lists → innermost decoded at depth N-1.
        // To hit exactly maxDepth, use maxDepth+1 lists.
        var n = maxDepth + 1;
        var input = new string('l', n) + new string('e', n);
        var bytes = Encoding.UTF8.GetBytes(input);
        var stream = new MemoryStream(bytes);
        await using var decoder = new BDecoder(stream, maxDepth);

        var decoded = await decoder.DecodeAsync(cancellationToken);

        decoded.ShouldBeOfType<BList>();
    }

    [Test]
    [Arguments(0)]
    [Arguments(1)]
    [Arguments(5)]
    [Arguments(64)]
    public async Task Should_Throw_When_Depth_Exceeds_Limit(
        int maxDepth,
        CancellationToken cancellationToken
    )
    {
        // maxDepth+2 lists → innermost decoded at depth maxDepth+1 > maxDepth → throws.
        var n = maxDepth + 2;
        var input = new string('l', n) + new string('e', n);
        var bytes = Encoding.UTF8.GetBytes(input);
        var stream = new MemoryStream(bytes);
        await using var decoder = new BDecoder(stream, maxDepth);

        await Should.ThrowAsync<BencodingException>(async () =>
            await decoder.DecodeAsync(cancellationToken)
        );
    }

    [Test]
    public async Task Should_Throw_When_Default_Depth_Limit_Exceeded(
        CancellationToken cancellationToken
    )
    {
        // 66 lists → innermost at depth 65 > default limit of 64.
        const int n = 66;
        var input = new string('l', n) + new string('e', n);
        var bytes = Encoding.UTF8.GetBytes(input);
        var stream = new MemoryStream(bytes);
        await using var decoder = new BDecoder(stream);

        await Should.ThrowAsync<BencodingException>(async () =>
            await decoder.DecodeAsync(cancellationToken)
        );
    }

    [Test]
    public async Task Should_Throw_On_Deeply_Nested_Dictionary(CancellationToken cancellationToken)
    {
        // d1:x d1:x ... i0e e e e
        const int maxDepth = 4;
        const int depth = maxDepth + 1;
        var input =
            string.Concat(Enumerable.Repeat("d1:x", depth)) + "i0e" + new string('e', depth);
        var bytes = Encoding.UTF8.GetBytes(input);
        var stream = new MemoryStream(bytes);
        await using var decoder = new BDecoder(stream, maxDepth);

        await Should.ThrowAsync<BencodingException>(async () =>
            await decoder.DecodeAsync(cancellationToken)
        );
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
