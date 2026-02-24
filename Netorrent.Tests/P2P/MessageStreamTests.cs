using System.Buffers;
using System.Buffers.Binary;
using Netorrent.Exceptions;
using Netorrent.Extensions;
using Netorrent.IO;
using Netorrent.Other;
using Netorrent.P2P.Messages;
using Netorrent.Tests.Fakes;
using R3;
using Shouldly;

namespace Netorrent.Tests.P2P;

[Timeout(5_000)]
internal class MessageStreamTests
{
    static PeerId PeerId => new();
    static byte[] InfoHash => new byte[20];

    [Test]
    public async Task Should_Receive_Choke(CancellationToken cancellationToken)
    {
        Subject<Message> messages = new();
        async ValueTask messageHandler(Message message, CancellationToken ct)
        {
            messages.OnNext(message);
        }
        var message = Message.Choke.Value;
        using var rawData = Message.SerializeChoke(message);
        var receviedTask = messages.FirstAsync(cancellationToken);

        await using var memoryStream = new FakeMemoryStream();
        await memoryStream.WriteAsync(rawData.Memory, cancellationToken);
        await memoryStream.FlushAsync(cancellationToken);

        await using var messageStream = new MessageStream(
            memoryStream,
            Handshake.Create(InfoHash, PeerId.ToBytes()),
            120.Seconds,
            new Bitfield(5)
        );

        _ = messageStream.StartAsync(messageHandler, cancellationToken);
        var received = await receviedTask;

        received.ShouldBe(message);
    }

    [Test]
    public async Task Should_Send_Choke(CancellationToken cancellationToken)
    {
        ValueTask messageHandler(Message message, CancellationToken ct) => ValueTask.CompletedTask;
        var message = new Message.Choke();
        using var expectedData = Message.SerializeChoke(message);

        await using var memoryStream = new FakeMemoryStream();
        await using var messageStream = new MessageStream(
            memoryStream,
            Handshake.Create(InfoHash, PeerId.ToBytes()),
            120.Seconds,
            new Bitfield(5)
        );

        var itemTask = memoryStream.WrittenData.FirstAsync(cancellationToken);
        await messageStream.SendAsync(message, cancellationToken);
        _ = messageStream.StartAsync(messageHandler, cancellationToken);

        var item = await itemTask;
        item.Span.SequenceEqual(expectedData.Memory.Span).ShouldBeTrue();
    }

    [Test]
    public async Task Should_Receive_Unchoke(CancellationToken cancellationToken)
    {
        Subject<Message> messages = new();
        async ValueTask messageHandler(Message message, CancellationToken ct)
        {
            messages.OnNext(message);
        }
        var message = Message.Unchoke.Value;
        using var rawData = Message.SerializeUnChoke(message);
        var messageTask = messages.FirstAsync(cancellationToken);

        await using var memoryStream = new FakeMemoryStream();
        await memoryStream.WriteAsync(rawData.Memory, cancellationToken);
        await memoryStream.FlushAsync(cancellationToken);

        await using var messageStream = new MessageStream(
            memoryStream,
            Handshake.Create(InfoHash, PeerId.ToBytes()),
            120.Seconds,
            new Bitfield(5)
        );

        _ = messageStream.StartAsync(messageHandler, cancellationToken);
        var received = await messageTask;

        received.ShouldBe(message);
    }

    [Test]
    public async Task Should_Send_Unchoke(CancellationToken cancellationToken)
    {
        ValueTask messageHandler(Message message, CancellationToken ct) => ValueTask.CompletedTask;
        var message = new Message.Unchoke();
        using var expectedData = Message.SerializeUnChoke(message);

        await using var memoryStream = new FakeMemoryStream();
        await using var messageStream = new MessageStream(
            memoryStream,
            Handshake.Create(InfoHash, PeerId.ToBytes()),
            120.Seconds,
            new Bitfield(5)
        );

        var itemTask = memoryStream.WrittenData.FirstAsync(cancellationToken);
        await messageStream.SendAsync(message, cancellationToken);
        _ = messageStream.StartAsync(messageHandler, cancellationToken);

        var item = await itemTask;
        item.Span.SequenceEqual(expectedData.Memory.Span).ShouldBeTrue();
    }

    [Test]
    public async Task Should_Receive_Interested(CancellationToken cancellationToken)
    {
        Subject<Message> messages = new();
        async ValueTask messageHandler(Message message, CancellationToken ct)
        {
            messages.OnNext(message);
        }
        var message = Message.Interested.Value;
        using var rawData = Message.SerializeInterested(message);
        var messageTask = messages.FirstAsync(cancellationToken);

        await using var memoryStream = new FakeMemoryStream();
        await memoryStream.WriteAsync(rawData.Memory, cancellationToken);
        await memoryStream.FlushAsync(cancellationToken);

        await using var messageStream = new MessageStream(
            memoryStream,
            Handshake.Create(InfoHash, PeerId.ToBytes()),
            120.Seconds,
            new Bitfield(5)
        );

        _ = messageStream.StartAsync(messageHandler, cancellationToken);
        var received = await messageTask;

        received.ShouldBe(message);
    }

    [Test]
    public async Task Should_Send_Interested(CancellationToken cancellationToken)
    {
        ValueTask messageHandler(Message message, CancellationToken ct) => ValueTask.CompletedTask;
        var message = new Message.Interested();
        using var expectedData = Message.SerializeInterested(message);

        await using var memoryStream = new FakeMemoryStream();
        await using var messageStream = new MessageStream(
            memoryStream,
            Handshake.Create(InfoHash, PeerId.ToBytes()),
            120.Seconds,
            new Bitfield(5)
        );

        var itemTask = memoryStream.WrittenData.FirstAsync(cancellationToken);
        await messageStream.SendAsync(message, cancellationToken);
        _ = messageStream.StartAsync(messageHandler, cancellationToken);

        var item = await itemTask;
        item.Span.SequenceEqual(expectedData.Memory.Span).ShouldBeTrue();
    }

    [Test]
    public async Task Should_Receive_NotInterested(CancellationToken cancellationToken)
    {
        Subject<Message> messages = new();
        async ValueTask messageHandler(Message message, CancellationToken ct)
        {
            messages.OnNext(message);
        }
        var message = Message.NotInterested.Value;
        using var rawData = Message.SerializeNotInterested(message);
        var messageTask = messages.FirstAsync(cancellationToken);

        await using var memoryStream = new FakeMemoryStream();
        await memoryStream.WriteAsync(rawData.Memory, cancellationToken);
        await memoryStream.FlushAsync(cancellationToken);

        await using var messageStream = new MessageStream(
            memoryStream,
            Handshake.Create(InfoHash, PeerId.ToBytes()),
            120.Seconds,
            new Bitfield(5)
        );

        _ = messageStream.StartAsync(messageHandler, cancellationToken);
        var received = await messageTask;

        received.ShouldBe(message);
    }

    [Test]
    public async Task Should_Send_NotInterested(CancellationToken cancellationToken)
    {
        ValueTask messageHandler(Message message, CancellationToken ct) => ValueTask.CompletedTask;
        var message = new Message.NotInterested();
        using var expectedData = Message.SerializeNotInterested(message);

        await using var memoryStream = new FakeMemoryStream();
        await using var messageStream = new MessageStream(
            memoryStream,
            Handshake.Create(InfoHash, PeerId.ToBytes()),
            120.Seconds,
            new Bitfield(5)
        );

        var itemTask = memoryStream.WrittenData.FirstAsync(cancellationToken);
        await messageStream.SendAsync(message, cancellationToken);
        _ = messageStream.StartAsync(messageHandler, cancellationToken);

        var item = await itemTask;
        item.Span.SequenceEqual(expectedData.Memory.Span).ShouldBeTrue();
    }

    [Test]
    public async Task Should_Receive_Have(CancellationToken cancellationToken)
    {
        Subject<Message> messages = new();
        async ValueTask messageHandler(Message message, CancellationToken ct)
        {
            messages.OnNext(message);
        }
        const int pieceIndex = 42;
        var message = new Message.Have(pieceIndex);
        using var rawData = Message.SerializeHave(message);
        var messageTask = messages.FirstAsync(cancellationToken);

        await using var memoryStream = new FakeMemoryStream();
        await memoryStream.WriteAsync(rawData.Memory, cancellationToken);
        await memoryStream.FlushAsync(cancellationToken);

        await using var messageStream = new MessageStream(
            memoryStream,
            Handshake.Create(InfoHash, PeerId.ToBytes()),
            120.Seconds,
            new Bitfield(5)
        );

        _ = messageStream.StartAsync(messageHandler, cancellationToken);
        var received = await messageTask;

        received.ShouldBe(message);
    }

    [Test]
    public async Task Should_Send_Have(CancellationToken cancellationToken)
    {
        ValueTask messageHandler(Message message, CancellationToken ct) => ValueTask.CompletedTask;
        var message = new Message.Have(1);
        using var expectedData = Message.SerializeHave(message);

        await using var memoryStream = new FakeMemoryStream();
        await using var messageStream = new MessageStream(
            memoryStream,
            Handshake.Create(InfoHash, PeerId.ToBytes()),
            120.Seconds,
            new Bitfield(5)
        );

        var itemTask = memoryStream.WrittenData.FirstAsync(cancellationToken);
        await messageStream.SendAsync(message, cancellationToken);
        _ = messageStream.StartAsync(messageHandler, cancellationToken);

        var item = await itemTask;
        item.Span.SequenceEqual(expectedData.Memory.Span).ShouldBeTrue();
    }

    [Test]
    public async Task Should_Receive_Bitfield(CancellationToken cancellationToken)
    {
        Subject<Message> messages = new();
        async ValueTask messageHandler(Message message, CancellationToken ct)
        {
            messages.OnNext(message);
        }
        var bitfield = new Bitfield(5, true);
        using var bitfieldData = bitfield.ToRentedArray();
        var message = new Message.BitfieldMessage(bitfield);
        using var rawData = Message.SerializeBitfield(message);

        await using var memoryStream = new FakeMemoryStream();
        await memoryStream.WriteAsync(rawData.Memory, cancellationToken);
        await memoryStream.FlushAsync(cancellationToken);
        var messageTask = messages.FirstAsync(cancellationToken);

        await using var messageStream = new MessageStream(
            memoryStream,
            Handshake.Create(InfoHash, PeerId.ToBytes()),
            120.Seconds,
            new Bitfield(5)
        );

        _ = messageStream.StartAsync(messageHandler, cancellationToken);
        var received = await messageTask;

        received.MatchBitfieldMessage(
            i => i.Bitfield.Length.ShouldBe(message.Bitfield.Length),
            () => throw new InvalidOperationException()
        );
    }

    [Test]
    public async Task Should_Send_Bitfield(CancellationToken cancellationToken)
    {
        ValueTask messageHandler(Message message, CancellationToken ct) => ValueTask.CompletedTask;
        var bitfield = new Bitfield(5, true);
        using var bitfieldData = bitfield.ToRentedArray();
        var message = new Message.BitfieldMessage(bitfield);
        using var expectedData = Message.SerializeBitfield(message);

        await using var memoryStream = new FakeMemoryStream();
        await using var messageStream = new MessageStream(
            memoryStream,
            Handshake.Create(InfoHash, PeerId.ToBytes()),
            120.Seconds,
            new Bitfield(5)
        );

        var itemTask = memoryStream.WrittenData.FirstAsync(cancellationToken);
        await messageStream.SendAsync(message, cancellationToken);
        _ = messageStream.StartAsync(messageHandler, cancellationToken);

        var item = await itemTask;
        item.Span.SequenceEqual(expectedData.Memory.Span).ShouldBeTrue();
    }

    [Test]
    public async Task Should_Receive_Request(CancellationToken cancellationToken)
    {
        Subject<Message> messages = new();
        async ValueTask messageHandler(Message message, CancellationToken ct)
        {
            messages.OnNext(message);
        }
        const int index = 1;
        const int begin = 16384;
        const int length = 16384;
        var message = new Message.RequestBlockMessage(index, begin, length);
        using var rawData = Message.SerializeRequest(message);
        var messageTask = messages.FirstAsync(cancellationToken);

        await using var memoryStream = new FakeMemoryStream();
        await memoryStream.WriteAsync(rawData.Memory, cancellationToken);
        await memoryStream.FlushAsync(cancellationToken);

        await using var messageStream = new MessageStream(
            memoryStream,
            Handshake.Create(InfoHash, PeerId.ToBytes()),
            120.Seconds,
            new Bitfield(5)
        );

        _ = messageStream.StartAsync(messageHandler, cancellationToken);
        var received = await messageTask;

        received.ShouldBe(message);
    }

    [Test]
    public async Task Should_Send_Request(CancellationToken cancellationToken)
    {
        ValueTask messageHandler(Message message, CancellationToken ct) => ValueTask.CompletedTask;
        const int index = 1;
        const int begin = 16384;
        const int length = 16384;
        var message = new Message.RequestBlockMessage(index, begin, length);
        using var expectedData = Message.SerializeRequest(message);

        await using var memoryStream = new FakeMemoryStream();
        await using var messageStream = new MessageStream(
            memoryStream,
            Handshake.Create(InfoHash, PeerId.ToBytes()),
            120.Seconds,
            new Bitfield(5)
        );

        var itemTask = memoryStream.WrittenData.FirstAsync(cancellationToken);
        await messageStream.SendAsync(message, cancellationToken);
        _ = messageStream.StartAsync(messageHandler, cancellationToken);

        var item = await itemTask;
        item.Span.SequenceEqual(expectedData.Memory.Span).ShouldBeTrue();
    }

    [Test]
    public async Task Should_Receive_Cancel(CancellationToken cancellationToken)
    {
        Subject<Message> messages = new();
        async ValueTask messageHandler(Message message, CancellationToken ct)
        {
            messages.OnNext(message);
        }
        const int index = 2;
        const int begin = 32768;
        const int length = 8192;
        var message = new Message.CancelMessage(index, begin, length);
        using var rawData = Message.SerializeCancel(message);
        var messageTask = messages.FirstAsync(cancellationToken);

        await using var memoryStream = new FakeMemoryStream();
        await memoryStream.WriteAsync(rawData.Memory, cancellationToken);
        await memoryStream.FlushAsync(cancellationToken);

        await using var messageStream = new MessageStream(
            memoryStream,
            Handshake.Create(InfoHash, PeerId.ToBytes()),
            120.Seconds,
            new Bitfield(5)
        );

        _ = messageStream.StartAsync(messageHandler, cancellationToken);
        var received = await messageTask;

        received.ShouldBe(message);
    }

    [Test]
    public async Task Should_Send_Cancel(CancellationToken cancellationToken)
    {
        ValueTask messageHandler(Message message, CancellationToken ct) => ValueTask.CompletedTask;
        const int index = 1;
        const int begin = 16384;
        const int length = 16384;
        var message = new Message.CancelMessage(index, begin, length);
        using var expectedData = Message.SerializeCancel(message);

        await using var memoryStream = new FakeMemoryStream();
        await using var messageStream = new MessageStream(
            memoryStream,
            Handshake.Create(InfoHash, PeerId.ToBytes()),
            120.Seconds,
            new Bitfield(5)
        );

        var itemTask = memoryStream.WrittenData.FirstAsync(cancellationToken);
        await messageStream.SendAsync(message, cancellationToken);
        _ = messageStream.StartAsync(messageHandler, cancellationToken);

        var item = await itemTask;
        item.Span.SequenceEqual(expectedData.Memory.Span).ShouldBeTrue();
    }

    [Test]
    public async Task Should_Receive_Piece(CancellationToken cancellationToken)
    {
        Subject<Message> messages = new();
        async ValueTask messageHandler(Message message, CancellationToken ct)
        {
            messages.OnNext(message);
        }
        const int index = 3;
        const int begin = 0;
        var blockData = new byte[16 * 1024];

        using var rentedBlock = new RentedArray<byte>(blockData.Length);
        blockData.AsMemory().CopyTo(rentedBlock.Memory);

        var message = new Message.BlockMessage(index, begin, rentedBlock);
        using var rawData = Message.SerializeBlock(message);
        var messageTask = messages.FirstAsync(cancellationToken);

        await using var memoryStream = new FakeMemoryStream();
        await memoryStream.WriteAsync(rawData.Memory, cancellationToken);
        await memoryStream.FlushAsync(cancellationToken);

        await using var messageStream = new MessageStream(
            memoryStream,
            Handshake.Create(InfoHash, PeerId.ToBytes()),
            120.Seconds,
            new Bitfield(5)
        );

        _ = messageStream.StartAsync(messageHandler, cancellationToken);
        var received = await messageTask;

        received.MatchBlockMessage(
            i =>
            {
                using var _ = i.Payload;
                i.Begin.ShouldBe(message.Begin);
                i.Index.ShouldBe(message.Index);
                i.Payload.Memory.Span.SequenceEqual(blockData);
            },
            () => throw new InvalidOperationException()
        );
    }

    [Test]
    public async Task Should_Send_Piece(CancellationToken cancellationToken)
    {
        ValueTask messageHandler(Message message, CancellationToken ct) => ValueTask.CompletedTask;
        const int index = 1;
        const int begin = 16384;
        var blockData = new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05 };

        using var rentedBlock = new RentedArray<byte>(blockData.Length);
        using var rentedBlock2 = new RentedArray<byte>(blockData.Length);
        blockData.AsMemory().CopyTo(rentedBlock.Memory);
        blockData.AsMemory().CopyTo(rentedBlock2.Memory);

        var message = new Message.BlockMessage(index, begin, rentedBlock);
        var expectedmessage = message with { Payload = rentedBlock2 };
        using var expectedData = Message.SerializeBlock(message);

        await using var memoryStream = new FakeMemoryStream();
        await using var messageStream = new MessageStream(
            memoryStream,
            Handshake.Create(InfoHash, PeerId.ToBytes()),
            120.Seconds,
            new Bitfield(5)
        );

        var itemTask = memoryStream.WrittenData.FirstAsync(cancellationToken);
        await messageStream.SendAsync(expectedmessage, cancellationToken);
        _ = messageStream.StartAsync(messageHandler, cancellationToken);

        var item = await itemTask;
        item.Span.SequenceEqual(expectedData.Memory.Span).ShouldBeTrue();
    }

    [Test]
    public async Task Should_Receive_KeepAlive(CancellationToken cancellationToken)
    {
        Subject<Message> messages = new();
        async ValueTask messageHandler(Message message, CancellationToken ct)
        {
            messages.OnNext(message);
        }
        var message = Message.KeepAlive.Value;
        using var rawData = Message.SerializeKeepAlive(message);
        var messageTask = messages.FirstAsync(cancellationToken);

        await using var memoryStream = new FakeMemoryStream();
        await memoryStream.WriteAsync(rawData.Memory, cancellationToken);
        await memoryStream.FlushAsync(cancellationToken);

        await using var messageStream = new MessageStream(
            memoryStream,
            Handshake.Create(InfoHash, PeerId.ToBytes()),
            120.Seconds,
            new Bitfield(5)
        );

        _ = messageStream.StartAsync(messageHandler, cancellationToken);
        var received = await messageTask;

        received.ShouldBe(message);
    }

    [Test]
    public async Task Should_Send_KeepAlive(CancellationToken cancellationToken)
    {
        ValueTask messageHandler(Message message, CancellationToken ct) => ValueTask.CompletedTask;
        var message = Message.KeepAlive.Value;
        using var expectedData = Message.SerializeKeepAlive(message);

        await using var memoryStream = new FakeMemoryStream();
        await using var messageStream = new MessageStream(
            memoryStream,
            Handshake.Create(InfoHash, PeerId.ToBytes()),
            120.Seconds,
            new Bitfield(5)
        );

        var itemTask = memoryStream.WrittenData.FirstAsync(cancellationToken);
        await messageStream.SendAsync(message, cancellationToken);
        _ = messageStream.StartAsync(messageHandler, cancellationToken);

        var item = await itemTask;
        item.Span.SequenceEqual(expectedData.Memory.Span).ShouldBeTrue();
    }

    [Test]
    public async Task Should_Timeout(CancellationToken cancellationToken)
    {
        Subject<Message> messages = new();
        ValueTask messageHandler(Message message, CancellationToken ct) => ValueTask.CompletedTask;
        await using var memoryStream = new FakeMemoryStream(true);
        await using var messageStream = new MessageStream(
            memoryStream,
            Handshake.Create(InfoHash, PeerId.ToBytes()),
            0.1.Seconds,
            new Bitfield(5)
        );

        var task = messageStream.StartAsync(messageHandler, cancellationToken);
        try
        {
            await task;
        }
        catch (OperationCanceledException ex)
        {
            ex.CancellationToken.ShouldNotBe(cancellationToken);
            return;
        }

        throw new InvalidOperationException();
    }

    [Test]
    public async Task Should_Cancel(CancellationToken cancellationToken)
    {
        ValueTask messageHandler(Message message, CancellationToken ct) => ValueTask.CompletedTask;
        await using var memoryStream = new FakeMemoryStream(true);
        await using var messageStream = new MessageStream(
            memoryStream,
            Handshake.Create(InfoHash, PeerId.ToBytes()),
            120.Seconds,
            new Bitfield(5)
        );
        var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.Cancel();
        var streamTask = messageStream.StartAsync(messageHandler, cts.Token);

        try
        {
            await streamTask;
        }
        catch (OperationCanceledException ex)
        {
            ex.CancellationToken.ShouldNotBe(cts.Token);
            return;
        }

        throw new InvalidOperationException();
    }

    [Test]
    [MethodDataSource(nameof(GetInvalidData))]
    public async Task Should_Throw_On_Invalid_Data(
        byte[] invalidData,
        CancellationToken cancellationToken
    )
    {
        Subject<Message> messages = new();
        async ValueTask messageHandler(Message message, CancellationToken ct)
        {
            messages.OnNext(message);
        }
        await using var memoryStream = new FakeMemoryStream();
        await using var messageStream = new MessageStream(
            memoryStream,
            Handshake.Create(InfoHash, PeerId.ToBytes()),
            1.Seconds,
            new Bitfield(5)
        );
        await memoryStream.WriteAsync(invalidData, cancellationToken);
        await memoryStream.FlushAsync(cancellationToken);

        var task = messageStream.StartAsync(messageHandler, cancellationToken);

        var messageTask = messages.Take(4).ToListAsync(cancellationToken);

        try
        {
            await task;
        }
        catch (Exception ex)
            when (ex
                    is OperationCanceledException
                        or EndOfStreamException
                        or BitorrentProtocolViolationException
            )
        {
            return;
        }

        throw new InvalidOperationException();
    }

    //TODO do some fuzzing to get invalid data
    public static IEnumerable<Func<byte[]>> GetInvalidData()
    {
        // random garbage
        yield return () => [5, 4, 43, 44, 123, 244, 99, 32, 0, 55, 255];

        // length = 1 but missing ID
        yield return () => [0, 0, 0, 1];

        // truncated payload
        yield return () => [0, 0, 0, 10, 1, 2];

        // keep-alive followed by garbage
        yield return () => [0, 0, 0, 0, 99];

        // invalid message ID
        yield return () => [0, 0, 0, 1, 255];

        // payload shorter than declared
        yield return () => [0, 0, 0, 5, 4, 0, 1];

        // payload longer than declared
        yield return () => [0, 0, 0, 1, 0, 99, 88];

        // negative length
        yield return () => [255, 255, 255, 255];

        // absurdly large length
        yield return () => [127, 255, 255, 255];

        // invalid piece message
        yield return () => [0, 0, 0, 2, 7, 0];
    }
}
