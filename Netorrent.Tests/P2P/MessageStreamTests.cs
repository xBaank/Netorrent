using System.Buffers;
using System.Buffers.Binary;
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
    [Test]
    public async Task Should_Receive_Choke(CancellationToken cancellationToken)
    {
        var peerId = new PeerId();
        var infohash = new byte[20];

        // Write message data first
        using var message = Message.CreateChoke();
        using var rawData = message.ToRentedArray();

        await using var memoryStream = new FakeMemoryStream();
        await memoryStream.WriteAsync(rawData.Memory, cancellationToken);
        await memoryStream.FlushAsync(cancellationToken);

        await using var messageStream = new MessageStream(
            memoryStream,
            Handshake.Create(infohash, peerId.ToBytes()),
            120.Seconds
        );

        var streamTask = messageStream.StartAsync(cancellationToken);
        using var receivedMessage = await messageStream.IncomingMessages.ReadAsync(
            cancellationToken
        );
        receivedMessage.Payload.ShouldBeNull();
        receivedMessage.Id.ShouldBe(Message.Choke);
    }

    [Test]
    public async Task Should_Send_Choke(CancellationToken cancellationToken)
    {
        var peerId = new PeerId();
        var infohash = new byte[20];

        using var message = Message.CreateChoke();
        using var expectedData = message.ToRentedArray();

        await using var memoryStream = new FakeMemoryStream();
        await using var messageStream = new MessageStream(
            memoryStream,
            Handshake.Create(infohash, peerId.ToBytes()),
            120.Seconds
        );

        var itemTask = memoryStream.WrittenData.FirstAsync(cancellationToken);
        await messageStream.OutgoingMessages.WriteAsync(message, cancellationToken);
        var streamTask = messageStream.StartAsync(cancellationToken);

        var item = await itemTask;
        item.Span.SequenceEqual(expectedData.Memory.Span).ShouldBeTrue();
    }

    [Test]
    public async Task Should_Receive_Unchoke(CancellationToken cancellationToken)
    {
        var peerId = new PeerId();
        var infohash = new byte[20];

        using var message = Message.CreateUnchoke();
        using var rawData = message.ToRentedArray();

        await using var memoryStream = new FakeMemoryStream();
        await memoryStream.WriteAsync(rawData.Memory, cancellationToken);
        await memoryStream.FlushAsync(cancellationToken);

        await using var messageStream = new MessageStream(
            memoryStream,
            Handshake.Create(infohash, peerId.ToBytes()),
            120.Seconds
        );

        var streamTask = messageStream.StartAsync(cancellationToken);
        using var receivedMessage = await messageStream.IncomingMessages.ReadAsync(
            cancellationToken
        );
        receivedMessage.Payload.ShouldBeNull();
        receivedMessage.Id.ShouldBe(Message.Unchoke);
    }

    [Test]
    public async Task Should_Receive_Interested(CancellationToken cancellationToken)
    {
        var peerId = new PeerId();
        var infohash = new byte[20];

        using var message = Message.CreateInterested();
        using var rawData = message.ToRentedArray();

        await using var memoryStream = new FakeMemoryStream();
        await memoryStream.WriteAsync(rawData.Memory, cancellationToken);
        await memoryStream.FlushAsync(cancellationToken);

        await using var messageStream = new MessageStream(
            memoryStream,
            Handshake.Create(infohash, peerId.ToBytes()),
            120.Seconds
        );

        var streamTask = messageStream.StartAsync(cancellationToken);
        using var receivedMessage = await messageStream.IncomingMessages.ReadAsync(
            cancellationToken
        );
        receivedMessage.Payload.ShouldBeNull();
        receivedMessage.Id.ShouldBe(Message.Interested);
    }

    [Test]
    public async Task Should_Receive_NotInterested(CancellationToken cancellationToken)
    {
        var peerId = new PeerId();
        var infohash = new byte[20];

        using var message = Message.CreateNotInterested();
        using var rawData = message.ToRentedArray();

        await using var memoryStream = new FakeMemoryStream();
        await memoryStream.WriteAsync(rawData.Memory, cancellationToken);
        await memoryStream.FlushAsync(cancellationToken);

        await using var messageStream = new MessageStream(
            memoryStream,
            Handshake.Create(infohash, peerId.ToBytes()),
            120.Seconds
        );

        var streamTask = messageStream.StartAsync(cancellationToken);
        using var receivedMessage = await messageStream.IncomingMessages.ReadAsync(
            cancellationToken
        );
        receivedMessage.Payload.ShouldBeNull();
        receivedMessage.Id.ShouldBe(Message.NotInterested);
    }

    [Test]
    public async Task Should_Receive_Have(CancellationToken cancellationToken)
    {
        var peerId = new PeerId();
        var infohash = new byte[20];
        const int pieceIndex = 42;

        using var message = Message.CreateHave(pieceIndex);
        using var rawData = message.ToRentedArray();

        await using var memoryStream = new FakeMemoryStream();
        await memoryStream.WriteAsync(rawData.Memory, cancellationToken);
        await memoryStream.FlushAsync(cancellationToken);

        await using var messageStream = new MessageStream(
            memoryStream,
            Handshake.Create(infohash, peerId.ToBytes()),
            120.Seconds
        );

        var streamTask = messageStream.StartAsync(cancellationToken);
        using var receivedMessage = await messageStream.IncomingMessages.ReadAsync(
            cancellationToken
        );
        receivedMessage.Payload.ShouldNotBeNull();
        receivedMessage.Id.ShouldBe(Message.Have);

        // Verify piece index from payload
        var pieceIndexFromPayload = BinaryPrimitives.ReadInt32BigEndian(
            receivedMessage.Payload!.Memory.Span
        );
        pieceIndexFromPayload.ShouldBe(pieceIndex);
    }

    [Test]
    public async Task Should_Receive_Bitfield(CancellationToken cancellationToken)
    {
        var peerId = new PeerId();
        var infohash = new byte[20];
        var bitfieldData = new byte[] { 0xAA, 0x55, 0xFF }; // Example bitfield

        using var rentedBitfield = new RentedArray<byte>(
            ArrayPool<byte>.Shared.Rent(bitfieldData.Length),
            bitfieldData.Length
        );
        bitfieldData.AsMemory().CopyTo(rentedBitfield.Memory);

        using var message = Message.CreateBitfield(rentedBitfield);
        using var rawData = message.ToRentedArray();

        await using var memoryStream = new FakeMemoryStream();
        await memoryStream.WriteAsync(rawData.Memory, cancellationToken);
        await memoryStream.FlushAsync(cancellationToken);

        await using var messageStream = new MessageStream(
            memoryStream,
            Handshake.Create(infohash, peerId.ToBytes()),
            120.Seconds
        );

        var streamTask = messageStream.StartAsync(cancellationToken);
        using var receivedMessage = await messageStream.IncomingMessages.ReadAsync(
            cancellationToken
        );
        receivedMessage.Payload.ShouldNotBeNull();
        receivedMessage.Id.ShouldBe(Message.Bitfield);

        // Verify bitfield data
        receivedMessage.Payload!.Length.ShouldBe(bitfieldData.Length);
        receivedMessage.Payload.Memory.Span.SequenceEqual(bitfieldData).ShouldBeTrue();
    }

    [Test]
    public async Task Should_Receive_Request(CancellationToken cancellationToken)
    {
        var peerId = new PeerId();
        var infohash = new byte[20];
        const int index = 1;
        const int begin = 16384;
        const int length = 16384;

        using var message = Message.CreateRequest(index, begin, length);
        using var rawData = message.ToRentedArray();

        await using var memoryStream = new FakeMemoryStream();
        await memoryStream.WriteAsync(rawData.Memory, cancellationToken);
        await memoryStream.FlushAsync(cancellationToken);

        await using var messageStream = new MessageStream(
            memoryStream,
            Handshake.Create(infohash, peerId.ToBytes()),
            120.Seconds
        );

        var streamTask = messageStream.StartAsync(cancellationToken);
        using var receivedMessage = await messageStream.IncomingMessages.ReadAsync(
            cancellationToken
        );
        receivedMessage.Payload.ShouldNotBeNull();
        receivedMessage.Id.ShouldBe(Message.Request);
        receivedMessage.Payload!.Length.ShouldBe(12);

        var span = receivedMessage.Payload.Memory.Span;
        var receivedIndex = BinaryPrimitives.ReadInt32BigEndian(span[..4]);
        var receivedBegin = BinaryPrimitives.ReadInt32BigEndian(span.Slice(4, 4));
        var receivedLength = BinaryPrimitives.ReadInt32BigEndian(span.Slice(8, 4));

        receivedIndex.ShouldBe(index);
        receivedBegin.ShouldBe(begin);
        receivedLength.ShouldBe(length);
    }

    [Test]
    public async Task Should_Receive_Cancel(CancellationToken cancellationToken)
    {
        var peerId = new PeerId();
        var infohash = new byte[20];
        const int index = 2;
        const int begin = 32768;
        const int length = 8192;

        using var message = Message.CreateCancel(index, begin, length);
        using var rawData = message.ToRentedArray();

        await using var memoryStream = new FakeMemoryStream();
        await memoryStream.WriteAsync(rawData.Memory, cancellationToken);
        await memoryStream.FlushAsync(cancellationToken);

        await using var messageStream = new MessageStream(
            memoryStream,
            Handshake.Create(infohash, peerId.ToBytes()),
            120.Seconds
        );

        var streamTask = messageStream.StartAsync(cancellationToken);
        using var receivedMessage = await messageStream.IncomingMessages.ReadAsync(
            cancellationToken
        );
        receivedMessage.Payload.ShouldNotBeNull();
        receivedMessage.Id.ShouldBe(Message.Cancel);
        receivedMessage.Payload!.Length.ShouldBe(12);

        var span = receivedMessage.Payload.Memory.Span;
        var receivedIndex = BinaryPrimitives.ReadInt32BigEndian(span[..4]);
        var receivedBegin = BinaryPrimitives.ReadInt32BigEndian(span.Slice(4, 4));
        var receivedLength = BinaryPrimitives.ReadInt32BigEndian(span.Slice(8, 4));

        receivedIndex.ShouldBe(index);
        receivedBegin.ShouldBe(begin);
        receivedLength.ShouldBe(length);
    }

    [Test]
    public async Task Should_Receive_Piece(CancellationToken cancellationToken)
    {
        var peerId = new PeerId();
        var infohash = new byte[20];
        const int index = 3;
        const int begin = 0;
        var blockData = new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05 };

        using var rentedBlock = new RentedArray<byte>(
            ArrayPool<byte>.Shared.Rent(blockData.Length),
            blockData.Length
        );
        blockData.AsMemory().CopyTo(rentedBlock.Memory);

        using var message = Message.CreatePiece(index, begin, rentedBlock);
        using var rawData = message.ToRentedArray();

        await using var memoryStream = new FakeMemoryStream();
        await memoryStream.WriteAsync(rawData.Memory, cancellationToken);
        await memoryStream.FlushAsync(cancellationToken);

        await using var messageStream = new MessageStream(
            memoryStream,
            Handshake.Create(infohash, peerId.ToBytes()),
            120.Seconds
        );

        var streamTask = messageStream.StartAsync(cancellationToken);
        using var receivedMessage = await messageStream.IncomingMessages.ReadAsync(
            cancellationToken
        );
        receivedMessage.Payload.ShouldNotBeNull();
        receivedMessage.Id.ShouldBe(Message.Piece);
        receivedMessage.Payload!.Length.ShouldBe(8 + blockData.Length);

        var span = receivedMessage.Payload.Memory.Span;
        var receivedIndex = BinaryPrimitives.ReadInt32BigEndian(span[..4]);
        var receivedBegin = BinaryPrimitives.ReadInt32BigEndian(span.Slice(4, 4));
        var receivedBlock = span.Slice(8, blockData.Length);

        receivedIndex.ShouldBe(index);
        receivedBegin.ShouldBe(begin);
        receivedBlock.SequenceEqual(blockData).ShouldBeTrue();
    }

    [Test]
    public async Task Should_Receive_KeepAlive(CancellationToken cancellationToken)
    {
        var peerId = new PeerId();
        var infohash = new byte[20];

        using var message = Message.CreateKeepAlive();
        using var rawData = message.ToRentedArray();

        await using var memoryStream = new FakeMemoryStream();
        await memoryStream.WriteAsync(rawData.Memory, cancellationToken);
        await memoryStream.FlushAsync(cancellationToken);

        await using var messageStream = new MessageStream(
            memoryStream,
            Handshake.Create(infohash, peerId.ToBytes()),
            120.Seconds
        );

        var streamTask = messageStream.StartAsync(cancellationToken);
        using var receivedMessage = await messageStream.IncomingMessages.ReadAsync(
            cancellationToken
        );
        receivedMessage.Payload.ShouldBeNull();
        receivedMessage.Id.ShouldBe(Message.KeepAlive.Id); // Keep-alive has special ID
    }

    [Test]
    public async Task Should_Receive_Port(CancellationToken cancellationToken)
    {
        var peerId = new PeerId();
        var infohash = new byte[20];
        const int port = 6881;

        // Port message (ID 9) with 2-byte port payload
        var portBytes = ArrayPool<byte>.Shared.Rent(2);
        BinaryPrimitives.WriteInt16BigEndian(portBytes.AsSpan(0, 2), (short)port);
        var rentedPayload = new RentedArray<byte>(portBytes, 2);

        using var message = new Message(Message.Port, rentedPayload);
        using var rawData = message.ToRentedArray();

        await using var memoryStream = new FakeMemoryStream();
        await memoryStream.WriteAsync(rawData.Memory, cancellationToken);
        await memoryStream.FlushAsync(cancellationToken);

        await using var messageStream = new MessageStream(
            memoryStream,
            Handshake.Create(infohash, peerId.ToBytes()),
            120.Seconds
        );

        var streamTask = messageStream.StartAsync(cancellationToken);
        using var receivedMessage = await messageStream.IncomingMessages.ReadAsync(
            cancellationToken
        );
        receivedMessage.Payload.ShouldNotBeNull();
        receivedMessage.Id.ShouldBe(Message.Port);
        receivedMessage.Payload!.Length.ShouldBe(2);

        var receivedPort = BinaryPrimitives.ReadInt16BigEndian(receivedMessage.Payload.Memory.Span);
        ((int)receivedPort).ShouldBe(port);
    }
}
