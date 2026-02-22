namespace Netorrent.Tracker.Udp.Request;

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using Netorrent.Extensions;
using Netorrent.Other;
using Netorrent.P2P.Messages;
using Netorrent.TorrentFile.FileStructure;

internal record UdpTrackerRequest(
    IPEndPoint IPEndPoint,
    InfoHash InfoHash,
    PeerId PeerId,
    long Downloaded,
    long Left,
    long Uploaded,
    string? Event,
    ushort Port,
    long ConnectionId,
    int TransactionId,
    int NumWant = -1,
    IPAddress? IpAddress = null,
    int Key = 0
) : IUdpTrackerSendPacket
{
    private const int SIZE = 98;

    public RentedArray<byte> ToMemoryRented()
    {
        var rentedArray = new RentedArray<byte>(SIZE);
        try
        {
            var memory = rentedArray.Memory;
            var span = memory.Span;

            int offset = 0;

            BinaryPrimitives.WriteInt64BigEndian(span[offset..], ConnectionId);
            offset += 8;

            BinaryPrimitives.WriteInt32BigEndian(span[offset..], 1);
            offset += 4;

            BinaryPrimitives.WriteInt32BigEndian(span[offset..], TransactionId);
            offset += 4;

            if (InfoHash.Data.Length != 20)
            {
                throw new ArgumentException("InfoHash must be 20 bytes", nameof(InfoHash));
            }

            InfoHash.Data.Span.CopyTo(span[offset..]);
            offset += 20;

            var peerBytes = PeerId.ToBytes();
            if (peerBytes.Length != 20)
            {
                throw new ArgumentException("PeerId must be 20 bytes", nameof(PeerId));
            }

            peerBytes.CopyTo(span[offset..]);
            offset += 20;

            BinaryPrimitives.WriteInt64BigEndian(span[offset..], Downloaded);
            offset += 8;

            BinaryPrimitives.WriteInt64BigEndian(span[offset..], Left);
            offset += 8;

            BinaryPrimitives.WriteInt64BigEndian(span[offset..], Uploaded);
            offset += 8;

            BinaryPrimitives.WriteInt32BigEndian(
                span[offset..],
                Event switch
                {
                    Events.Completed => 1,
                    Events.Started => 2,
                    Events.Stopped => 3,
                    _ => 0,
                }
            );
            offset += 4;

            if (
                IpAddress is not null
                && (
                    IpAddress.IsIPv4MappedToIPv6 == true
                    || IpAddress.AddressFamily == AddressFamily.InterNetwork
                )
            )
            {
                var ipv4Bytes = IpAddress.MapToIPv4().GetAddressBytes();
                ipv4Bytes.CopyTo(span[offset..]);
            }
            else
            {
                BinaryPrimitives.WriteInt32BigEndian(span[offset..], 0);
            }
            offset += 4;

            BinaryPrimitives.WriteInt32BigEndian(span[offset..], Key);
            offset += 4;

            BinaryPrimitives.WriteInt32BigEndian(span[offset..], NumWant);
            offset += 4;

            BinaryPrimitives.WriteUInt16BigEndian(span[offset..], Port);

            return rentedArray;
        }
        catch
        {
            rentedArray.Dispose();
            throw;
        }
    }
}
