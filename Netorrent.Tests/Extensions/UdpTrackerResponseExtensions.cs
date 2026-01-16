using System.Buffers.Binary;
using System.Net.Sockets;
using System.Text;
using Netorrent.Tracker.Udp.Response;

namespace Netorrent.Tests.Extensions;

internal static class UdpTrackerExtensions
{
    extension(UdpTrackerConnectResponse response)
    {
        public byte[] ToBytes()
        {
            var buffer = new byte[16];
            var span = buffer.AsSpan();

            // action
            BinaryPrimitives.WriteInt32BigEndian(
                span.Slice(0, 4),
                UdpTrackerConnectResponse.Action
            );

            // transaction id
            BinaryPrimitives.WriteInt32BigEndian(span.Slice(4, 4), response.TransactionId);

            // connection id
            BinaryPrimitives.WriteInt64BigEndian(span.Slice(8, 8), response.ConnectionId);

            return buffer;
        }
    }

    extension(UdpTrackerErrorResponse response)
    {
        public byte[] ToBytes()
        {
            var messageBytes = Encoding.UTF8.GetBytes(response.Message);

            var buffer = new byte[8 + messageBytes.Length];
            var span = buffer.AsSpan();

            // action
            BinaryPrimitives.WriteInt32BigEndian(span.Slice(0, 4), UdpTrackerErrorResponse.Action);

            // transaction id
            BinaryPrimitives.WriteInt32BigEndian(span.Slice(4, 4), response.TransactionId);

            // message (UTF-8, no terminator)
            messageBytes.CopyTo(span.Slice(8));

            return buffer;
        }
    }

    extension(UdpTrackerResponse response)
    {
        public byte[] ToBytes(AddressFamily addressFamily)
        {
            int peerSize = addressFamily switch
            {
                AddressFamily.InterNetwork => 6,
                AddressFamily.InterNetworkV6 => 18,
                _ => throw new NotSupportedException(
                    $"AddressFamily {addressFamily} is not supported"
                ),
            };

            var buffer = new byte[20 + response.Peers.Count * peerSize];
            var span = buffer.AsSpan();

            // Header (20 bytes)
            BinaryPrimitives.WriteInt32BigEndian(span.Slice(0, 4), UdpTrackerResponse.Action);
            BinaryPrimitives.WriteInt32BigEndian(span.Slice(4, 4), response.TransactionId);
            BinaryPrimitives.WriteInt32BigEndian(span.Slice(8, 4), response.Interval);
            BinaryPrimitives.WriteInt32BigEndian(span.Slice(12, 4), response.Leechers);
            BinaryPrimitives.WriteInt32BigEndian(span.Slice(16, 4), response.Seeders);

            var offset = 20;

            foreach (var peer in response.Peers)
            {
                byte[] ipBytes;
                if (addressFamily == AddressFamily.InterNetwork)
                {
                    // Target is IPv4: ensure we write 4 bytes.
                    // If peer has an IPv6 mapped IPv4, map it to IPv4.
                    ipBytes = peer.Address.AddressFamily switch
                    {
                        AddressFamily.InterNetwork => peer.Address.GetAddressBytes(),
                        AddressFamily.InterNetworkV6 when peer.Address.IsIPv4MappedToIPv6 => peer
                            .Address.MapToIPv4()
                            .GetAddressBytes(),
                        _ => throw new ArgumentException(
                            $"Peer {peer} cannot be represented as IPv4 in this response"
                        ),
                    };

                    if (ipBytes.Length != 4)
                    {
                        throw new InvalidOperationException("IPv4 address bytes length must be 4");
                    }

                    // Write ip (4 bytes)
                    span.Slice(offset, 4).CopyTo(ipBytes); // <-- wrong direction (we'll fix below)
                }
                else
                {
                    ipBytes = peer.Address.AddressFamily switch
                    {
                        AddressFamily.InterNetworkV6 => peer.Address.GetAddressBytes(),
                        AddressFamily.InterNetwork => peer.Address.MapToIPv6().GetAddressBytes(),
                        _ => throw new ArgumentException(
                            $"Peer {peer} cannot be represented as IPv6 in this response"
                        ),
                    };

                    if (ipBytes.Length != 16)
                    {
                        throw new InvalidOperationException("IPv6 address bytes length must be 16");
                    }
                }

                // Correct copy: source -> destination
                ipBytes.CopyTo(span.Slice(offset, ipBytes.Length));
                offset += ipBytes.Length;

                // Write port (2 bytes BE)
                BinaryPrimitives.WriteUInt16BigEndian(span.Slice(offset, 2), (ushort)peer.Port);
                offset += 2;
            }

            if (offset != buffer.Length)
            {
                throw new InvalidOperationException("Serialized length mismatch");
            }

            return buffer;
        }
    }
}
