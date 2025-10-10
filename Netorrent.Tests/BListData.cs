using Netorrent.Bencoding.Structs;

namespace Netorrent.Tests
{
    public class BlistData : TheoryData<string, BList>
    {
        public BlistData()
        {
            Add("le", new List<IBencodingType>() { });
            // Empty list

            Add("l4:spame", new List<IBencodingType>() { new BString("spam") });
            // Single string

            Add("li42ee", new List<IBencodingType>() { new BInt(42) });
            // Single integer

            Add(
                "l4:spam4:eggse",
                new List<IBencodingType>() { new BString("spam"), new BString("eggs") }
            );
            // Two strings

            Add(
                "li1ei2ei3ee",
                new List<IBencodingType>() { new BInt(1), new BInt(2), new BInt(3) }
            );
            // Multiple integers

            Add(
                "l4:spamli1ei2ee4:eggse",
                new List<IBencodingType>()
                {
                    new BString("spam"),
                    new BList(new List<IBencodingType>() { new BInt(1), new BInt(2) }),
                    new BString("eggs"),
                }
            );
            // Nested list

            Add(
                "ll4:spamee",
                new List<IBencodingType>()
                {
                    new BList(new List<IBencodingType>() { new BString("spam") }),
                }
            );
            // List inside list

            Add("l0:4:datae", new List<IBencodingType>() { new BString(""), new BString("data") });
            // Includes empty string

            Add(
                "li-42e7:negintle",
                new List<IBencodingType>() { new BInt(-42), new BString("negintl") }
            );
            // Mix of integer and string

            Add(
                "l5:hello5:worldi123e3:abce",
                new List<IBencodingType>()
                {
                    new BString("hello"),
                    new BString("world"),
                    new BInt(123),
                    new BString("abc"),
                }
            );
            // Mixed types

            Add(
                "ll4:innee3:oute",
                new List<IBencodingType>()
                {
                    new BList(new List<IBencodingType>() { new BString("inne") }),
                    new BString("out"),
                }
            );
            // Deeply nested
        }
    }
}
