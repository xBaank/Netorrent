using Netorrent.Bencoding.Structs;

namespace Netorrent.Tests.Bencoding.Data;

public class BlistData : TheoryData<string, BList>
{
    public BlistData()
    {
        Add("le", new List<IBencodingNode>() { });

        Add("l4:spame", new List<IBencodingNode>() { new BString("spam") });

        Add("li42ee", new List<IBencodingNode>() { new BInt(42) });

        Add(
            "l4:spam4:eggse",
            new List<IBencodingNode>() { new BString("spam"), new BString("eggs") }
        );

        Add("li1ei2ei3ee", new List<IBencodingNode>() { new BInt(1), new BInt(2), new BInt(3) });

        Add(
            "l4:spamli1ei2ee4:eggse",
            new List<IBencodingNode>()
            {
                new BString("spam"),
                new BList(new List<IBencodingNode>() { new BInt(1), new BInt(2) }),
                new BString("eggs"),
            }
        );

        Add("ll4:spamee", new List<IBencodingNode>() { new BList([new BString("spam")]) });

        Add("l0:4:datae", new List<IBencodingNode>() { new BString(""), new BString("data") });

        Add(
            "li-42e7:negintle",
            new List<IBencodingNode>() { new BInt(-42), new BString("negintl") }
        );

        Add(
            "l5:hello5:worldi123e3:abce",
            new List<IBencodingNode>()
            {
                new BString("hello"),
                new BString("world"),
                new BInt(123),
                new BString("abc"),
            }
        );

        Add(
            "ll4:innee3:oute",
            new List<IBencodingNode>() { new BList([new BString("inne")]), new BString("out") }
        );

        Add(
            "ld3:key5:valuee4:testi123ee",
            new List<IBencodingNode>()
            {
                new BDictionary(
                    new Dictionary<BString, IBencodingNode>()
                    {
                        [new BString("key")] = new BString("value"),
                    }
                ),
                new BString("test"),
                new BInt(123),
            }
        );
    }
}
