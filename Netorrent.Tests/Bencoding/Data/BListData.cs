using Netorrent.Bencoding.Structs;

namespace Netorrent.Tests.Bencoding.Data;

public class BlistData
{
    public static IEnumerable<(string, BList)> GetTestData()
    {
        yield return ("le", new List<IBencodingNode>() { });

        yield return ("l4:spame", new List<IBencodingNode>() { new BString("spam") });

        yield return ("li42ee", new List<IBencodingNode>() { new BInt(42) });

        yield return (
            "l4:spam4:eggse",
            new List<IBencodingNode>() { new BString("spam"), new BString("eggs") }
        );

        yield return (
            "li1ei2ei3ee",
            new List<IBencodingNode>() { new BInt(1), new BInt(2), new BInt(3) }
        );

        yield return (
            "l4:spamli1ei2ee4:eggse",
            new List<IBencodingNode>()
            {
                new BString("spam"),
                new BList(new List<IBencodingNode>() { new BInt(1), new BInt(2) }),
                new BString("eggs"),
            }
        );

        yield return (
            "ll4:spamee",
            new List<IBencodingNode>() { new BList([new BString("spam")]) }
        );

        yield return (
            "l0:4:datae",
            new List<IBencodingNode>() { new BString(""), new BString("data") }
        );

        yield return (
            "li-42e7:negintle",
            new List<IBencodingNode>() { new BInt(-42), new BString("negintl") }
        );

        yield return (
            "l5:hello5:worldi123e3:abce",
            new List<IBencodingNode>()
            {
                new BString("hello"),
                new BString("world"),
                new BInt(123),
                new BString("abc"),
            }
        );

        yield return (
            "ll4:innee3:oute",
            new List<IBencodingNode>() { new BList([new BString("inne")]), new BString("out") }
        );

        yield return (
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
