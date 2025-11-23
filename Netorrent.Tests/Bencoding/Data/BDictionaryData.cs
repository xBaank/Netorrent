using Netorrent.Bencoding.Structs;

namespace Netorrent.Tests.Bencoding.Data;

public class BDictionaryData
{
    public static IEnumerable<(string, IBencodingNode)> GetTestData()
    {
        yield return ("de", new BDictionary([]));

        yield return (
            "d3:cow3:mooe",
            new BDictionary(
                new Dictionary<BString, IBencodingNode>()
                {
                    [new BString("cow")] = new BString("moo"),
                }
            )
        );

        yield return (
            "d3:foo3:bare",
            new BDictionary(
                new Dictionary<BString, IBencodingNode>()
                {
                    [new BString("foo")] = new BString("bar"),
                }
            )
        );

        yield return (
            "d3:bar4:spam3:fooi42ee",
            new BDictionary(
                new Dictionary<BString, IBencodingNode>()
                {
                    [new BString("bar")] = new BString("spam"),
                    [new BString("foo")] = new BInt(42),
                }
            )
        );

        yield return (
            "d4:spaml1:a1:bee",
            new BDictionary(
                new Dictionary<BString, IBencodingNode>()
                {
                    [new BString("spam")] = new BList([new BString("a"), new BString("b")]),
                }
            )
        );

        yield return (
            "d4:name5:alice3:numli1ei2ei3eee",
            new BDictionary(
                new Dictionary<BString, IBencodingNode>()
                {
                    [new BString("num")] = new BList([new BInt(1), new BInt(2), new BInt(3)]),
                    [new BString("name")] = new BString("alice"),
                }
            )
        );

        yield return (
            "d4:datai123e4:text5:helloe",
            new BDictionary(
                new Dictionary<BString, IBencodingNode>()
                {
                    [new BString("data")] = new BInt(123),
                    [new BString("text")] = new BString("hello"),
                }
            )
        );

        yield return (
            "d4:metad3:fooi1ee4:testi2ee",
            new BDictionary(
                new Dictionary<BString, IBencodingNode>()
                {
                    [new BString("meta")] = new BDictionary(
                        new Dictionary<BString, IBencodingNode>()
                        {
                            [new BString("foo")] = new BInt(1),
                        }
                    ),
                    [new BString("test")] = new BInt(2),
                }
            )
        );

        yield return (
            "d3:outd3:inn4:deepee",
            new BDictionary(
                new Dictionary<BString, IBencodingNode>()
                {
                    [new BString("out")] = new BDictionary(
                        new Dictionary<BString, IBencodingNode>()
                        {
                            [new BString("inn")] = new BString("deep"),
                        }
                    ),
                }
            )
        );

        yield return (
            "d5:emptyle4:type4:liste",
            new BDictionary(
                new Dictionary<BString, IBencodingNode>()
                {
                    [new BString("empty")] = new BList(new List<IBencodingNode>()),
                    [new BString("type")] = new BString("list"),
                }
            )
        );
    }
}
