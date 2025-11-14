using Netorrent.Bencoding.Structs;

namespace Netorrent.Tests.Bencoding.Data;

public class BDictionaryData : TheoryData<string, BDictionary>
{
    public BDictionaryData()
    {
        Add("de", new BDictionary([]));

        Add(
            "d3:cow3:mooe",
            new BDictionary(
                new Dictionary<BString, IBencodingNode>()
                {
                    [new BString("cow")] = new BString("moo"),
                }
            )
        );

        Add(
            "d3:foo3:bare",
            new BDictionary(
                new Dictionary<BString, IBencodingNode>()
                {
                    [new BString("foo")] = new BString("bar"),
                }
            )
        );

        Add(
            "d3:bar4:spam3:fooi42ee",
            new BDictionary(
                new Dictionary<BString, IBencodingNode>()
                {
                    [new BString("bar")] = new BString("spam"),
                    [new BString("foo")] = new BInt(42),
                }
            )
        );

        Add(
            "d4:spaml1:a1:bee",
            new BDictionary(
                new Dictionary<BString, IBencodingNode>()
                {
                    [new BString("spam")] = new BList([new BString("a"), new BString("b")]),
                }
            )
        );

        Add(
            "d4:name5:alice3:numli1ei2ei3eee",
            new BDictionary(
                new Dictionary<BString, IBencodingNode>()
                {
                    [new BString("num")] = new BList([new BInt(1), new BInt(2), new BInt(3)]),
                    [new BString("name")] = new BString("alice"),
                }
            )
        );

        Add(
            "d4:datai123e4:text5:helloe",
            new BDictionary(
                new Dictionary<BString, IBencodingNode>()
                {
                    [new BString("data")] = new BInt(123),
                    [new BString("text")] = new BString("hello"),
                }
            )
        );

        Add(
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

        Add(
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

        Add(
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
