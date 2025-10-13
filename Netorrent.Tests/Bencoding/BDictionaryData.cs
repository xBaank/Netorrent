using Netorrent.Bencoding.Structs;

namespace Netorrent.Tests.Bencoding;

public class BDictionaryData : TheoryData<string, BDictionary>
{
    public BDictionaryData()
    {
        Add("de", new BDictionary(new Dictionary<BString, IBencodingType>()));
        // Empty dictionary

        Add(
            "d3:cow3:mooe",
            new BDictionary(
                new Dictionary<BString, IBencodingType>()
                {
                    [new BString("cow")] = new BString("moo"),
                }
            )
        );
        // Single key-value

        Add(
            "d3:foo3:bare",
            new BDictionary(
                new Dictionary<BString, IBencodingType>()
                {
                    [new BString("foo")] = new BString("bar"),
                }
            )
        );
        // Simple pair

        Add(
            "d3:bar4:spam3:fooi42ee",
            new BDictionary(
                new Dictionary<BString, IBencodingType>()
                {
                    [new BString("bar")] = new BString("spam"),
                    [new BString("foo")] = new BInt(42),
                }
            )
        );
        // Multiple entries (keys sorted: "bar" < "foo")

        Add(
            "d4:spaml1:a1:bee",
            new BDictionary(
                new Dictionary<BString, IBencodingType>()
                {
                    [new BString("spam")] = new BList(
                        new List<IBencodingType>() { new BString("a"), new BString("b") }
                    ),
                }
            )
        );
        // Dictionary with list value

        Add(
            "d3:numli1ei2ei3ee4:name5:alicee",
            new BDictionary(
                new Dictionary<BString, IBencodingType>()
                {
                    [new BString("num")] = new BList(
                        new List<IBencodingType>() { new BInt(1), new BInt(2), new BInt(3) }
                    ),
                    [new BString("name")] = new BString("alice"),
                }
            )
        );
        // Mixed list and string values

        Add(
            "d4:datai123e4:text5:helloe",
            new BDictionary(
                new Dictionary<BString, IBencodingType>()
                {
                    [new BString("data")] = new BInt(123),
                    [new BString("text")] = new BString("hello"),
                }
            )
        );
        // Integer + string pair

        Add(
            "d4:metad3:fooi1ee4:testi2ee",
            new BDictionary(
                new Dictionary<BString, IBencodingType>()
                {
                    [new BString("meta")] = new BDictionary(
                        new Dictionary<BString, IBencodingType>()
                        {
                            [new BString("foo")] = new BInt(1),
                        }
                    ),
                    [new BString("test")] = new BInt(2),
                }
            )
        );
        // Nested dictionary

        Add(
            "d3:outd3:inn4:deepee",
            new BDictionary(
                new Dictionary<BString, IBencodingType>()
                {
                    [new BString("out")] = new BDictionary(
                        new Dictionary<BString, IBencodingType>()
                        {
                            [new BString("inn")] = new BString("deep"),
                        }
                    ),
                }
            )
        );
        // Deep nested dictionary

        Add(
            "d5:emptyle4:type4:liste",
            new BDictionary(
                new Dictionary<BString, IBencodingType>()
                {
                    [new BString("empty")] = new BList(new List<IBencodingType>()),
                    [new BString("type")] = new BString("list"),
                }
            )
        );
        // Contains empty list
    }
}
