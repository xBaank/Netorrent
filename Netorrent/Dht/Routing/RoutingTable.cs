namespace Netorrent.Dht.Routing;

internal sealed class RoutingTable(NodeId selfId, int k = 8)
{
    private readonly KBucket[] _buckets = Enumerable
        .Range(0, 160)
        .Select(_ => new KBucket(k))
        .ToArray();

    public int Count => _buckets.Sum(b => b.Nodes.Count);

    /// <summary>
    /// Tries to insert a node. Returns the LRS eviction candidate if the target bucket is full.
    /// The caller is responsible for pinging the candidate and calling ConfirmAlive or Evict.
    /// </summary>
    public DhtNode? TryInsert(DhtNode node)
    {
        if (node.Id == selfId)
            return null;

        var bucketIndex = selfId.BucketIndex(node.Id);
        if (bucketIndex < 0)
            return null;

        return _buckets[bucketIndex].TryAdd(node);
    }

    public KBucket GetBucketFor(NodeId id)
    {
        var bucketIndex = selfId.BucketIndex(id);
        if (bucketIndex < 0)
            return _buckets[0];
        return _buckets[bucketIndex];
    }

    /// <summary>
    /// Returns up to <paramref name="count"/> nodes closest to <paramref name="target"/> by XOR distance.
    /// </summary>
    public IReadOnlyList<DhtNode> GetClosest(NodeId target, int count = 8)
    {
        var candidates = new List<(NodeId Distance, DhtNode Node)>();
        foreach (var bucket in _buckets)
        {
            foreach (var node in bucket.Nodes)
                candidates.Add((node.Id.Xor(target), node));
        }
        candidates.Sort(static (a, b) => a.Distance.CompareTo(b.Distance));
        return candidates.Take(count).Select(static c => c.Node).ToList();
    }

    public void RefreshNode(NodeId id)
    {
        var bucketIndex = selfId.BucketIndex(id);
        if (bucketIndex < 0)
            return;
        var bucket = _buckets[bucketIndex];
        foreach (var node in bucket.Nodes)
        {
            if (node.Id == id)
            {
                node.LastSeen = DateTime.UtcNow;
                return;
            }
        }
    }

    public void Remove(NodeId id)
    {
        var bucketIndex = selfId.BucketIndex(id);
        if (bucketIndex < 0)
            return;
        _buckets[bucketIndex].Evict(id);
    }

    /// <summary>
    /// Returns all buckets that haven't been refreshed recently (no activity in 15 minutes).
    /// </summary>
    public IEnumerable<(int BucketIndex, NodeId RandomTarget)> GetStaleRefreshTargets()
    {
        for (int i = 0; i < 160; i++)
        {
            var bucket = _buckets[i];
            if (bucket.Nodes.Count == 0)
                continue;
            if (bucket.Nodes.All(n => DateTime.UtcNow - n.LastSeen > TimeSpan.FromMinutes(15)))
            {
                // Generate a random ID that falls into this bucket's range
                var randomTarget = NodeId.Generate();
                yield return (i, randomTarget);
            }
        }
    }
}
