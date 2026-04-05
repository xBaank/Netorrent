namespace Netorrent.Dht.Routing;

internal sealed class KBucket(int k = 8)
{
    // Ordered LRS-first, MRS-last
    private readonly List<DhtNode> _nodes = new(k);
    private readonly Queue<DhtNode> _replacement = new();

    public IReadOnlyList<DhtNode> Nodes => _nodes;

    /// <summary>
    /// Tries to add a node.
    /// Returns the least-recently-seen node if the bucket is full (caller must ping it).
    /// Returns null if the node was added or updated without eviction needed.
    /// </summary>
    public DhtNode? TryAdd(DhtNode node)
    {
        // Already known: move to MRS position (tail)
        for (int i = 0; i < _nodes.Count; i++)
        {
            if (_nodes[i].Id == node.Id)
            {
                _nodes[i].LastSeen = DateTime.UtcNow;
                var existing = _nodes[i];
                _nodes.RemoveAt(i);
                _nodes.Add(existing);
                return null;
            }
        }

        if (_nodes.Count < k)
        {
            _nodes.Add(node);
            return null;
        }

        // Bucket full: queue the new node for possible replacement and return LRS for ping
        _replacement.Enqueue(node);
        return _nodes[0];
    }

    /// <summary>
    /// Called after the LRS node responds to a ping: refresh it and discard one replacement.
    /// </summary>
    public void ConfirmAlive(NodeId id)
    {
        for (int i = 0; i < _nodes.Count; i++)
        {
            if (_nodes[i].Id == id)
            {
                _nodes[i].LastSeen = DateTime.UtcNow;
                var node = _nodes[i];
                _nodes.RemoveAt(i);
                _nodes.Add(node);
                // Discard the rejected replacement candidate
                _replacement.TryDequeue(out _);
                return;
            }
        }
    }

    /// <summary>
    /// Called after the LRS node fails to respond: remove it and promote one replacement.
    /// </summary>
    public void Evict(NodeId id)
    {
        for (int i = 0; i < _nodes.Count; i++)
        {
            if (_nodes[i].Id == id)
            {
                _nodes.RemoveAt(i);
                if (_replacement.TryDequeue(out var replacement))
                    _nodes.Add(replacement);
                return;
            }
        }
    }
}
