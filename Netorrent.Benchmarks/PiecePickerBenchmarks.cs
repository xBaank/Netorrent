using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using Netorrent.P2P.Download;
using Netorrent.P2P.Messages;

namespace Netorrent.Benchmarks;

/// <summary>
/// Benchmarks for <see cref="PiecePicker"/> covering the key hot paths:
///
///   1. TryGetRequestBlock — steady state (new piece), no-pending guard, end-game
///   2. CompletePiece + ConfirmPiece — counter-update overhead per downloaded piece
///   3. ResetBlocksToPending — batch timeout reset per slow peer
///
/// Run with:
///   dotnet run --project Netorrent.Benchmarks -c Release -- --filter '*'
///
/// Single category:
///   dotnet run --project Netorrent.Benchmarks -c Release -- --filter '*EndGame*'
/// </summary>
[MemoryDiagnoser]
[SimpleJob(RuntimeMoniker.Net10_0)]
public class PiecePickerBenchmarks
{
    private const int BlockSize = 16 * 1024; // 16 KB standard block
    private const int PieceSize = BlockSize * 4; // 64 KB pieces

    [Params(100, 1_000, 10_000)]
    public int PieceCount { get; set; }

    private FakePeerConnection _peer = null!;

    // Peer with only the first half of pieces — used for the NoPending scenario
    // so _steadyPicker has _isEndGame == false after setup.
    private FakePeerConnection _halfPeer = null!;

    // Pickers for each scenario — rebuilt in GlobalSetup per PieceCount value
    private PiecePicker _freshPicker = null!; // nothing requested yet
    private PiecePicker _steadyPicker = null!; // first half of pieces requested via _halfPeer, no pending, not end-game
    private PiecePicker _endGamePicker = null!; // IsEndGame == true (all pieces requested)

    // Pickers rebuilt per-iteration via IterationSetup (construction cost excluded from timing)
    private PiecePicker _newPiecePicker = null!;
    private PiecePicker _completePiecePicker = null!;
    private PiecePicker _resetPicker = null!;

    [GlobalSetup]
    public void Setup()
    {
        long totalSize = (long)PieceCount * PieceSize;
        var myBitfield = new Bitfield(PieceCount, isInitialized: false);
        var seederBitfield = new Bitfield(PieceCount, isInitialized: true);

        _peer = new FakePeerConnection(myBitfield, seederBitfield, BlockSize);
        _peer.PeerChoking.Value = false;
        _peer.AmInterested.Value = true;

        // Half-peer has only pieces [0 .. PieceCount/2). Used to build a
        // steady-state picker that is NOT in end-game (_unrequestedPieceCount > 0).
        var halfBitfield = new Bitfield(PieceCount, isInitialized: false);
        for (int i = 0; i < PieceCount / 2; i++)
            halfBitfield.SetPiece(i);
        _halfPeer = new FakePeerConnection(myBitfield, halfBitfield, BlockSize);
        _halfPeer.PeerChoking.Value = false;
        _halfPeer.AmInterested.Value = true;

        _freshPicker = MakePicker(PieceCount, PieceSize, totalSize, seed: 42);
        _steadyPicker = BuildSteadyPicker(PieceCount, PieceSize, totalSize, _halfPeer);
        _endGamePicker = BuildSteadyPicker(PieceCount, PieceSize, totalSize, _peer);
    }

    // ── 1. New-piece path: exercises GetPiece + block initialisation ──────────

    [IterationSetup(Target = nameof(TryGetRequestBlock_NewPiece))]
    public void SetupNewPiece()
    {
        _newPiecePicker = MakePicker(PieceCount, PieceSize, (long)PieceCount * PieceSize, seed: 1);
    }

    /// <summary>
    /// Calls TryGetRequestBlock on a freshly constructed picker — all pieces
    /// unrequested. Picker construction is excluded via IterationSetup; only
    /// GetPiece + block initialisation is timed (O(n) in PieceCount).
    /// </summary>
    [Benchmark]
    public bool TryGetRequestBlock_NewPiece() => _newPiecePicker.TryGetRequestBlock(_peer, out _);

    // ── 2. Pending-block guard: returning next pending block ──────────────────

    /// <summary>
    /// Handing out a pending block from an already-started piece.
    /// _pendingBlockCount > 0 short-circuits to Phase 1 immediately, making
    /// this O(1) independent of PieceCount.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("Steady")]
    public bool TryGetRequestBlock_PendingBlock() => _freshPicker.TryGetRequestBlock(_peer, out _);

    // ── 3. No-pending, not end-game: GetPiece scan returns null ──────────────

    /// <summary>
    /// All blocks for the first half of pieces are Requested; the peer only has
    /// those pieces. _pendingBlockCount == 0 and IsEndGame == false, so Phase 1
    /// is skipped. GetPiece is called but finds no eligible piece for this peer
    /// (all available pieces are already in _requestedIndexes) and returns null.
    /// Cost is O(n) — GetPiece always scans all pieces in the peer bitfield.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("Steady")]
    public bool TryGetRequestBlock_NoPending() =>
        _steadyPicker.TryGetRequestBlock(_halfPeer, out _);

    // ── 4. End-game: O(1) detection + Phase 1 scan ───────────────────────────

    /// <summary>
    /// All pieces requested, IsEndGame == true.  End-game detection is a single
    /// integer comparison (_unrequestedPieceCount == 0); the remaining cost is
    /// the Phase 1 scan to find a re-requestable block (O(n) in block count).
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("EndGame")]
    public bool TryGetRequestBlock_EndGame() => _endGamePicker.TryGetRequestBlock(_peer, out _);

    // ── 5. CompletePiece + ConfirmPiece cycle ─────────────────────────────────

    [IterationSetup(Target = nameof(CompletePiece_ConfirmPiece))]
    public void SetupCompletePiece()
    {
        _completePiecePicker = MakePicker(
            PieceCount,
            PieceSize,
            (long)PieceCount * PieceSize,
            seed: 3
        );
        _completePiecePicker.TryGetRequestBlock(_peer, out _);
    }

    /// <summary>
    /// Counter updates called once per downloaded piece. Picker construction and
    /// initial block selection are excluded via IterationSetup; only CompletePiece
    /// + ConfirmPiece are timed.
    /// </summary>
    [Benchmark]
    public void CompletePiece_ConfirmPiece()
    {
        _completePiecePicker.CompletePiece(0);
        _completePiecePicker.ConfirmPiece(0);
    }

    // ── 6. Batch timeout reset ────────────────────────────────────────────────

    [IterationSetup(Target = nameof(ResetBlocksToPending))]
    public void SetupResetBlocksToPending()
    {
        _resetPicker = BuildSteadyPicker(
            PieceCount,
            PieceSize,
            (long)PieceCount * PieceSize,
            _peer
        );
    }

    /// <summary>
    /// Resetting all blocks belonging to one slow peer back to Pending.  Used
    /// by the CheckTimeout path in RequestScheduler — once per timed-out peer
    /// rather than once per timed-out block. O(n) in block count, zero alloc.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("Timeout")]
    public void ResetBlocksToPending() => _resetPicker.ResetBlocksToPending(_peer);

    // ── helpers ───────────────────────────────────────────────────────────────

    private static PiecePicker MakePicker(int pieceCount, int pieceSize, long totalSize, int seed)
    {
        var picker = new PiecePicker(
            new Bitfield(pieceCount, false),
            BlockSize,
            pieceSize,
            totalSize
        );
        var rng = new Random(seed);
        for (int i = 0; i < pieceCount; i++)
        {
            int rarity = rng.Next(1, 20);
            for (int r = 0; r < rarity; r++)
                picker.IncreaseRarity(i);
        }
        return picker;
    }

    /// <summary>
    /// Builds a picker where every piece available to <paramref name="peer"/> is
    /// in-progress with all blocks in <c>Requested</c> state and no pending blocks.
    /// IsEndGame is true iff <paramref name="peer"/> has all pieces.
    /// </summary>
    private static PiecePicker BuildSteadyPicker(
        int pieceCount,
        int pieceSize,
        long totalSize,
        FakePeerConnection peer
    )
    {
        var picker = MakePicker(pieceCount, pieceSize, totalSize, seed: 11);
        while (picker.TryGetRequestBlock(peer, out var block))
        {
            block.State = RequestBlockState.Requested;
            block.RequestedFrom.Add(peer);
        }
        return picker;
    }
}
