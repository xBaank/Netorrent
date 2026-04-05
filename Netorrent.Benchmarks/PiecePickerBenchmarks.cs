using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using Netorrent.P2P.Download;
using Netorrent.P2P.Messages;

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

    // Pickers for each scenario — rebuilt in GlobalSetup per PieceCount value
    private PiecePicker _freshPicker = null!; // nothing requested yet
    private PiecePicker _steadyPicker = null!; // all blocks Requested, no pending
    private PiecePicker _endGamePicker = null!; // IsEndGame == true

    [GlobalSetup]
    public void Setup()
    {
        long totalSize = (long)PieceCount * PieceSize;
        var myBitfield = new Bitfield(PieceCount, isInitialized: false);
        var seederBitfield = new Bitfield(PieceCount, isInitialized: true);

        _peer = new FakePeerConnection(myBitfield, seederBitfield, BlockSize);
        _peer.PeerChoking.Value = false;
        _peer.AmInterested.Value = true;

        _freshPicker = MakePicker(PieceCount, PieceSize, totalSize, seed: 42);
        _steadyPicker = BuildSteadyPicker(PieceCount, PieceSize, totalSize);
        _endGamePicker = BuildEndGamePicker(PieceCount, PieceSize, totalSize);
    }

    // ── 1. New-piece path: exercises GetPiece + block initialisation ──────────

    /// <summary>
    /// Calls TryGetRequestBlock on a freshly constructed picker — all pieces
    /// unrequested. Each iteration creates a new picker so GetPiece is always
    /// called fresh; shows allocation cost and selection time per piece.
    /// </summary>
    [Benchmark]
    public bool TryGetRequestBlock_NewPiece()
    {
        long totalSize = (long)PieceCount * PieceSize;
        var picker = MakePicker(PieceCount, PieceSize, totalSize, seed: 1);
        return picker.TryGetRequestBlock(_peer, out _);
    }

    // ── 2. Pending-block guard: returning next pending block ──────────────────

    /// <summary>
    /// Handing out a pending block from an already-started piece.  The
    /// _pendingBlockCount guard keeps this O(1) independent of PieceCount.
    ///
    /// Uses the fresh picker that still has pending blocks after the first
    /// TryGetRequestBlock in GlobalSetup (called zero times on _freshPicker).
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("Steady")]
    public bool TryGetRequestBlock_PendingBlock()
    {
        // _freshPicker has PieceCount pieces all pending; we only measure the
        // time to hand out one block (Phase 1 short-circuits immediately).
        return _freshPicker.TryGetRequestBlock(_peer, out _);
    }

    // ── 3. No-pending guard: all blocks in-flight, not end-game ──────────────

    /// <summary>
    /// All blocks are in Requested state and no new pieces are available to the
    /// peer.  _pendingBlockCount == 0 and IsEndGame == false so Phase 1 is
    /// skipped entirely and GetPiece returns null — O(1) regardless of PieceCount.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("Steady")]
    public bool TryGetRequestBlock_NoPending()
    {
        return _steadyPicker.TryGetRequestBlock(_peer, out _);
    }

    // ── 4. End-game: O(1) detection + Phase 1 scan ───────────────────────────

    /// <summary>
    /// All pieces requested, IsEndGame == true.  End-game detection is now a
    /// single integer comparison (_unrequestedPieceCount == 0); the remaining
    /// cost is the Phase 1 SelectMany scan to find a re-requestable block.
    /// This benchmark intentionally shows how Phase 1 scales with PieceCount
    /// when end-game is active.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("EndGame")]
    public bool TryGetRequestBlock_EndGame()
    {
        return _endGamePicker.TryGetRequestBlock(_peer, out _);
    }

    // ── 5. CompletePiece + ConfirmPiece cycle ─────────────────────────────────

    /// <summary>
    /// Counter updates that replaced the O(n) finally-block scan.  Called once
    /// per downloaded piece so very infrequent — but this confirms constant-time
    /// cost regardless of PieceCount.
    /// </summary>
    [Benchmark]
    public void CompletePiece_ConfirmPiece()
    {
        long totalSize = (long)PieceCount * PieceSize;
        var myBitfield = new Bitfield(PieceCount, false);
        var picker = MakePicker(PieceCount, PieceSize, totalSize, seed: 3);

        // Simulate selecting, completing, and confirming piece 0
        picker.TryGetRequestBlock(_peer, out _);
        picker.CompletePiece(0);
        myBitfield.SetPiece(0);
        picker.ConfirmPiece(0);
    }

    // ── 6. Batch timeout reset ────────────────────────────────────────────────

    /// <summary>
    /// Resetting all blocks belonging to one slow peer back to Pending.  Used
    /// by the new CheckTimeout path in RequestScheduler — once per timed-out peer
    /// rather than once per timed-out block.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("Timeout")]
    public void ResetBlocksToPending()
    {
        // Rebuild each iteration so there are always blocks to reset
        var picker = BuildEndGamePicker(PieceCount, PieceSize, (long)PieceCount * PieceSize);
        picker.ResetBlocksToPending(_peer);
    }

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
    /// Builds a picker where every piece is in-progress with all blocks in
    /// <c>Requested</c> state (no pending blocks, not end-game from the picker's
    /// view, though the peer still has pieces available).
    /// </summary>
    private PiecePicker BuildSteadyPicker(int pieceCount, int pieceSize, long totalSize)
    {
        var picker = MakePicker(pieceCount, pieceSize, totalSize, seed: 11);
        while (picker.TryGetRequestBlock(_peer, out var block))
        {
            block.State = RequestBlockState.Requested;
            block.RequestedFrom.Add(_peer);
        }
        return picker;
    }

    /// <summary>
    /// Builds a picker with <c>IsEndGame == true</c>: all pieces in-progress,
    /// all blocks in <c>Requested</c> state.
    /// </summary>
    private PiecePicker BuildEndGamePicker(int pieceCount, int pieceSize, long totalSize) =>
        BuildSteadyPicker(pieceCount, pieceSize, totalSize);
}
