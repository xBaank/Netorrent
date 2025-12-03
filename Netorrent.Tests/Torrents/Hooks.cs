namespace Netorrent.Tests.Torrents;

public static class Hooks
{
    [Before(TestSession)]
    public static Task Setup(CancellationToken _)
    {
        CleanDirectories();
        return Task.CompletedTask;
    }

    [After(TestSession)]
    public static Task Dispose(CancellationToken _)
    {
        CleanDirectories();
        return Task.CompletedTask;
    }

    private static void CleanDirectories()
    {
        if (Directory.Exists("Output"))
            Directory.Delete("Output", true);
        if (Directory.Exists("Input"))
            Directory.Delete("Input", true);
    }
}
