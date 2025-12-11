using Netorrent.TorrentFile;
using Spectre.Console;

using var cts = new CancellationTokenSource();

Console.CancelKeyPress += (s, e) =>
{
    e.Cancel = true;
    cts.Cancel();
};
try
{
    // Prompt for .torrent file
    var torrentPath = await BrowseForTorrent(cts.Token);

    // Prompt for output directory
    var outputPath = await BrowseForOutputDir(cts.Token);

    // Initialize TorrentClient
    var client = new TorrentClient();

    await using var torrent = await client.ImportTorrentAsync(torrentPath, outputPath);

    // A task to keep refreshing status on screen
    var statusTask = RunStatusUI(torrent, cts.Token);

    // Start the torrent
    await torrent.StartAsync(cts.Token);

    // Wait for completion or cancellation
    var completed = await Task.WhenAny(
        torrent.Statistics.Completion.AsTask(),
        Task.Delay(Timeout.Infinite, cts.Token)
    );

    // If torrent finished normally
    if (completed == torrent.Statistics.Completion.AsTask())
    {
        AnsiConsole.MarkupLine("[green]Download complete.[/]");
    }
}
catch (OperationCanceledException)
{
    AnsiConsole.MarkupLine("[yellow]Canceled by user.[/]");
}

static async ValueTask<string> BrowseForOutputDir(CancellationToken cancellationToken)
{
    var current = Directory.GetCurrentDirectory();

    while (true)
    {
        string[] dirs;
        try
        {
            dirs = Directory.GetDirectories(current);
        }
        catch (Exception ex) when (ex is UnauthorizedAccessException || ex is IOException)
        {
            AnsiConsole.MarkupLine($"[red]Cannot enumerate directory: {ex.Message}[/]");
            var parent = Directory.GetParent(current);
            if (parent == null)
            {
                // If we cannot read the start folder and there's no parent, fall back to current directory.
                current = Directory.GetCurrentDirectory();
            }
            else
            {
                current = parent.FullName;
            }
            continue;
        }

        List<string> items =
        [
            ".. (up)",
            .. dirs.Select(d =>
            {
                var name = Path.GetFileName(d);
                if (string.IsNullOrEmpty(name)) // root drive (e.g. "C:\")
                    name = d;
                return $"(dir) {name}";
            }),
            "(select) Use this directory",
            "(new) Create new directory here",
        ];

        var choice = await AnsiConsole.PromptAsync(
            new SelectionPrompt<string>()
                .Title($"Browsing output directories: [green]{current}[/]")
                .PageSize(20)
                .AddChoices(items),
            cancellationToken
        );

        if (choice == ".. (up)")
        {
            var parent = Directory.GetParent(current);
            if (parent == null)
            {
                // already at root, ignore
                continue;
            }
            current = parent.FullName;
            continue;
        }

        if (choice.StartsWith("(dir) "))
        {
            var name = choice.Substring("(dir) ".Length);
            // If name is a full path (happens for root), prefer that
            string candidate =
                dirs.FirstOrDefault(d =>
                    Path.GetFileName(d).Equals(name, StringComparison.OrdinalIgnoreCase)
                    || string.Equals(d, name, StringComparison.OrdinalIgnoreCase)
                ) ?? Path.Combine(current, name);
            current = Path.GetFullPath(candidate);
            continue;
        }

        if (choice == "(select) Use this directory")
        {
            return current;
        }

        if (choice == "(new) Create new directory here")
        {
            var newName = await AnsiConsole.PromptAsync(
                new TextPrompt<string>("Enter new directory name:").Validate(n =>
                {
                    if (string.IsNullOrWhiteSpace(n))
                        return ValidationResult.Error("[red]Name cannot be empty[/]");
                    if (n.IndexOfAny(Path.GetInvalidFileNameChars()) >= 0)
                        return ValidationResult.Error("[red]Name contains invalid characters[/]");
                    return ValidationResult.Success();
                }),
                cancellationToken
            );

            var newPath = Path.Combine(current, newName);

            try
            {
                Directory.CreateDirectory(newPath);
                current = Path.GetFullPath(newPath);
            }
            catch (Exception ex)
                when (ex is UnauthorizedAccessException
                    || ex is IOException
                    || ex is ArgumentException
                )
            {
                AnsiConsole.MarkupLine($"[red]Could not create directory: {ex.Message}[/]");
            }

            continue;
        }
    }
}

static async ValueTask<string> BrowseForTorrent(CancellationToken cancellationToken)
{
    var current = Directory.GetCurrentDirectory();

    while (true)
    {
        var dirs = Directory
            .GetDirectories(current)
            .Select(d => ("D", Path.GetFileName(d), d))
            .ToList();

        var files = Directory
            .GetFiles(current, "*.torrent")
            .Select(f => ("F", Path.GetFileName(f), f))
            .ToList();

        List<string> items =
        [
            ".. (up)",
            .. dirs.Select(t => $"(dir) {t.Item2}"),
            .. files.Select(t => $"(file) {t.Item2}"),
        ];

        var choice = await AnsiConsole.PromptAsync(
            new SelectionPrompt<string>()
                .Title($"Browsing: [green]{current}[/]")
                .PageSize(20)
                .AddChoices(items),
            cancellationToken
        );

        if (choice == ".. (up)")
        {
            var parent = Directory.GetParent(current);
            if (parent == null)
                continue;
            current = parent.FullName;
            continue;
        }

        if (choice.StartsWith("(dir) "))
        {
            var name = choice.Substring("(dir) ".Length);
            current = Path.Combine(current, name);
            continue;
        }

        if (choice.StartsWith("(file) "))
        {
            var name = choice.Substring("(file) ".Length);
            return Path.Combine(current, name);
        }
    }
}

static async Task RunStatusUI(Torrent torrent, CancellationToken token) =>
    await AnsiConsole
        .Progress()
        .AutoClear(false)
        .Columns(
            new TaskDescriptionColumn(),
            new ProgressBarColumn(),
            new PercentageColumn(),
            new RemainingTimeColumn(),
            new SpinnerColumn(),
            new TransferSpeedColumn(),
            new DownloadedColumn()
        )
        .StartAsync(async ctx =>
        {
            var progressTask = ctx.AddTask("[green]Torrent Download[/]", autoStart: true);

            // Initialize
            var totalBytes = torrent.Statistics.Transfer.TotalBytes.Bytes;
            progressTask.MaxValue(totalBytes);

            while (!token.IsCancellationRequested)
            {
                var t = torrent.Statistics.Transfer;
                var p = torrent.Statistics.Peers;

                progressTask.Value(t.DownloadedBytes.Bytes);

                progressTask.Description =
                    $@"[green]{torrent.MetaInfo.Title ?? torrent.MetaInfo.Info.Name}[/]";

                await Task.Delay(1000, token);
            }

            progressTask.StopTask();
        });
