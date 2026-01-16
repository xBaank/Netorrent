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
    var torrentPath = await BrowseForTorrent(cts.Token);
    var outputPath = await BrowseForOutputDir(cts.Token);

    await using var client = new TorrentClient();
    await using var torrent = await client.LoadTorrentAsync(
        torrentPath,
        outputPath,
        cancellationToken: cts.Token
    );
    cts.Token.Register(torrent.Stop);
    var statusTask = RunStatusUI(torrent, cts.Token);

    await torrent.CheckAsync(cts.Token);
    await torrent.StartAsync();

    await Task.WhenAll(Task.Delay(-1, cts.Token), torrent.Completion.AsTask());

    AnsiConsole.MarkupLine("[green]Download complete.[/]");
}
catch (OperationCanceledException oce) when (oce.CancellationToken == cts.Token)
{
    AnsiConsole.MarkupLine("[yellow]Canceled by user.[/]");
}
catch (Exception ex)
{
    AnsiConsole.MarkupLine($"[yellow]Error {ex.Message.EscapeMarkup()}[/]");
    AnsiConsole.MarkupLine($"[yellow]Stacktrace {ex.StackTrace.EscapeMarkup()}[/]");
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
            AnsiConsole.MarkupLine(
                $"[red]Cannot enumerate directory: {ex.Message.EscapeMarkup()}[/]"
            );
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
                {
                    name = d;
                }
                return $"(dir) {name}";
            }),
            "(select) Use this directory",
            "(new) Create new directory here",
        ];

        var choice = await AnsiConsole.PromptAsync(
            new SelectionPrompt<string>()
                .Title($"Browsing output directories: [green]{current.EscapeMarkup()}[/]")
                .PageSize(20)
                .AddChoices(items.Select(StringExtensions.EscapeMarkup)),
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
            var name = choice.RemoveMarkup().Substring("(dir) ".Length);
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
                    {
                        return ValidationResult.Error("[red]Name cannot be empty[/]");
                    }

                    if (n.IndexOfAny(Path.GetInvalidFileNameChars()) >= 0)
                    {
                        return ValidationResult.Error("[red]Name contains invalid characters[/]");
                    }

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
                AnsiConsole.MarkupLine(
                    $"[red]Could not create directory: {ex.Message.EscapeMarkup()}[/]"
                );
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
                .Title($"Browsing: [green]{current.EscapeMarkup()}[/]")
                .PageSize(20)
                .AddChoices(items.Select(StringExtensions.EscapeMarkup)),
            cancellationToken
        );

        if (choice == ".. (up)")
        {
            var parent = Directory.GetParent(current);
            if (parent == null)
            {
                continue;
            }

            current = parent.FullName;
            continue;
        }

        if (choice.StartsWith("(dir) "))
        {
            var name = choice.RemoveMarkup().Substring("(dir) ".Length);
            current = Path.Combine(current, name);
            continue;
        }

        if (choice.StartsWith("(file) "))
        {
            var name = choice.RemoveMarkup().Substring("(file) ".Length);
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
            new ProgressBarColumn
            {
                CompletedStyle = new Style(Color.Green),
                FinishedStyle = new Style(Color.Lime),
                RemainingStyle = new Style(Color.Grey),
            },
            new PercentageColumn(),
            new RemainingTimeColumn(),
            new SpinnerColumn()
        )
        .StartAsync(async ctx =>
        {
            var checkTask = ctx.AddTask("Checking", autoStart: true);
            var progressTask = ctx.AddTask("Downloading", autoStart: true);
            var uploadTask = ctx.AddTask("Uploading", autoStart: true);

            // Initialize
            checkTask.MaxValue(torrent.Statistics.Check.TotalPiecesCount);
            progressTask.MaxValue(torrent.Statistics.Data.Total.Bytes);
            uploadTask.IsIndeterminate(true);
            progressTask.IsIndeterminate(true);

            while (!token.IsCancellationRequested)
            {
                if (checkTask.IsFinished)
                {
                    progressTask.IsIndeterminate(false);
                }

                checkTask.Value(torrent.Statistics.Check.CheckedPiecesCount);
                progressTask.Value(torrent.Statistics.Data.Verified.Bytes);
                uploadTask.Value(torrent.Statistics.Data.Uploaded.Bytes);
                var desc = progressTask.Description = (
                    torrent.MetaInfo.Title ?? torrent.MetaInfo.Info.Name
                ).EscapeMarkup();

                checkTask.Description = $@"Checking {desc}";
                progressTask.Description = $@"Downloading {desc}";
                uploadTask.Description = $@"Uploading {desc}";

                await Task.Delay(1000, token);
            }

            progressTask.StopTask();
        });
