using TUnit.Core.Logging;

namespace Netorrent.Tests.Integration;

internal class TUnitLogger(DefaultLogger logger) : Microsoft.Extensions.Logging.ILogger
{
    public IDisposable? BeginScope<TState>(TState state)
        where TState : notnull
    {
        return null;
    }

    public bool IsEnabled(Microsoft.Extensions.Logging.LogLevel logLevel) =>
        logger.IsEnabled(
            logLevel switch
            {
                Microsoft.Extensions.Logging.LogLevel.Trace => LogLevel.Trace,
                Microsoft.Extensions.Logging.LogLevel.Debug => LogLevel.Debug,
                Microsoft.Extensions.Logging.LogLevel.Information => LogLevel.Information,
                Microsoft.Extensions.Logging.LogLevel.Warning => LogLevel.Warning,
                Microsoft.Extensions.Logging.LogLevel.Error => LogLevel.Error,
                Microsoft.Extensions.Logging.LogLevel.Critical => LogLevel.Critical,
                Microsoft.Extensions.Logging.LogLevel.None => LogLevel.None,
                _ => LogLevel.None,
            }
        );

    public void Log<TState>(
        Microsoft.Extensions.Logging.LogLevel logLevel,
        Microsoft.Extensions.Logging.EventId eventId,
        TState state,
        Exception? exception,
        Func<TState, Exception?, string> formatter
    )
    {
        var newLogLevel = logLevel switch
        {
            Microsoft.Extensions.Logging.LogLevel.Trace => LogLevel.Trace,
            Microsoft.Extensions.Logging.LogLevel.Debug => LogLevel.Debug,
            Microsoft.Extensions.Logging.LogLevel.Information => LogLevel.Information,
            Microsoft.Extensions.Logging.LogLevel.Warning => LogLevel.Warning,
            Microsoft.Extensions.Logging.LogLevel.Error => LogLevel.Error,
            Microsoft.Extensions.Logging.LogLevel.Critical => LogLevel.Critical,
            Microsoft.Extensions.Logging.LogLevel.None => LogLevel.None,
            _ => LogLevel.None,
        };

        logger.Log(newLogLevel, state, exception, formatter);
    }
}
