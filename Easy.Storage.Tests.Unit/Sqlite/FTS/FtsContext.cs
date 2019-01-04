namespace Easy.Storage.Tests.Unit.SQLite.FTS
{
    using System;

    internal class FTSContext
    {
        internal sealed class Log
        {
            public long Id { get; set; }
            public Level Level { get; set; }
            public DateTime Timestamp { get; set; }
            public string Message { get; set; }
        }

        internal enum Level
        {
            Debug = 0,
            Info,
            Warn,
            Error,
            Fatal
        }
    }
}