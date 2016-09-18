namespace Easy.Storage.Tests.Unit.Sqlite.FTS
{
    using System;

    internal class FtsContext
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