namespace Easy.Storage.Common
{
    /// <summary>
    /// Represents the kind of the <c>SQL</c> database.
    /// </summary>
    internal enum Dialect
    {
        Generic = 0,
        Sqlite,
        SqlServer
    }
}