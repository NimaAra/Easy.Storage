namespace Easy.Storage.SQLite.FTS
{
    /// <summary>
    /// Represents the type based on which a match should be applied.
    /// </summary>
    public enum Match
    {
        /// <summary>
        /// Matches any of the items provided.
        /// </summary>
        Any,

        /// <summary>
        /// Matches all of the items provided.
        /// </summary>
        All
    }
}