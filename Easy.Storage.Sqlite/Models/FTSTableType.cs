namespace Easy.Storage.SQLite.Models
{
    /// <summary>
    /// Represents the different types of <c>SQLite</c> Full Text Search table.
    /// </summary>
    // ReSharper disable once InconsistentNaming
    public enum FTSTableType
    {
        /// <summary>
        /// An <c>FTS</c> table that does not store a copy of the indexed documents at all.
        /// </summary>
        ContentLess = 0,

        /// <summary>
        /// An <c>FTS</c> table is similar to a <c>Contentless</c> table, except that if evaluation of a query requires 
        /// the value of a column other than <c>docId</c>, <c>FTS</c> attempts to retrieve that value from a table 
        /// (or view, or virtual table) nominated by the user.
        /// </summary>
        ExternalContent,

        /// <summary>
        /// An <c>FTS</c> table which stores a copy of the indexed documents internally.
        /// </summary>
        Content
    }
}