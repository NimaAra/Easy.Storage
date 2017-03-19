namespace Easy.Storage.Common
{

    /// <summary>
    /// Represents a generic <c>SQL</c> dialect.
    /// </summary>
    public sealed class GenericSQLDialect : Dialect
    {
        static GenericSQLDialect() { }
        private GenericSQLDialect() : base(DialectType.Generic) { }

        /// <summary>
        /// Gets a single instance of the <see cref="GenericSQLDialect"/>.
        /// </summary>
        public static GenericSQLDialect Instance { get; } = new GenericSQLDialect();

        internal override string GetPartialInsertQuery<T>(Table table, object item) => throw new System.NotImplementedException();
    }
}