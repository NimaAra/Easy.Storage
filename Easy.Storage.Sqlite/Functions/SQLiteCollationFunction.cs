namespace Easy.Storage.Sqlite.Functions
{
    using System;
    using System.Data.SQLite;
    using Easy.Common;

    /// <summary>
    /// An abstraction to handle user-defined collation functions more easily. 
    /// </summary>
    // ReSharper disable once InconsistentNaming
    public sealed class SQLiteCollationFunction : SQLiteFunctionBase
    {
        private readonly Func<string, string, int> _callBack;

        /// <summary>
        /// Creates an instance of the <see cref="SQLiteCollationFunction"/>.
        /// </summary>
        /// <param name="name">The name of the function.</param>
        /// <param name="compare">The function to perform comparison.</param>
        public SQLiteCollationFunction(string name, Func<string, string, int> compare)
            : base(name, FunctionType.Collation, 2)
        {
            _callBack = Ensure.NotNull(compare, nameof(compare));
        }

        /// <summary>
        /// User-defined collating sequences override this method to provide a custom string sorting algorithm. 
        /// </summary>
        /// <returns>
        /// <c>1</c> if <paramref name="first"/> is greater than <paramref name="second"/>, 
        /// <c>0</c> if they are equal, or <c>-1</c> if <paramref name="first"/> is less than <paramref name="second"/>.</returns>
        public override int Compare(string first, string second) => _callBack(first, second);
    }
}