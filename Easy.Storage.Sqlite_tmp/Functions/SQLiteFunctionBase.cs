namespace Easy.Storage.SQLite.Functions
{
    using System.Data.SQLite;
    using Easy.Common;

    /// <summary>
    /// An abstraction designed to handle user-defined functions more easily. 
    /// An instance of the derived class is made for each connection to the database. 
    /// </summary>
    // ReSharper disable once InconsistentNaming
    public abstract class SQLiteFunctionBase : SQLiteFunction
    {
        /// <summary>
        /// Creates an instance of the <see cref="SQLiteFunctionBase"/>.
        /// </summary>
        protected SQLiteFunctionBase(string name, FunctionType type, uint argumentCount)
        {
            Ensure.NotNullOrEmptyOrWhiteSpace(name);
            Name = name;
            Type = type;
            ArgumentCount = (int)argumentCount;
        }

        /// <summary>
        /// Gets the name of the function.
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// Gets the number of arguments passed into the function.
        /// </summary>
        public int ArgumentCount { get; }

        /// <summary>
        /// Gets the type of the function.
        /// </summary>
        internal FunctionType Type { get; }
    }
}