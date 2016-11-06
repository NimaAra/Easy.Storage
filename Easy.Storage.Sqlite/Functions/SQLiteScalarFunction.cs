namespace Easy.Storage.Sqlite.Functions
{
    using System;
    using System.Data.SQLite;
    using Easy.Common;

    /// <summary>
    /// An abstraction to handle user-defined scalar functions more easily. 
    /// </summary>
    // ReSharper disable once InconsistentNaming
    public sealed class SQLiteScalarFunction : SQLiteFunctionBase
    {
        private readonly Func<object[], object> _callBack;

        /// <summary>
        /// Creates an instance of the <see cref="SQLiteScalarFunction"/>.
        /// </summary>
        /// <param name="name">The name of the function.</param>
        /// <param name="argCount">The number of arguments passed into the function.</param>
        /// <param name="callBack">The function to be invoked for every column passed into this function.</param>
        public SQLiteScalarFunction(string name, uint argCount, Func<object[], object> callBack)
            : base(name, FunctionType.Scalar, argCount)
        {
            _callBack = Ensure.NotNull(callBack, nameof(callBack));
        }

        /// <summary>
        /// The function to be invoked for every column passed into this function.
        /// </summary>
        /// <param name="args">The arguments passed into the function.</param>
        /// <returns>The result of the scalar function.</returns>
        public override object Invoke(object[] args) => _callBack(args);
    }
}