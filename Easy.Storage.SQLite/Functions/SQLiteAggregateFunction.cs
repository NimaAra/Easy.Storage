namespace Easy.Storage.SQLite.Functions
{
    using System;
    using System.Data.SQLite;
    using Easy.Common;

    /// <summary>
    /// An abstraction to handle user-defined aggregate functions more easily. 
    /// </summary>
    // ReSharper disable once InconsistentNaming
    public sealed class SQLiteAggregateFunction : SQLiteFunctionBase
    {
        private object _state;
        private readonly Func<object[], int, object, object> _stepCallBack;
        private readonly Func<object, object> _finalCallBack;

        /// <summary>
        /// Creates an instance of the <see cref="SQLiteAggregateFunction"/>.
        /// </summary>
        /// <param name="name">The name of the function.</param>
        /// <param name="argCount">The number of arguments passed into the function.</param>
        /// <param name="state">The initial state before aggregation begins.</param>
        /// <param name="stepCallback">The callback to be invoked at every step of aggregation.</param>
        /// <param name="finalCallback">The callback to be invoked at the end of aggregation.</param>
        public SQLiteAggregateFunction(
            string name, uint argCount, object state, Func<object[], int, object, object> stepCallback, Func<object, object> finalCallback)
            : base(name, FunctionType.Aggregate, argCount)
        {
            _state = Ensure.NotNull(state, nameof(state));
            _stepCallBack = Ensure.NotNull(stepCallback, nameof(stepCallback));
            _finalCallBack = Ensure.NotNull(finalCallback, nameof(finalCallback));
        }

        /// <summary>
        /// The callback to be invoked at every step of aggregation.
        /// </summary>
        /// <param name="args">The arguments passed into the function.</param>
        /// <param name="stepNumber">The number of current aggregation step.</param>
        /// <param name="contextData">The state of aggregation.</param>
        public override void Step(object[] args, int stepNumber, ref object contextData)
            => _state = _stepCallBack(args, stepNumber, _state);

        /// <summary>
        /// The callback to be invoked at the end of aggregation.
        /// </summary>
        /// <param name="contextData">The state of aggregation at the end of all the steps.</param>
        /// <returns>The final state of aggregation.</returns>
        public override object Final(object contextData) => _finalCallBack(_state);
    }
}