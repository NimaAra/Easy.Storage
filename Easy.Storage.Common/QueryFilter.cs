namespace Easy.Storage.Common
{
    using System;
    using System.Linq.Expressions;
    using System.Text;
    using Dapper;
    using Easy.Common;
    using Easy.Storage.Common.Extensions;

    /// <summary>
    /// Represents an object used to specify a filter for querying the data.
    /// </summary>
    /// <typeparam name="T">The type of the model to be queried.</typeparam>
    public sealed class QueryFilter<T> // [ToDo] - add IN, NOT IN, OrderBy, Dont' keep hold of _builder
    {
        private readonly Table _table;
        private readonly StringBuilder _builder;
        private uint _paramCounter;

        /// <summary>
        /// Creates an instance of the <see cref="QueryFilter{T}"/>.
        /// </summary>
        public QueryFilter()
        {
            _table = Table.Get<T>();
            _builder = new StringBuilder(_table.Select);
            Parameters = new DynamicParameters();
        }

        internal string Query => _builder.ToString();
        internal DynamicParameters Parameters { get; }

        /// <summary>
        /// Adds an <c>AND</c> clause to the <see cref="QueryFilter{T}"/>.
        /// </summary>
        /// <param name="selector">The property of the model whose value should be included in the filter</param>
        /// <param name="operand">The operand defining the filter</param>
        /// <param name="value">The value of the clause</param>
        public void And<TProperty>(Expression<Func<T, TProperty>> selector, Operand operand, TProperty value)
        {
            Ensure.NotNull(selector, nameof(selector));
            AddClause(Formatter.AndClauseSeparator, selector, operand, value);
        }

        /// <summary>
        /// Adds an <c>Or</c> clause to the <see cref="QueryFilter{T}"/>.
        /// </summary>
        /// <param name="selector">The property of the model whose value should be included in the filter</param>
        /// <param name="operand">The operand defining the filter</param>
        /// <param name="value">The value of the clause</param>
        public void Or<TProperty>(Expression<Func<T, TProperty>> selector, Operand operand, TProperty value)
        {
            Ensure.NotNull(selector, nameof(selector));
            AddClause(Formatter.OrClauseSeparator, selector, operand, value);
        }

        private void AddClause<TProperty>(
            string clause, 
            Expression<Func<T, TProperty>> 
            selector, 
            Operand operand,
            TProperty value)
        {
            var propertyName = selector.GetPropertyName();
            var columnName = _table.PropertyNamesToColumns[propertyName];

            var paramName = string.Concat(propertyName, _paramCounter++.ToString());
            Parameters.Add(paramName, value);

            _builder.Remove(_builder.Length - 1, 1); // remove the ';'
            _builder.AppendFormat("{0}({1}{2}@{3});", clause, columnName, operand.OperandAsStr(), paramName);
        }
    }

    /// <summary>
    /// Represents different operations supported by the <see cref="QueryFilter{T}"/>.
    /// </summary>
    public enum Operand
    {
        /// <summary>
        /// Equal.
        /// </summary>
        Equal = 0,

        /// <summary>
        /// Not equal.
        /// </summary>
        NotEqual,

        /// <summary>
        /// Greater than.
        /// </summary>
        GreaterThan,

        /// <summary>
        /// Greater than or equal.
        /// </summary>
        GreaterThanOrEqual,

        /// <summary>
        /// Less than.
        /// </summary>
        LessThan,

        /// <summary>
        /// Less than or equal.
        /// </summary>
        LessThanOrEqual
    }
}