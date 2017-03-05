namespace Easy.Storage.Common.Filter
{
    using System;
    using System.Collections.Generic;
    using System.Linq.Expressions;
    using System.Text;
    using Easy.Common.Extensions;
    using Easy.Storage.Common.Extensions;

    /// <summary>
    /// Represents a filtered query.
    /// </summary>
    internal sealed class FilteredQuery
    {
        private readonly Table _table;
        private readonly StringBuilder _builder;
        private uint _paramCounter;

        private FilteredQuery(Table table)
        {
            _table = table;
            _builder = new StringBuilder();
            Parameters = new Dictionary<string, object>();
        }

        internal IDictionary<string, object> Parameters { get; }
        
        internal static FilteredQuery Make<T>() => new FilteredQuery(Table.Make<T>());

        /// <summary>
        /// Compiles and gets the <c>SQL</c> of the <see cref="FilteredQuery"/>.
        /// </summary>
        /// <param name="sqlToPrefix">
        /// An optional <c>SQL</c> to prefix to the result.
        /// <remarks>
        /// If <paramref name="sqlToPrefix"/> is <c>NULL</c> or <c>Empty</c>, a default 
        /// <c>SELECT</c> including all the columns for the record will be added.
        /// </remarks>
        /// </param>
        internal string Compile(string sqlToPrefix = null)
        {
            if (sqlToPrefix.IsNullOrEmpty())
            {
                sqlToPrefix = _table.Select;
            }

            // ReSharper disable once PossibleNullReferenceException
            var endIndex = sqlToPrefix.Length;
            if (sqlToPrefix.EndsWith(";"))
            {
                endIndex = endIndex - 1;
            }

            for (var i = 0; i < endIndex; i++)
            {
                _builder.Insert(i, sqlToPrefix[i]);
            }

            _builder.Append(';');

            var result = _builder.ToString();

            // clean up so we can reuse the filtering part of the query.
            _builder.Remove(0, endIndex);
            _builder.Remove(_builder.Length - 1, 1);

            return result;
        }

        internal void AddClause<T, TProperty>(Expression<Func<T, TProperty>> selector, Operator @operator, TProperty value, string clause)
        {
            AppendSQL(clause, selector, value, @operator.AsString());
        }

        internal void AddInClause<T, TProperty>(Expression<Func<T, TProperty>> selector, string clause, bool isIn, params TProperty[] values)
        {
            var inClause = isIn ? Formatter.InClauseSeparator : Formatter.NotInClauseSeparator;
            AppendSQL(clause, selector, values, inClause);
        }

        // ReSharper disable once InconsistentNaming
        private void AppendSQL<T, TProperty, TValue>(string clause, Expression<Func<T, TProperty>> selector, TValue value, string operation)
        {
            var propertyName = selector.GetPropertyName();
            var paramName = AddAndReturnParameter(propertyName, value);
            var columnName = _table.PropertyNamesToColumns[propertyName];

            //_builder.Remove(_builder.Length - 1, 1); // remove the ';'
            _builder.AppendFormat("{0}({1}{2}@{3})", clause, columnName, operation, paramName);
        }

        private string AddAndReturnParameter<TValue>(string propertyName, TValue value)
        {
            var paramName = string.Concat(propertyName, (++_paramCounter).ToString());
            Parameters.Add(paramName, value);
            return paramName;
        }
    }
}