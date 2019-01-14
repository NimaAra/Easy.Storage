namespace Easy.Storage.Common
{
    using System;
    using System.Collections.Generic;
    using Easy.Storage.Common.Filter;
    using System.Linq.Expressions;
    using Easy.Common;
    using Easy.Common.Extensions;

    /// <summary>
    /// Represents a helper class to be used for creating queries 
    /// and filters for the given model of type <typeparamref name="T"/>.
    /// </summary>
    public class Query<T>
    {
        private readonly Table _table;

        private Query(Table table) => _table = table;
        
        internal static Query<T> Make(Table table) => new Query<T>(table);

        /// <summary>
        /// Gets an instance of the <see cref="Filter{T}"/> for creating query filters.
        /// </summary>
        public Filter<T> Filter => new Filter<T>(_table);

        // [ToDo] - Add OrderBy etc
    }

    // [ToDo] - CoMPLETE make public?
    internal sealed class OrderBy<T>
    {
        private readonly Queue<KeyValuePair<string, bool>> _clauses;

        internal OrderBy()
        {
            _clauses = new Queue<KeyValuePair<string, bool>>();
        }

        public OrderBy<T> Ascending<TProperty>(Expression<Func<T, TProperty>> selector)
        {
            var propName = selector.GetPropertyName();
            _clauses.Enqueue(new KeyValuePair<string, bool>(propName, true));
            return this;
        }

        public OrderBy<T> Descending<TProperty>(Expression<Func<T, TProperty>> selector)
        {
            var propName = selector.GetPropertyName();
            _clauses.Enqueue(new KeyValuePair<string, bool>(propName, false));
            return this;
        }

        internal string GetSQL()
        {
            if (_clauses.Count == 0) { return string.Empty; }

            var builder = StringBuilderCache.Acquire();

            builder.Append("ORDER BY");
            
            foreach (var pair in _clauses)
            {
                builder.AppendLine().Append("  ").Append(pair.Key).Append(" ").Append(pair.Value ? "ASC" : "DESC");
            }

            return StringBuilderCache.GetStringAndRelease(builder);
        }
    }
}