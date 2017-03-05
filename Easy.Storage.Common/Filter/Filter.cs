﻿namespace Easy.Storage.Common.Filter
{
    using System;
    using System.Collections.Generic;
    using System.Linq.Expressions;

    /// <summary>
    /// Represents a filter to limit the records returned from storage.
    /// </summary>
    /// <typeparam name="T">The type of the records to filter.</typeparam>
    public sealed class Filter<T>
    {
        private readonly FilteredQuery _query;

        private Filter()
        {
            _query = FilteredQuery.Make<T>();
        }

        /// <summary>
        /// Gets the parameters generated by this instance.
        /// </summary>
        public IDictionary<string, object> Parameters => _query.Parameters;

        /// <summary>
        /// Makes and returns a <see cref="Filter{T}"/>.
        /// </summary>
        public static Filter<T> Make => new Filter<T>();

        /// <summary>
        /// Gets the <c>SQL</c> generated by this instance.
        /// </summary>
        /// <param name="sqlToPrefix">
        /// An optional <c>SQL</c> to prefix to the result.
        /// <remarks>
        /// If <paramref name="sqlToPrefix"/> is <c>NULL</c> or <c>Empty</c>, a default 
        /// <c>SELECT</c> including all the columns for the record will be added.
        /// </remarks>
        /// </param>
        // ReSharper disable once InconsistentNaming
        public string GetSQL(string sqlToPrefix = null) => _query.Compile(sqlToPrefix);
        
        /// <summary>
        /// Adds an <c>AND IN</c> query for the column represented by <paramref name="selector"/>.
        /// </summary>
        /// <param name="selector">The column to take part in the filter.</param>
        /// <param name="values">The values by which the <paramref name="selector"/> should be filtered.</param>
        public Filter<T> AndIn<TProperty>(Expression<Func<T, TProperty>> selector, params TProperty[] values)
        {
            _query.AddInClause(selector, Formatter.AndClauseSeparator, true, values);
            return this;
        }

        /// <summary>
        /// Adds an <c>AND NOT IN</c> query for the column represented by <paramref name="selector"/>.
        /// </summary>
        /// <param name="selector">The column to take part in the filter.</param>
        /// <param name="values">The values by which the <paramref name="selector"/> should be filtered.</param>
        public Filter<T> AndNotIn<TProperty>(Expression<Func<T, TProperty>> selector, params TProperty[] values)
        {
            _query.AddInClause(selector, Formatter.AndClauseSeparator, false, values);
            return this;
        }

        /// <summary>
        /// Adds an <c>AND</c> query for the column represented by <paramref name="selector"/>.
        /// </summary>
        /// <param name="selector">The column to take part in the filter.</param>
        /// <param name="operator">The operator.</param>
        /// <param name="value">The value by which the <paramref name="selector"/> should be filtered.</param>
        public Filter<T> And<TProperty>(Expression<Func<T, TProperty>> selector, Operator @operator, TProperty value)
        {
            _query.AddClause(selector, @operator, value, Formatter.AndClauseSeparator);
            return this;
        }

        /// <summary>
        /// Adds an <c>OR</c> query for the column represented by <paramref name="selector"/>.
        /// </summary>
        /// <param name="selector">The column to take part in the filter.</param>
        /// <param name="operator">The operator.</param>
        /// <param name="value">The value by which the <paramref name="selector"/> should be filtered.</param>
        public Filter<T> Or<TProperty>(Expression<Func<T, TProperty>> selector, Operator @operator, TProperty value)
        {
            _query.AddClause(selector, @operator, value, Formatter.OrClauseSeparator);
            return this;
        }

        /// <summary>
        /// Adds an <c>OR IN</c> query for the column represented by <paramref name="selector"/>.
        /// </summary>
        /// <param name="selector">The column to take part in the filter.</param>
        /// <param name="values">The values by which the <paramref name="selector"/> should be filtered.</param>
        public Filter<T> OrIn<TProperty>(Expression<Func<T, TProperty>> selector, params TProperty[] values)
        {
            _query.AddInClause(selector, Formatter.OrClauseSeparator, true, values);
            return this;
        }

        /// <summary>
        /// Adds an <c>OR NOT IN</c> query for the column represented by <paramref name="selector"/>.
        /// </summary>
        /// <param name="selector">The column to take part in the filter.</param>
        /// <param name="values">The values by which the <paramref name="selector"/> should be filtered.</param>
        public Filter<T> OrNotIn<TProperty>(Expression<Func<T, TProperty>> selector, params TProperty[] values)
        {
            _query.AddInClause(selector, Formatter.OrClauseSeparator, false, values);
            return this;
        }
    }
}