namespace Easy.Storage.SQLite.FTS
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Linq.Expressions;
    using Easy.Common;
    using Easy.Storage.Common;
    using Easy.Storage.Common.Extensions;

    /// <summary>
    /// Represents an object for creating <c>Full Text Search</c> queries.
    /// </summary>
    /// <typeparam name="T">The type of the model representing the data.</typeparam>
    public sealed class Term<T> : ITerm<T>
    {
        private const string AndClause = " "; // space instead of AND
        private const string OrClause = " OR ";
        private const string IntersectClause = "\r\nINTERSECT\r\n";
        private const string UnionClause = "\r\nUNION\r\n";
        private readonly TermConfig _config;
        private readonly List<TermInternal> _terms;
        
        private Term()
        {
            _terms = new List<TermInternal>();
            _config = TermConfig.Get();
        }

        /// <summary>
        /// Gets the term object to build the query on which by default all rows are returned.
        /// </summary>
        public static ITerm<T> All => new Term<T>();

        /// <summary>
        /// Clears all the <see cref="Term{T}"/>.
        /// </summary>
        public void Clear()
        {
            _terms.Clear();
        }

        /// <summary>
        /// Compiles and returns the textual representation of the <see cref="Term{T}"/>.
        /// </summary>
        public override string ToString()
        {
            // Can also do the following format: 
            // SELECT [Log].* FROM Log, Log_fts WHERE [Log_fts].[Foo] MATCH '"bar" OR "baz"' AND Log.Id = [Log_fts].[docId]
            var builder = StringBuilderCache.Acquire();

            if (!_terms.Any())
            {
                builder.Append(_config.PrefixQuery).Append(" 1=1");
                return builder.ToString();
            }

            foreach (var term in _terms)
            {
                string separator;
                if (term.Type == Match.Any)
                {
                    separator = OrClause;
                }
                else if (term.Type == Match.All)
                {
                    separator = AndClause;
                }
                else
                {
                    throw new ArgumentOutOfRangeException(nameof(term.Type));
                }

                if (term.JoinClause == JoinClauses.Intersect)
                {
                    builder.Append(IntersectClause);
                }
                else if (term.JoinClause == JoinClauses.Union)
                {
                    builder.Append(UnionClause);
                }

                var keywordsQuery = string.Join(separator, term.Keywords.Select(kw => "\"" + kw + "\""));
                
                if (term.IsNegation)
                {
                    builder.AppendFormat("{0} {1}.[docId] NOT IN{2}({2}{3}{0} {1}.{4} MATCH '{5}'{6})",
                        _config.PrefixQuery, _config.TableName, Formatter.NewLine, Formatter.Spacer, term.ColumnName, keywordsQuery, Formatter.NewLine);
                } else
                {

                    builder.AppendFormat("{0} {1}.{2} MATCH '{3}'", 
                        _config.PrefixQuery, _config.TableName, term.ColumnName, keywordsQuery);
                }
            }

            return StringBuilderCache.GetStringAndRelease(builder);
        }

        internal void Add<TProperty>(JoinClauses clause, bool isNegation, Match type, Expression<Func<T, TProperty>> selector, params TProperty[] values)
        {
            var colName = _config.GetColumnName(selector.GetPropertyName());
            var termInternal = new TermInternal
            {
                IsNegation = isNegation,
                Type = type,
                JoinClause = _terms.Any() ? clause : JoinClauses.None,
                ColumnName = colName,
                Keywords = ToKeywords(values)
            };

            _terms.Add(termInternal);
        }

        private static IEnumerable<string> ToKeywords<TProperty>(params TProperty[] keywords)
        {
            foreach (object item in keywords)
            {
                if (item.GetType().IsEnum)
                {
                    yield return ((int)item).ToString();
                }
                else
                {
                    yield return item.ToString();
                }
            }
        }

        private sealed class TermConfig
        {
            private static readonly ConcurrentDictionary<Type, TermConfig> Cache = new ConcurrentDictionary<Type, TermConfig>();
            private readonly Table _table;

            internal static TermConfig Get()
            {
                return Cache.GetOrAdd(typeof(T), theType => new TermConfig());
            }

            private TermConfig()
            {
                _table = Table.Get<T>();
                TableName = "[" + _table.Name + "_fts]";
                PrefixQuery = $"SELECT {TableName}.[docId] FROM {TableName} WHERE";
            }

            internal string PrefixQuery { get; }
            internal string TableName { get; }

            internal string GetColumnName(string getPropertyName)
            {
                return _table.PropertyNamesToColumns[getPropertyName];
            }
        }

        private sealed class TermInternal
        {
            internal bool IsNegation { get; set; }
            internal Match Type { get; set; }
            internal JoinClauses JoinClause { get; set; }
            internal string ColumnName { get; set; }
            internal IEnumerable<string> Keywords { get; set; }
        }
    }
}