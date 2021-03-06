﻿namespace Easy.Storage.Common
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
    using Easy.Common;
    using Easy.Common.Extensions;
    using Easy.Storage.Common.Attributes;
    using Easy.Storage.Common.Extensions;

    /// <summary>
    /// Represents all the required information about a given table and a matching <c>C# POCO</c>.
    /// </summary>
    public sealed class Table
    {
        private static readonly Type KeyAttributeType = typeof(KeyAttribute);
        private static readonly ConcurrentDictionary<TableKey, Table> Cache = 
            new ConcurrentDictionary<TableKey, Table>();
        
        internal static Table MakeOrGet<T>(Dialect dialect, string name)
        {
            Ensure.NotNull(dialect, nameof(dialect));

            var key = new TableKey(typeof(T), dialect, name.GetHashCode());
            return Cache.GetOrAdd(key, k => new Table(k, name));
        }

        internal readonly HashSet<string> IgnoredProperties;
        internal readonly Dictionary<PropertyInfo, string> PropertyToColumns;
        internal readonly Dictionary<string, string> PropertyNamesToColumns;
        internal readonly bool HasIdentityColumn;
        internal readonly PropertyInfo IdentityColumn;

        private Table(TableKey key, string tableName)
        {
            Dialect = key.Dialect;
            ModelType = key.Type;

            if (tableName.IsNullOrEmpty())
            {
                tableName = GetModelName(ModelType);
            }

            Name = tableName.GetAsEscapedSQLName();

            var props = key.Type.GetProperties(BindingFlags.Public | BindingFlags.Instance);

            PropertyToColumns = GetPropertiesToColumnsMappings(props, out HashSet<string> ignoredProperties);
            PropertyNamesToColumns = PropertyToColumns.ToDictionary(kv => kv.Key.Name, kv => kv.Value);
            IgnoredProperties = ignoredProperties;

            Select = Dialect.GetSelectQuery(this);
            Delete = Dialect.GetDeleteQuery(this);
            
            GetKeyProperty(props, out IdentityColumn, out HasIdentityColumn);
            
            UpdateAll = Dialect.GetUpdateQuery(this, true);
            InsertAll = Dialect.GetInsertQuery(this, true);
            
            if (HasIdentityColumn)
            {
                UpdateIdentity = Dialect.GetUpdateQuery(this, false);
                InsertIdentity = Dialect.GetInsertQuery(this, false);
            }
        }

        /// <summary>
        /// Gets the <see cref="Dialect"/> used to generate this instance.
        /// </summary>
        public Dialect Dialect { get; }

        /// <summary>
        /// Gets the type of the model represented by this instance.
        /// </summary>
        public Type ModelType { get; }

        /// <summary>
        /// Gets the name by which the model will be stored as.
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// Gets the <c>SELECt</c> query to retrieve the model.
        /// </summary>
        public string Select { get; }

        /// <summary>
        /// Gets the default <c>UPDATE</c> query to update the model based on the model's Ids.
        /// </summary>
        public string UpdateIdentity { get; }

        /// <summary>
        /// Gets the default <c>UPDATE</c> query to update the model based on any of the model's columns.
        /// </summary>
        public string UpdateAll { get; }

        /// <summary>
        /// Gets the <c>INSERT</c> query to store the model with an identity column defined.
        /// </summary>
        public string InsertIdentity { get; }

        /// <summary>
        /// Gets the <c>INSERT</c> query to store all the columns of the model.
        /// </summary>
        public string InsertAll { get; }

        /// <summary>
        /// Gets the <c>DELETE</c> query to delete the model.
        /// </summary>
        public string Delete { get; }

        private static void GetKeyProperty(
            PropertyInfo[] props, out PropertyInfo result, out bool isIdentity)
        {
            var possibleIdentityColumns = props
                .Where(p => p.CustomAttributes.Any(at => at.AttributeType == KeyAttributeType))
                .ToArray();

            if (possibleIdentityColumns.Length > 1)
            {
                throw new InvalidOperationException("The model can only have one property specified as the Identity.");
            }
            
            // A marked Identity property has precedence over default Id property
            if (possibleIdentityColumns.Length == 1)
            {
                result = possibleIdentityColumns[0];
                isIdentity = result.GetCustomAttribute<KeyAttribute>().IsIdentity;
                return;
            }

            var defaultIdProp = props.SingleOrDefault(p => p.Name.Equals("Id", StringComparison.Ordinal));
            if (defaultIdProp is null)
            {
                result = null;
                isIdentity = false;
            } else
            {
                result = defaultIdProp;
                isIdentity = true;
            }
        }

        private static string GetModelName(Type type) => 
            type.GetCustomAttributes(typeof(AliasAttribute), false)
                .FirstOrDefault() is AliasAttribute aliasAttr ? aliasAttr.Name : type.Name;

        // ReSharper disable once ParameterTypeCanBeEnumerable.Local
        private static Dictionary<PropertyInfo, string> GetPropertiesToColumnsMappings(
            PropertyInfo[] properties, out HashSet<string> ignored)
        {
            var ignoredProperties = new HashSet<string>();
            var result = new Dictionary<PropertyInfo, string>();

            foreach (var prop in properties)
            {
                var ignoreAttr = prop.GetCustomAttribute<IgnoreAttribute>();
                if (ignoreAttr != null)
                {
                    ignoredProperties.Add(prop.Name);
                    continue;
                }

                var propName = prop.Name;
                var aliasAttr = prop.GetCustomAttribute<AliasAttribute>();
                var columnName = aliasAttr == null ? propName : aliasAttr.Name;
                columnName = columnName.GetAsEscapedSQLName();

                result.Add(prop, columnName);
            }

            ignored = ignoredProperties;
            return result;
        }

        private struct TableKey : IEquatable<TableKey>
        {
            private readonly int _tableHash;
            
            public TableKey(Type type, Dialect dialect, int tableHash)
            {
                Type = type;
                Dialect = dialect;
                _tableHash = tableHash;
            }


            internal Type Type { get; }
            internal Dialect Dialect { get; }
            
            #region Equality

            public bool Equals(TableKey other) => GetHashCode() == other.GetHashCode();

            public static bool operator ==(TableKey left, TableKey right) => left.Equals(right);

            public static bool operator !=(TableKey left, TableKey right) => !left.Equals(right);

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj)) return false;
                return obj is TableKey key && Equals(key);
            }

            public override int GetHashCode() => HashHelper.GetHashCode(Type, Dialect, _tableHash);

            #endregion
        }
    }
}