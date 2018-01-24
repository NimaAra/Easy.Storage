namespace Easy.Storage.Common
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
        private static readonly ConcurrentDictionary<TableKey, Table> Cache = new ConcurrentDictionary<TableKey, Table>();
        
        internal static Table MakeOrGet<TItem>(Dialect dialect, string name)
        {
            Ensure.NotNull(dialect, nameof(dialect));

            var tableName = name;
            var modelType = typeof(TItem);
            
            if (tableName.IsNullOrEmptyOrWhiteSpace())
            {
                tableName = GetModelName(modelType);
            }

            var key = new TableKey(modelType, dialect, tableName.GetAsEscapedSQLName());
            return Cache.GetOrAdd(key, theKey => new Table(theKey));
        }

        internal readonly HashSet<string> IgnoredProperties;
        internal readonly Dictionary<PropertyInfo, string> PropertyToColumns;
        internal readonly Dictionary<string, string> PropertyNamesToColumns;
        internal readonly PropertyInfo IdentityColumn;
        
        private Table(TableKey key)
        {
            Dialect = key.Dialect;
            ModelType = key.Type;
            Name = key.Name;

            var props = key.Type.GetProperties(BindingFlags.Public | BindingFlags.Instance);
            IdentityColumn = GetIdentityColumn(ModelType, props);

            PropertyToColumns = GetPropertiesToColumnsMappings(props, out HashSet<string> ignoredProperties);
            PropertyNamesToColumns = PropertyToColumns.ToDictionary(kv => kv.Key.Name, kv => kv.Value);

            IgnoredProperties = ignoredProperties;

            Select = Dialect.GetSelectQuery(this);
            Delete = Dialect.GetDeleteQuery(this);
            UpdateAll = Dialect.GetUpdateQuery(this, true);
            UpdateIdentity = Dialect.GetUpdateQuery(this, false);
            InsertAll = Dialect.GetInsertQuery(this, true);
            InsertIdentity = Dialect.GetInsertQuery(this, false);
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

        private static PropertyInfo GetIdentityColumn(Type modelType, PropertyInfo[] props)
        {
            var possibleIdentityColumns = props
                .Where(p => p.CustomAttributes.Any(at => at.AttributeType == typeof(IdentityAttribute)))
                .ToArray();

            Ensure.That<InvalidOperationException>(possibleIdentityColumns.Length <= 1,
                "The model can only have one property specified as the Identity.");

            // A marked Identity property has precedence over default Id property
            if (possibleIdentityColumns.Length == 1) { return possibleIdentityColumns[0]; }

            var defaultIdProp = props.SingleOrDefault(p => p.Name.Equals("Id", StringComparison.Ordinal));

            if (defaultIdProp != null) { return defaultIdProp; }

            throw new InvalidOperationException($"The model: '{modelType.Name}' does not have a default 'Id' property specified or any of its members marked as 'Identity'.");
        }

        private static string GetModelName(Type type)
        {
            var aliasAttr = type
                .GetCustomAttributes(typeof(AliasAttribute), false)
                .FirstOrDefault() as AliasAttribute;
            return aliasAttr != null ? aliasAttr.Name : type.Name;
        }

        // ReSharper disable once ParameterTypeCanBeEnumerable.Local
        private static Dictionary<PropertyInfo, string> GetPropertiesToColumnsMappings(PropertyInfo[] properties, out HashSet<string> ignored)
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
            public TableKey(Type type, Dialect dialect, string name)
            {
                Type = type;
                Dialect = dialect;
                Name = name;
            }

            internal Type Type { get; }
            internal Dialect Dialect { get; }
            internal string Name { get; }

            #region Equality

            public bool Equals(TableKey other)
            {
                return GetHashCode() == other.GetHashCode();
            }

            public static bool operator ==(TableKey left, TableKey right) => left.Equals(right);

            public static bool operator !=(TableKey left, TableKey right) => !left.Equals(right);

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj)) return false;
                return obj is TableKey key && Equals(key);
            }

            public override int GetHashCode() => HashHelper.GetHashCode(Type, Dialect, Name);

            #endregion
        }
    }
}