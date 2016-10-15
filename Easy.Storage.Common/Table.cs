namespace Easy.Storage.Common
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using Easy.Common;
    using Easy.Common.Extensions;
    using Easy.Storage.Common.Attributes;
    using Easy.Storage.Common.Extensions;

    /// <summary>
    /// Represents information about the table a model should be stored at.
    /// </summary>
    public sealed class Table
    {
        private static readonly ConcurrentDictionary<TableKey, Table> Cache = new ConcurrentDictionary<TableKey, Table>();
        internal static Table Get<TItem>(Dialect dialect = Dialect.Generic)
        {
            var key = new TableKey(typeof(TItem), dialect);
            return Cache.GetOrAdd(key, theKey => new Table(theKey));
        }

        internal readonly Dictionary<PropertyInfo, string> PropertyToColumns;
        internal readonly Dictionary<string, string> PropertyNamesToColumns;
        internal readonly PropertyInfo PrimaryKey;

        private Table(TableKey key)
        {
            Dialect = key.Dialect;
            Name = GetModelName(key.Type);
            var props = key.Type.GetProperties(BindingFlags.Public | BindingFlags.Instance);

            PrimaryKey = GetPrimaryKey(props);
            PropertyToColumns = GetPropertiesToColumnsMappings(props);
            PropertyNamesToColumns = PropertyToColumns.ToDictionary(kv => kv.Key.Name, kv => kv.Value);

            var propNames = PropertyToColumns.Keys.Select(p => p.Name);
            var colNames = PropertyToColumns.Values;

            var propToColsMinusPrimaryKey = PropertyToColumns.Where(p => p.Key != PrimaryKey).ToArray();
            var colNamesMinusPrimaryKey = propToColsMinusPrimaryKey.Select(kv => kv.Value).ToArray();
            var propNamesMinusPrimaryKey = propToColsMinusPrimaryKey.Select(kv => kv.Key.Name).ToArray();

            var columnsMinusPrimaryKey = string.Join(Formatter.ColumnSeparator, colNamesMinusPrimaryKey);
            var propertiesMinusPrimaryKey = string.Join(Formatter.ColumnSeparator, propNamesMinusPrimaryKey.Select(x => "@" + x));
            var colsAsPropNameAlias = string.Join(Formatter.ColumnSeparator, colNames.Zip(propNames, (col, prop) => $"{col} AS '{prop}'"));
            var colEqualPropMinusPrimaryKey = string.Join(Formatter.ColumnSeparator, colNamesMinusPrimaryKey.Zip(propNamesMinusPrimaryKey, (col, propName) => $"{col} = @{propName}"));

            Select =        $"SELECT{Formatter.NewLine}{Formatter.Spacer}{colsAsPropNameAlias}{Formatter.NewLine}FROM {Name}{Formatter.NewLine}WHERE{Formatter.NewLine}{Formatter.Spacer}1 = 1;";
            UpdateDefault = $"UPDATE {Name} SET{Formatter.NewLine}{Formatter.Spacer}{colEqualPropMinusPrimaryKey}{Formatter.NewLine}WHERE{Formatter.NewLine}{Formatter.Spacer}{PropertyToColumns[PrimaryKey]} = @{PrimaryKey.Name};";
            UpdateCustom =  $"UPDATE {Name} SET{Formatter.NewLine}{Formatter.Spacer}{colEqualPropMinusPrimaryKey}{Formatter.NewLine}WHERE{Formatter.NewLine}{Formatter.Spacer}1 = 1;";
            Delete =        $"DELETE FROM {Name}{Formatter.NewLine}WHERE{Formatter.NewLine}{Formatter.Spacer}1 = 1;";
            Insert =        GetInsertQuery(Dialect, Name, columnsMinusPrimaryKey, propertiesMinusPrimaryKey);
        }

        /// <summary>
        /// Gets the type of the <c>SQL</c> database.
        /// </summary>
        internal Dialect Dialect { get; }

        /// <summary>
        /// Gets the name by which the model will be stored as.
        /// </summary>
        internal string Name { get; }

        /// <summary>
        /// Gets the <c>SELECt</c> query to retrieve the model.
        /// </summary>
        internal string Select { get; }

        /// <summary>
        /// Gets the <c>INSERT</c> query to store the model.
        /// </summary>
        internal string Insert { get; }

        /// <summary>
        /// Gets the default <c>UPDATE</c> query to update the model based on the model's Ids.
        /// </summary>
        internal string UpdateDefault { get; }

        /// <summary>
        /// Gets the default <c>UPDATE</c> query to update the model based on any of the model's columns.
        /// </summary>
        internal string UpdateCustom { get; }

        /// <summary>
        /// Gets the <c>DELETE</c> query to delete the model.
        /// </summary>
        internal string Delete { get; }

        internal string GetSqlWithClause<T, TProperty>(Expression<Func<T, TProperty>> selector, string baseQuery, bool single)
        {
            var propertyName = selector.GetPropertyName();
            var columnName = PropertyNamesToColumns[propertyName];

            const string MultipleClause = " IN @Values";
            const string SingleClause = " = @Value";
            
            var subQuery = $"{Formatter.AndClauseSeparator}({columnName} {(single ? SingleClause : MultipleClause)});";
            return baseQuery.Replace(";", subQuery);
        }

        private static PropertyInfo GetPrimaryKey(PropertyInfo[] props)
        {
            var possiblePrimaryKeys = props.Where(p => p.CustomAttributes.Any(at => at.AttributeType == typeof(PrimaryKeyAttribute)))
                .ToArray();

            Ensure.That<InvalidOperationException>(possiblePrimaryKeys.Length <= 1,
                "The model can only have one property specified as the Primary Key.");

            // A marked PrimaryKey has precedence over default Id property
            if (possiblePrimaryKeys.Length == 1) { return possiblePrimaryKeys[0]; }

            var defaultIdProp = props.SingleOrDefault(p => p.Name == "Id" &&
                (p.PropertyType == typeof(int) || p.PropertyType == typeof(long)));

            if (defaultIdProp != null) { return defaultIdProp; }

            throw new InvalidOperationException("The model does not have a default 'Id' property specified or any it's members marked as primary key.");
        }

        private static string GetInsertQuery(Dialect dialect, string name, string columnsMinusIds, string propertiesMinusIds)
        {
            switch (dialect)
            {
                case Dialect.Generic:
                    return $"INSERT INTO {name}{Formatter.NewLine}({Formatter.NewLine}{Formatter.Spacer}{columnsMinusIds}{Formatter.NewLine}){Formatter.NewLine}VALUES{Formatter.NewLine}({Formatter.NewLine}{Formatter.Spacer}{propertiesMinusIds}{Formatter.NewLine});";
                case Dialect.Sqlite:
                    return $"INSERT INTO {name}{Formatter.NewLine}({Formatter.NewLine}{Formatter.Spacer}{columnsMinusIds}{Formatter.NewLine}){Formatter.NewLine}VALUES{Formatter.NewLine}({Formatter.NewLine}{Formatter.Spacer}{propertiesMinusIds}{Formatter.NewLine}); SELECT last_insert_rowid();";
                case Dialect.SqlServer:
                    var idColumnName = "Id";
                    var declarationClause = $"DECLARE @InsertedRows AS TABLE ({idColumnName} BIGINT);";
                    var outputClause = $"OUTPUT Inserted.{idColumnName} INTO @InsertedRows";
                    var selectInsertedIdClause = $"SELECT {idColumnName} FROM @InsertedRows;";
                    return $"{declarationClause}{Formatter.NewLine}INSERT INTO {name}{Formatter.NewLine}({Formatter.NewLine}{Formatter.Spacer}{columnsMinusIds}{Formatter.NewLine}) {outputClause}{Formatter.NewLine}VALUES{Formatter.NewLine}({Formatter.NewLine}{Formatter.Spacer}{propertiesMinusIds}{Formatter.NewLine});{Formatter.NewLine}{selectInsertedIdClause}";
            }

            throw new ArgumentOutOfRangeException(nameof(dialect), dialect, null);
        }

        private static string GetModelName(Type type)
        {
            var aliasAttr = type
                .GetCustomAttributes(typeof(AliasAttribute), false)
                .FirstOrDefault() as AliasAttribute;

            return aliasAttr != null ? aliasAttr.Name : type.Name;
        }

        private static string EscapeAsSqlName(string name)
        {
            if (name.IsNullOrEmptyOrWhiteSpace()) { return name; }

            if (!name.StartsWith("[") && !name.EndsWith("]"))
            {
                name = string.Concat("[", name, "]");
            }
            return name;
        }

        // ReSharper disable once ParameterTypeCanBeEnumerable.Local
        private static Dictionary<PropertyInfo, string> GetPropertiesToColumnsMappings(PropertyInfo[] properties)
        {
            var result = new Dictionary<PropertyInfo, string>();
            foreach (var prop in properties)
            {
                var ignoreAttr = prop.GetCustomAttribute<IgnoreAttribute>();
                if (ignoreAttr != null) { continue; }

                var propName = prop.Name;
                var aliasAttr = prop.GetCustomAttribute<AliasAttribute>();
                var columnName = aliasAttr == null ? propName : aliasAttr.Name;
                columnName = EscapeAsSqlName(columnName);

                result.Add(prop, columnName);
            }

            return result;
        }

        private struct TableKey : IEquatable<TableKey>
        {
            public TableKey(Type type, Dialect dialect)
            {
                Type = type;
                Dialect = dialect;
            }

            internal Type Type { get; }
            internal Dialect Dialect { get; }

            #region Equality

            public bool Equals(TableKey other)
            {
                return GetHashCode() == other.GetHashCode();
            }

            public static bool operator ==(TableKey left, TableKey right)
            {
                return left.Equals(right);
            }

            public static bool operator !=(TableKey left, TableKey right)
            {
                return !left.Equals(right);
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj)) return false;
                return obj is TableKey && Equals((TableKey) obj);
            }

            public override int GetHashCode()
            {
                return HashHelper.GetHashCode(Type, Dialect);
            }

            #endregion
        }
    }
}