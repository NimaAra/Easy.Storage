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
        
        internal static Table Make<TItem>(Dialect dialect = Dialect.Generic)
        {
            var key = new TableKey(typeof(TItem), dialect);
            return Cache.GetOrAdd(key, theKey => new Table(theKey));
        }

        internal readonly Dictionary<PropertyInfo, string> PropertyToColumns;
        internal readonly Dictionary<string, string> PropertyNamesToColumns;
        internal readonly PropertyInfo IdentityColumn;
        // ReSharper disable once InconsistentNaming
        private const string SQLServerInsertedRowDeclarationClause = "DECLARE @InsertedRows AS TABLE (Id BIGINT);";
        // ReSharper disable once InconsistentNaming
        private const string SQLServerSelectInsertedRowClause = "SELECT Id FROM @InsertedRows;";

        private Table(TableKey key)
        {
            Dialect = key.Dialect;
            Name = GetModelName(key.Type);
            var props = key.Type.GetProperties(BindingFlags.Public | BindingFlags.Instance);

            IdentityColumn = GetIdentityColumn(props);
            PropertyToColumns = GetPropertiesToColumnsMappings(props);
            PropertyNamesToColumns = PropertyToColumns.ToDictionary(kv => kv.Key.Name, kv => kv.Value);

            var propNames = PropertyToColumns.Keys.Select(p => p.Name);
            var colNames = PropertyToColumns.Values;

            var allColNames = PropertyToColumns.Select(kv => kv.Value).ToArray();
            var allPropNames = PropertyToColumns.Select(kv => kv.Key.Name).ToArray();

            var columns = string.Join(Formatter.ColumnSeparator, allColNames);
            var properties = string.Join(Formatter.ColumnSeparator, allPropNames.Select(x => "@" + x));

            var insertSeg = $"INSERT INTO [{Name}]{Formatter.NewLine}({Formatter.NewLine}{Formatter.Spacer}{columns}{Formatter.NewLine})";
            var valuesSeg = $"{Formatter.NewLine}VALUES{Formatter.NewLine}({Formatter.NewLine}{Formatter.Spacer}{properties}{Formatter.NewLine});";
            var insertAll = GetInsertQueries(Dialect, insertSeg, valuesSeg);

            var propToColsMinusIdentity = PropertyToColumns.Where(p => p.Key != IdentityColumn).ToArray();
            var colNamesMinusIdentity = propToColsMinusIdentity.Select(kv => kv.Value).ToArray();
            var propNamesMinusIdentity = propToColsMinusIdentity.Select(kv => kv.Key.Name).ToArray();

            var columnsMinusIdentity = string.Join(Formatter.ColumnSeparator, colNamesMinusIdentity);
            var propertiesMinusIdentity = string.Join(Formatter.ColumnSeparator, propNamesMinusIdentity.Select(x => "@" + x));

            insertSeg = $"INSERT INTO [{Name}]{Formatter.NewLine}({Formatter.NewLine}{Formatter.Spacer}{columnsMinusIdentity}{Formatter.NewLine})";
            valuesSeg = $"{Formatter.NewLine}VALUES{Formatter.NewLine}({Formatter.NewLine}{Formatter.Spacer}{propertiesMinusIdentity}{Formatter.NewLine});";
            var insertIdentity = GetInsertQueries(Dialect, insertSeg, valuesSeg);

            var colsAsPropNameAlias = string.Join(Formatter.ColumnSeparator, colNames.Zip(propNames, (col, prop) => $"[{Name}].{col} AS '{prop}'"));
            var colEqualPropMinusIdentity = string.Join(Formatter.ColumnSeparator, colNamesMinusIdentity.Zip(propNamesMinusIdentity, (col, propName) => $"{col} = @{propName}"));

            Select =                $"SELECT{Formatter.NewLine}{Formatter.Spacer}{colsAsPropNameAlias}{Formatter.NewLine}FROM [{Name}]{Formatter.NewLine}WHERE{Formatter.NewLine}{Formatter.Spacer}1 = 1;";
            UpdateDefault =         $"UPDATE [{Name}] SET{Formatter.NewLine}{Formatter.Spacer}{colEqualPropMinusIdentity}{Formatter.NewLine}WHERE{Formatter.NewLine}{Formatter.Spacer}{PropertyToColumns[IdentityColumn]} = @{IdentityColumn.Name};";
            UpdateCustom =          $"UPDATE [{Name}] SET{Formatter.NewLine}{Formatter.Spacer}{colEqualPropMinusIdentity}{Formatter.NewLine}WHERE{Formatter.NewLine}{Formatter.Spacer}1 = 1;";
            Delete =                $"DELETE FROM [{Name}]{Formatter.NewLine}WHERE{Formatter.NewLine}{Formatter.Spacer}1 = 1;";
            InsertIdentity =        insertIdentity;
            InsertAll =             insertAll;
        }

        /// <summary>
        /// Gets the type of the <c>SQL</c> database.
        /// </summary>
        public Dialect Dialect { get; }

        /// <summary>
        /// Gets the name by which the model will be stored as.
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// Gets the <c>SELECt</c> query to retrieve the model.
        /// </summary>
        public string Select { get; }

        /// <summary>
        /// Gets the <c>INSERT</c> query to store the model with an identity column defined.
        /// </summary>
        public string InsertIdentity { get; }

        /// <summary>
        /// Gets the <c>INSERT</c> query to store all the columns of the model.
        /// </summary>
        public string InsertAll { get; }

        /// <summary>
        /// Gets the default <c>UPDATE</c> query to update the model based on the model's Ids.
        /// </summary>
        public string UpdateDefault { get; }

        /// <summary>
        /// Gets the default <c>UPDATE</c> query to update the model based on any of the model's columns.
        /// </summary>
        public string UpdateCustom { get; }

        /// <summary>
        /// Gets the <c>DELETE</c> query to delete the model.
        /// </summary>
        public string Delete { get; }

        internal string GetSqlWithClause<T, TProperty>(Expression<Func<T, TProperty>> selector, string baseQuery, bool single)
        {
            var propertyName = selector.GetPropertyName();
            var columnName = PropertyNamesToColumns[propertyName];

            const string MultipleClause = " IN @Values";
            const string SingleClause = " = @Value";
            
            var subQuery = $"{Formatter.AndClauseSeparator}({columnName} {(single ? SingleClause : MultipleClause)});";
            return baseQuery.Replace(";", subQuery);
        }

        private static PropertyInfo GetIdentityColumn(PropertyInfo[] props)
        {
            var possibleIdentityColumns = props.Where(p => p.CustomAttributes.Any(at => at.AttributeType == typeof(IdentityAttribute)))
                .ToArray();

            Ensure.That<InvalidOperationException>(possibleIdentityColumns.Length <= 1,
                "The model can only have one property specified as the Identity.");

            // A marked Identity property has precedence over default Id property
            if (possibleIdentityColumns.Length == 1) { return possibleIdentityColumns[0]; }

            var defaultIdProp = props.SingleOrDefault(p => p.Name == "Id" &&
                (p.PropertyType == typeof(int) || p.PropertyType == typeof(long)));

            if (defaultIdProp != null) { return defaultIdProp; }

            throw new InvalidOperationException("The model does not have a default 'Id' property specified or any of its members marked as Identity.");
        }

        private string GetInsertQueries(Dialect dialect, string insertSegment, string valuesSegment)
        {
            switch (dialect)
            {
                case Dialect.SQLite:
                    return $"{insertSegment}{valuesSegment}{Formatter.NewLine}SELECT last_insert_rowid();";

                case Dialect.SQLServer:
                    var idColumnName = PropertyToColumns[IdentityColumn];
                    var outputClause = $"OUTPUT Inserted.{idColumnName} INTO @InsertedRows";
                    return $"{SQLServerInsertedRowDeclarationClause}{Formatter.NewLine}{insertSegment} {outputClause}{valuesSegment}{Formatter.NewLine}{SQLServerSelectInsertedRowClause}";

                case Dialect.Generic:
                    return $"{insertSegment}{valuesSegment}";
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