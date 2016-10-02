namespace Easy.Storage.Common
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using Easy.Common.Extensions;
    using Easy.Storage.Common.Attributes;
    using Easy.Storage.Common.Extensions;

    /// <summary>
    /// Represents information about the table a model should be stored at.
    /// </summary>
    public sealed class Table // [ToDo] Test model with multiple primary keys
    {
        private static readonly ConcurrentDictionary<Type, Table> Cache = new ConcurrentDictionary<Type, Table>();

        internal static Table Get<TItem>()
        {
            return Cache.GetOrAdd(typeof(TItem), theType => new Table(theType));
        }

        internal readonly Dictionary<PropertyInfo, string> PropertyToColumns;
        internal readonly HashSet<PropertyInfo> IdProperties;
        private readonly Dictionary<string, string> _propertyNamesToColumnNames;

        private Table(Type type)
        {
            Name = GetModelName(type);
            var props = type.GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly);
            PropertyToColumns = GetPropertiesToColumnsMappings(props);
            _propertyNamesToColumnNames = PropertyToColumns.ToDictionary(kv => kv.Key.Name, kv => kv.Value);

            IdProperties = new HashSet<PropertyInfo>(props
                .Where(p => p.CustomAttributes.Any(at => at.AttributeType == typeof(PrimaryKeyAttribute))));

            var defaultIdProp = props.SingleOrDefault(p => p.Name == "Id");
            if (!IdProperties.Any())
            {
                if (defaultIdProp != null)
                {
                    IdProperties.Add(defaultIdProp);
                } else
                {
                    throw new InvalidOperationException("The model does not have a default 'Id' property specified or any it's members marked as primary key.");
                }
            }

            var propNames = PropertyToColumns.Keys.Select(p => p.Name);
            var colNames = PropertyToColumns.Values;

            // all properties that are not marked as PrimaryKey
            var propToColsMinusIds = PropertyToColumns.Where(p => !IdProperties.Contains(p.Key)).ToArray();
            var colNamesMinusIds = propToColsMinusIds.Select(kv => kv.Value).ToArray();
            var propNamesMinusIds = propToColsMinusIds.Select(kv => kv.Key.Name).ToArray();

            var columnsMinusIds = string.Join(Formatter.ColumnSeparator, colNamesMinusIds);
            var propertiesMinusIds = string.Join(Formatter.ColumnSeparator, propNamesMinusIds.Select(x => "@" + x));
            var colsAsPropNameAlias = string.Join(Formatter.ColumnSeparator, colNames.Zip(propNames, (col, prop) => $"{col} AS '{prop}'"));
            var idColToIdPropName = string.Join(Formatter.AndClauseSeparator, IdProperties.Select(p => $"{PropertyToColumns[p]} = @{p.Name}"));
            var colEqualPropMinusIds = string.Join(Formatter.ColumnSeparator, colNamesMinusIds.Zip(propNamesMinusIds, (col, propName) => $"{col} = @{propName}"));

            Select =        $"SELECT{Formatter.NewLine}{Formatter.Spacer}{colsAsPropNameAlias}{Formatter.NewLine}FROM {Name}{Formatter.NewLine}WHERE{Formatter.NewLine}{Formatter.Spacer}1 = 1;";
            Insert =        $"INSERT INTO {Name}{Formatter.NewLine}({Formatter.NewLine}{Formatter.Spacer}{columnsMinusIds}{Formatter.NewLine}){Formatter.NewLine}VALUES{Formatter.NewLine}({Formatter.NewLine}{Formatter.Spacer}{propertiesMinusIds}{Formatter.NewLine});";
            UpdateDefault = $"UPDATE {Name} SET{Formatter.NewLine}{Formatter.Spacer}{colEqualPropMinusIds}{Formatter.NewLine}WHERE{Formatter.NewLine}{Formatter.Spacer}{idColToIdPropName};";
            UpdateCustom =  $"UPDATE {Name} SET{Formatter.NewLine}{Formatter.Spacer}{colEqualPropMinusIds}{Formatter.NewLine}WHERE{Formatter.NewLine}{Formatter.Spacer}1 = 1;";
            Delete =        $"DELETE FROM {Name}{Formatter.NewLine}WHERE{Formatter.NewLine}{Formatter.Spacer}1 = 1;";
        }

        /// <summary>
        /// Gets the name by which the model will be stored as.
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// Gets the <c>SELECt</c> query to retrieve the model.
        /// </summary>
        public string Select { get; }

        /// <summary>
        /// Gets the <c>INSERT</c> query to store the model.
        /// </summary>
        public string Insert { get; }

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
            var columnName = GetColumnName(propertyName);

            const string MultipleClause = " IN @Values";
            const string SingleClause = " = @Value";
            
            var subQuery = $"{Formatter.AndClauseSeparator}({columnName} {(single ? SingleClause : MultipleClause)});";
            return baseQuery.Replace(";", subQuery);
        }

        internal string GetColumnName(string propertyName)
        {
            return _propertyNamesToColumnNames[propertyName];
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
    }
}