namespace Easy.Storage.SqlServer
{
    /// <summary>
    /// Provides <c>SQL Server</c> specific <c>SQL</c> queries.
    /// </summary>
    public static class SqlServerSql
    {
        /// <summary>
        /// Query to find if a given table exists.
        /// </summary>
        public const string AllObjects = "SELECT " 
                                         + "id AS Id,"
                                         + "name AS Name,"
                                         + "uid AS SchemaId,"
                                         + "parent_obj AS ParentId,"
                                         + "crdate AS CreationDate,"
                                         + "xtype AS Type"
                                         + " FROM sysobjects";
        
        /// <summary>
        /// Query to find if a given table exists.
        /// </summary>
        public const string TableExists = "SELECT 1 FROM sysobjects WHERE name=@tableName AND xtype='U'";

        /// <summary>
        /// Query to get the information relating to a given table.
        /// </summary>
        public const string TableInfo = @"
SELECT 
	typ.name AS TypeName,
	typ.precision AS 'Precision',
	c.TABLE_CATALOG AS 'Database',
	c.TABLE_SCHEMA AS 'Schema',
	c.TABLE_NAME AS TableName,	
	c.COLUMN_NAME AS Name,
	c.ORDINAL_POSITION AS Position,
	CASE WHEN c.CHARACTER_MAXIMUM_LENGTH IS NULL THEN typ.max_length
	ELSE c.CHARACTER_MAXIMUM_LENGTH
	END AS MaximumLength,
    c.NUMERIC_SCALE AS Scale,
	c.IS_NULLABLE AS IsNullable,
	c.COLLATION_NAME AS Collation,
	CASE WHEN cu.COLUMN_NAME IS NULL THEN 0
	ELSE 1
	END AS IsPrimaryKey
FROM INFORMATION_SCHEMA.COLUMNS c
LEFT JOIN 
	(
		SELECT 
			tc.TABLE_CATALOG,
			tc.TABLE_SCHEMA,
			tc.TABLE_NAME,
			ck.COLUMN_NAME
		FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
		JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ck 
			ON tc.TABLE_CATALOG = ck.TABLE_CATALOG
			AND tc.TABLE_SCHEMA = ck.TABLE_SCHEMA
			AND tc.TABLE_NAME = ck.TABLE_NAME
			AND tc.CONSTRAINT_NAME = ck.CONSTRAINT_NAME		
			AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
	) AS cu
	ON c.TABLE_CATALOG = cu.TABLE_CATALOG
	AND c.TABLE_SCHEMA = cu.TABLE_SCHEMA
	AND c.TABLE_NAME = cu.TABLE_NAME
	AND c.COLUMN_NAME = cu.COLUMN_NAME
JOIN sys.types typ ON c.DATA_TYPE = typ.name
WHERE c.TABLE_NAME = @tableName;";
    }
}