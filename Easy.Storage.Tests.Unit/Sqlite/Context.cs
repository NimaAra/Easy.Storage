namespace Easy.Storage.Tests.Unit.SQLite
{
    internal class Context
    {
        protected static string TableQuery => "CREATE TABLE IF NOT EXISTS [Person] (" +
                                              "Id INTEGER PRIMARY KEY NOT NULL," +
                                              "Name TEXT NOT NULL," +
                                              "Age INTEGER NOT NULL);";

        protected static string TableQueryWithNoIdentity => "CREATE TABLE IF NOT EXISTS [Person] (" +
                                              "Id INTEGER NOT NULL," +
                                              "Name TEXT NOT NULL," +
                                              "Age INTEGER NOT NULL);";

        protected static string ViewQuery => "CREATE VIEW IF NOT EXISTS [Person_view] AS " +
                                             "SELECT * FROM [Person];";

        protected static string TriggerQuery => "CREATE TRIGGER IF NOT EXISTS Person_bu BEFORE UPDATE ON Person BEGIN " +
                                                "DELETE FROM Person WHERE 1<>1; " +
                                                "END";

        protected static string IndexQuery => "CREATE INDEX IF NOT EXISTS Person_idx ON [Person](Name);";
    }
}