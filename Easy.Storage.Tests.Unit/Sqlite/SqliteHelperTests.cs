namespace Easy.Storage.Tests.Unit.SQLite
{
    using System;
    using Easy.Storage.SQLite;
    using NUnit.Framework;
    using Shouldly;

    [TestFixture]
    internal sealed class SQLiteHelperTests
    {
        [Test]
        public void When_getting_data_source_from_an_invalid_sqlite_connection_string()
        {
            Should.Throw<ArgumentException>(() => SQLiteHelper.GetDatabaseFile(null))
                .Message.ShouldBe("String must not be null, empty or whitespace.");

            Should.Throw<ArgumentException>(() => SQLiteHelper.GetDatabaseFile("DataSource"))
                .Message.ShouldBe("Invalid SQLite connection string.");
        }

        [Test]
        public void When_getting_data_source_from_file_based_sqlite_connection_string()
        {
            const string FileConnectionString1 =
                @"data source=""C:\Users\nemo\AppData\Local\Temp\tmpFB82.tmp"";failifmissing=False;pooling=False;binaryguid=True;datetimekind=Utc;datetimeformat=UnixEpoch;journal mode=Wal;synchronous=Off;useutf16encoding=False;read only=False;legacy format=False;page size=4096;cache size=10000";

            var dbFile1 = SQLiteHelper.GetDatabaseFile(FileConnectionString1);
            dbFile1.FullName.ShouldBe(@"C:\Users\nemo\AppData\Local\Temp\tmpFB82.tmp");

            const string FileConnectionString2 =
                @"data source=C:\Users\nemo\AppData\Local\Temp\tmpFB82.tmp;failifmissing=False;pooling=False;binaryguid=True;datetimekind=Utc;datetimeformat=UnixEpoch;journal mode=Wal;synchronous=Off;useutf16encoding=False;read only=False;legacy format=False;page size=4096;cache size=10000";

            var dbFile2 = SQLiteHelper.GetDatabaseFile(FileConnectionString2);
            dbFile2.FullName.ShouldBe(@"C:\Users\nemo\AppData\Local\Temp\tmpFB82.tmp");

            const string FileConnectionString3 = @"data source=SomeFile.db";
            var dbFile3 = SQLiteHelper.GetDatabaseFile(FileConnectionString3);

            dbFile3.FullName.ShouldEndWith(@"SomeFile.db");
        }
    }
}