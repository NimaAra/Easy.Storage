namespace Easy.Storage.Tests.Unit.Sqlite
{
    using System;
    using System.Data;
    using System.IO;
    using System.Threading.Tasks;
    using Easy.Storage.Sqlite;
    using NUnit.Framework;
    using Shouldly;

    [TestFixture]
    internal sealed class SqliteConnectionWrapperTests
    {
        [Test]
        public void When_creating_an_file_sqlite_connection_wrapper()
        {
            var dbFile = Path.GetTempFileName();
            var fileInfo = new FileInfo(dbFile);

            try
            {
                var wrapper = SqliteConnectionBuilder.GetFileConnectionWrapper(fileInfo);

                wrapper.IsInMemory.ShouldBeFalse();
                wrapper.ServerVersion.ShouldStartWith("3.");
                wrapper.State.ShouldBe(ConnectionState.Closed);
                wrapper.ConnectionString.ShouldNotBeNullOrWhiteSpace();
                wrapper.ConnectionString.ShouldContain(dbFile);
                wrapper.ConnectionString.ShouldContain("WAL");
                wrapper.ConnectionTimeout.ShouldBe(15);
                wrapper.Database.ShouldBe("main");
                wrapper.DataSource.ShouldBeNull();
                wrapper.Open();
                wrapper.State.ShouldBe(ConnectionState.Open);
                wrapper.BeginTransaction().ShouldNotBeNull();
                wrapper.BeginTransaction(IsolationLevel.ReadCommitted).ShouldNotBeNull();
                wrapper.BeginDbTransaction().ShouldNotBeNull();

                wrapper.Close();
                wrapper.State.ShouldBe(ConnectionState.Closed);
                wrapper.CloseForce();
                wrapper.State.ShouldBe(ConnectionState.Closed);

                Should.Throw<NotImplementedException>(() => wrapper.ChangeDatabase("Foo"));

                wrapper.CreateCommand().ShouldNotBeNull();

                wrapper.Dispose();
                wrapper.State.ShouldBe(ConnectionState.Closed);
            } finally
            {
                fileInfo.Delete();
            }
        }

        [Test]
        public void When_creating_an_in_memory_sqlite_connection_wrapper()
        {
            var wrapper = SqliteConnectionBuilder.GetInMemoryConnectionWrapper();

            wrapper.ConnectionString.ShouldNotBeNullOrWhiteSpace();
            wrapper.ConnectionString.ShouldContain(":memory:");
            wrapper.IsInMemory.ShouldBeTrue();
            wrapper.ConnectionTimeout.ShouldBe(15);
            wrapper.Database.ShouldBe("main");
            wrapper.DataSource.ShouldBeNull();

            wrapper.State.ShouldBe(ConnectionState.Closed);
            wrapper.Open();
            wrapper.State.ShouldBe(ConnectionState.Open);

            wrapper.Close();
            wrapper.State.ShouldBe(ConnectionState.Open);

            wrapper.CloseForce();
            wrapper.State.ShouldBe(ConnectionState.Closed);

            wrapper.Open();
            wrapper.State.ShouldBe(ConnectionState.Open);

            wrapper.Dispose();
            wrapper.State.ShouldBe(ConnectionState.Open);
        }

        [Test]
        public void When_getting_data_source_from_an_invalid_sqlite_connection_string()
        {
            Should.Throw<ArgumentException>(() => SqliteHelper.GetDatabaseFile(null))
                .Message.ShouldBe("String must not be null, empty or whitespace.");

            Should.Throw<ArgumentException>(() => SqliteHelper.GetDatabaseFile("DataSource"))
                .Message.ShouldBe("Invalid SQLite connection string.");
        }

        [Test]
        public void When_getting_data_source_from_file_based_sqlite_connection_string()
        {
            const string FileConnectionString1 =
                @"data source=""C:\Users\nemo\AppData\Local\Temp\tmpFB82.tmp"";failifmissing=False;pooling=False;binaryguid=True;datetimekind=Utc;datetimeformat=UnixEpoch;journal mode=Wal;synchronous=Off;useutf16encoding=False;read only=False;legacy format=False;page size=4096;cache size=10000";

            var dbFile1 = SqliteHelper.GetDatabaseFile(FileConnectionString1);
            dbFile1.FullName.ShouldBe(@"C:\Users\nemo\AppData\Local\Temp\tmpFB82.tmp");

            const string FileConnectionString2 =
                @"data source=C:\Users\nemo\AppData\Local\Temp\tmpFB82.tmp;failifmissing=False;pooling=False;binaryguid=True;datetimekind=Utc;datetimeformat=UnixEpoch;journal mode=Wal;synchronous=Off;useutf16encoding=False;read only=False;legacy format=False;page size=4096;cache size=10000";

            var dbFile2 = SqliteHelper.GetDatabaseFile(FileConnectionString2);
            dbFile2.FullName.ShouldBe(@"C:\Users\nemo\AppData\Local\Temp\tmpFB82.tmp");

            const string FileConnectionString3 = @"data source=SomeFile.db";
            var dbFile3 = SqliteHelper.GetDatabaseFile(FileConnectionString3);

            dbFile3.FullName.ShouldEndWith(@"SomeFile.db");
        }

        [Test]
        public void When_getting_data_source_from_in_memory_sqlite_connection_string()
        {
            const string FileConnectionString =
                @"data source=:memory:;failifmissing=False;pooling=False;binaryguid=True;datetimekind=Utc;datetimeformat=UnixEpoch;journal mode=Wal;synchronous=Off;useutf16encoding=False;read only=False;legacy format=False;page size=4096;cache size=10000";
            var dbFile = SqliteHelper.GetDatabaseFile(FileConnectionString);
            dbFile.ShouldBeNull("Because the database is in-memory");
        }

        [Test]
        public async Task When_rolling_database_file_is_opened()
        {
            var dbFile = Path.GetTempFileName();
            var fileInfo = new FileInfo(dbFile);
            var dbNameWithoutExtension = Path.GetFileNameWithoutExtension(dbFile);
            Console.WriteLine("DB File: " + dbFile);

            var wrapper = SqliteConnectionBuilder.GetFileConnectionWrapper(fileInfo, TimeSpan.FromSeconds(2));

            wrapper.State.ShouldBe(ConnectionState.Closed);
            wrapper.ConnectionString.ShouldNotBeNullOrWhiteSpace();
            wrapper.ConnectionString.ShouldNotContain(dbFile);
            wrapper.ConnectionString.ShouldContain(Path.Combine(fileInfo.DirectoryName, dbNameWithoutExtension + "_[1]["));
            wrapper.ConnectionString.ShouldContain("WAL");
            wrapper.ConnectionTimeout.ShouldBe(15);
            wrapper.Database.ShouldBe("main");
            wrapper.DataSource.ShouldBeNull();
            wrapper.RollCount.ShouldBe((uint)1);
            wrapper.RollEvery.ShouldBe(TimeSpan.FromSeconds(2));

            wrapper.Open();
            wrapper.State.ShouldBe(ConnectionState.Open);

            wrapper.RollCount.ShouldBe((uint)1);

            wrapper.Close();
            wrapper.State.ShouldBe(ConnectionState.Closed);

            wrapper.RollCount.ShouldBe((uint)1);

            await Task.Delay(TimeSpan.FromSeconds(4)).ConfigureAwait(false);

            wrapper.RollCount.ShouldBe((uint)1);

            wrapper.Open();
            wrapper.State.ShouldBe(ConnectionState.Open);

            wrapper.RollCount.ShouldBe((uint)1);

            wrapper.Dispose();

            var totalDbFiles = Directory.GetFiles(fileInfo.DirectoryName, dbNameWithoutExtension + "*.tmp");
            Array.ForEach(totalDbFiles, Console.WriteLine);
            totalDbFiles.Length.ShouldBe(2);

            Array.ForEach(totalDbFiles, File.Delete);
        }
    }
}