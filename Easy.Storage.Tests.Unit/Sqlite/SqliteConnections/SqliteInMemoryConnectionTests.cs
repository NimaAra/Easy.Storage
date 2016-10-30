// ReSharper disable ObjectCreationAsStatement
namespace Easy.Storage.Tests.Unit.Sqlite.SQLiteConnections
{
    using System;
    using System.Data;
    using System.IO;
    using Easy.Storage.Sqlite;
    using Easy.Storage.Sqlite.Connections;
    using NUnit.Framework;
    using Shouldly;

    [TestFixture]
    internal sealed class SqliteInMemoryConnectionTests
    {
        [Test]
        public void When_creating_connection()
        {
            using (var conn = new SqliteInMemoryConnection())
            {
                conn.ConnectionString.ShouldBe("Data Source=:memory:");
                conn.ConnectionTimeout.ShouldBe(15);
                conn.DataSource.ShouldBeNull();
                conn.Database.ShouldBe("main");
                conn.ServerVersion.ShouldBe("3.14.2");
                conn.State.ShouldBe(ConnectionState.Closed);
            }
        }

        [Test]
        public void When_creating_connection_with_valid_connection_string()
        {
            using (var conn = new SqliteInMemoryConnection(SqliteConnectionStringBuilder.GetInMemoryConnectionString()))
            {
                conn.ConnectionString.ShouldBe("Data Source=:memory:");
                conn.ConnectionTimeout.ShouldBe(15);
                conn.DataSource.ShouldBeNull();
                conn.Database.ShouldBe("main");
                conn.ServerVersion.ShouldBe("3.14.2");
                conn.State.ShouldBe(ConnectionState.Closed);
            }
        }

        [Test]
        public void When_creating_connection_with_invalid_connection_string()
        {
            Should.Throw<ArgumentException>(() => new SqliteInMemoryConnection("foo"))
                .Message.ShouldBe("Not a valid SQLite memory connection-string.");
        }

        [Test]
        public void When_creating_connection_with_null_empty_and_whitespace_connection_string()
        {
            Should.Throw<ArgumentException>(() => new SqliteInMemoryConnection(null))
                .Message.ShouldBe("Connection string cannot be null, empty or whitespace.");

            Should.Throw<ArgumentException>(() => new SqliteInMemoryConnection(string.Empty))
                .Message.ShouldBe("Connection string cannot be null, empty or whitespace.");

            Should.Throw<ArgumentException>(() => new SqliteInMemoryConnection(" "))
                .Message.ShouldBe("Connection string cannot be null, empty or whitespace.");
        }

        [Test]
        public void When_creating_connection_with_non_memory_connection_string()
        {
            var fileConnectionString = SqliteConnectionStringBuilder.GetFileConnectionString(new FileInfo("SomeFile.db"));
            Should.Throw<ArgumentException>(() => new SqliteInMemoryConnection(fileConnectionString))
                .Message.ShouldBe("Not a valid SQLite memory connection-string.");
        }

        [Test]
        public void When_changing_connection_state()
        {
            var conn = new SqliteInMemoryConnection();
            conn.State.ShouldBe(ConnectionState.Closed);

            conn.Open();
            conn.State.ShouldBe(ConnectionState.Open);

            conn.Close();
            conn.State.ShouldBe(ConnectionState.Open);

            conn.Dispose();

            Should.Throw<ObjectDisposedException>(() => conn.State.ShouldBe(ConnectionState.Closed))
                .Message.ShouldBe("Cannot access a disposed object.\r\nObject name: 'SQLiteConnection'.");
        }
    }
}