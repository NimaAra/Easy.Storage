// ReSharper disable ObjectCreationAsStatement
namespace Easy.Storage.Tests.Unit.SQLite.SQLiteConnections
{
    using System;
    using System.Data;
    using System.IO;
    using Easy.Storage.SQLite;
    using Easy.Storage.SQLite.Connections;
    using NUnit.Framework;
    using Shouldly;

    [TestFixture]
    // ReSharper disable once InconsistentNaming
    internal sealed class SQLiteInMemoryConnectionTests
    {
        [Test]
        public void When_creating_connection()
        {
            using (var conn = new SQLiteInMemoryConnection())
            {
                conn.ConnectionString.ShouldBe("Data Source=:memory:");
                conn.ConnectionTimeout.ShouldBe(15);
                conn.DataSource.ShouldBeNull();
                conn.Database.ShouldBe("main");
                conn.ServerVersion.ShouldBe("3.21.0");
                conn.State.ShouldBe(ConnectionState.Closed);
            }
        }

        [Test]
        public void When_creating_connection_with_valid_connection_string()
        {
            using (var conn = new SQLiteInMemoryConnection(SQLiteConnectionStringProvider.GetInMemoryConnectionString()))
            {
                conn.ConnectionString.ShouldBe("Data Source=:memory:");
                conn.ConnectionTimeout.ShouldBe(15);
                conn.DataSource.ShouldBeNull();
                conn.Database.ShouldBe("main");
                conn.ServerVersion.ShouldBe("3.21.0");
                conn.State.ShouldBe(ConnectionState.Closed);
            }
        }

        [Test]
        public void When_creating_connection_with_invalid_connection_string()
        {
            Should.Throw<ArgumentException>(() => new SQLiteInMemoryConnection("foo"))
                .Message.ShouldBe("Not a valid SQLite memory connection-string.");
        }

        [Test]
        public void When_creating_connection_with_null_empty_and_whitespace_connection_string()
        {
            Should.Throw<ArgumentException>(() => new SQLiteInMemoryConnection(null))
                .Message.ShouldBe("Connection string cannot be null, empty or whitespace.");

            Should.Throw<ArgumentException>(() => new SQLiteInMemoryConnection(string.Empty))
                .Message.ShouldBe("Connection string cannot be null, empty or whitespace.");

            Should.Throw<ArgumentException>(() => new SQLiteInMemoryConnection(" "))
                .Message.ShouldBe("Connection string cannot be null, empty or whitespace.");
        }

        [Test]
        public void When_creating_connection_with_non_memory_connection_string()
        {
            var fileConnectionString = SQLiteConnectionStringProvider.GetFileConnectionString(new FileInfo("SomeFile.db"));
            Should.Throw<ArgumentException>(() => new SQLiteInMemoryConnection(fileConnectionString))
                .Message.ShouldBe("Not a valid SQLite memory connection-string.");
        }

        [Test]
        public void When_changing_connection_state()
        {
            var conn = new SQLiteInMemoryConnection();
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