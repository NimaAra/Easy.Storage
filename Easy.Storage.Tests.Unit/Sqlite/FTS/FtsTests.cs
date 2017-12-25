namespace Easy.Storage.Tests.Unit.SQLite.FTS
{
    using System.Data;
    using System.Data.SQLite;
    using System.Linq;
    using System.Threading.Tasks;
    using Easy.Storage.Common.Extensions;
    using Easy.Storage.SQLite;
    using Easy.Storage.SQLite.Connections;
    using Easy.Storage.SQLite.Extensions;
    using Easy.Storage.SQLite.FTS;
    using Easy.Storage.SQLite.Models;
    using NUnit.Framework;
    using Shouldly;

    [TestFixture]
    // ReSharper disable once InconsistentNaming
    internal sealed class FTSTests
    {
        [Test]
        public void When_searching_for_non_existing_table()
        {
            using (var conn = new SQLiteInMemoryConnection())
            {
                var selectAllTerm = Term<FTSContext.Log>.All;
                Should.Throw<SQLiteException>(async () => await conn.Search(selectAllTerm))
                    .Message.ShouldBe("SQL logic error\r\nno such table: Log");
            }
        }

        [Test]
        public async Task When_searching_for_all_records_in_empty_table()
        {
            using (var conn = new SQLiteInMemoryConnection())
            {
                await Given_a_logTtable_and_an_ftsTable(conn);

                var selectAllTerm = Term<FTSContext.Log>.All;
                var result = await conn.Search(selectAllTerm);
                result.ShouldBeEmpty();
            }
        }

        [Test]
        public async Task When_searching_for_all_records_in_a_table()
        {
            using (var conn = new SQLiteInMemoryConnection())
            {
                await Given_a_logTtable_and_an_ftsTable(conn);

                var logs = new[]
                {
                    new FTSContext.Log {Level = FTSContext.Level.Debug, Message = "There is a Cat"},
                    new FTSContext.Log {Level = FTSContext.Level.Debug, Message = "There is a Dog"},
                    new FTSContext.Log {Level = FTSContext.Level.Debug, Message = "There is a Cat and a Dog"}
                };

                var repo = conn.GetStorageContext<FTSContext.Log>(SQLiteDialect.Instance);
                await repo.Insert(logs);

                var selectAllTerm = Term<FTSContext.Log>.All;
                var result = (await conn.Search(selectAllTerm)).ToArray();
                result.Length.ShouldBe(3);
                result[0].Level.ShouldBe(logs[0].Level);
                result[0].Message.ShouldBe(logs[0].Message);

                result[1].Level.ShouldBe(logs[1].Level);
                result[1].Message.ShouldBe(logs[1].Message);
                       
                result[1].Level.ShouldBe(logs[1].Level);
                result[1].Message.ShouldBe(logs[1].Message);

                result[2].Level.ShouldBe(logs[2].Level);
                result[2].Message.ShouldBe(logs[2].Message);
            }
        }

        [Test]
        public async Task When_searching_for_all_records_in_a_table_and_matching_any_of_the_given_log_level()
        {
            using (var conn = new SQLiteInMemoryConnection())
            {
                await Given_a_logTtable_and_an_ftsTable(conn);

                var logs = new[]
                {
                    new FTSContext.Log {Level = FTSContext.Level.Debug, Message = "There is a Cat"},
                    new FTSContext.Log {Level = FTSContext.Level.Info, Message = "There is a Dog"},
                    new FTSContext.Log {Level = FTSContext.Level.Warn, Message = "There is a Cat and a Dog"}
                };

                var repo = conn.GetStorageContext<FTSContext.Log>(SQLiteDialect.Instance);
                await repo.Insert(logs);

                var term = Term<FTSContext.Log>.All
                    .And(Match.Any, l => l.Level, FTSContext.Level.Debug, FTSContext.Level.Info);

                var result = (await conn.Search(term)).ToArray();
                result.Length.ShouldBe(2);
                result[0].Level.ShouldBe(logs[0].Level);
                result[0].Message.ShouldBe(logs[0].Message);

                result[1].Level.ShouldBe(logs[1].Level);
                result[1].Message.ShouldBe(logs[1].Message);

                result[1].Level.ShouldBe(logs[1].Level);
                result[1].Message.ShouldBe(logs[1].Message);
            }
        }

        [Test]
        public async Task When_searching_for_all_records_in_a_table_and_matching_all_of_the_given_keywords()
        {
            using (var conn = new SQLiteInMemoryConnection())
            {
                await Given_a_logTtable_and_an_ftsTable(conn);

                var logs = new[]
                {
                    new FTSContext.Log {Level = FTSContext.Level.Debug, Message = "There is a Cat"},
                    new FTSContext.Log {Level = FTSContext.Level.Info, Message = "There is a Dog"},
                    new FTSContext.Log {Level = FTSContext.Level.Warn, Message = "There is a Cat and a Dog"}
                };

                var repo = conn.GetStorageContext<FTSContext.Log>(SQLiteDialect.Instance);
                await repo.Insert(logs);

                var term = Term<FTSContext.Log>.All
                    .And(Match.All, l => l.Message, "Cat", "Dog");

                var result = (await conn.Search(term)).ToArray();
                result.Length.ShouldBe(1);
                result[0].Level.ShouldBe(logs[2].Level);
                result[0].Message.ShouldBe(logs[2].Message);
            }
        }

        [Test]
        public async Task When_searching_for_all_records_in_a_table_and_matching_any_of_the_given_keywords()
        {
            using (var conn = new SQLiteInMemoryConnection())
            {
                await Given_a_logTtable_and_an_ftsTable(conn);

                var logs = new[]
                {
                    new FTSContext.Log {Level = FTSContext.Level.Debug, Message = "There is a Cat"},
                    new FTSContext.Log {Level = FTSContext.Level.Debug, Message = "There is a dog"},
                    new FTSContext.Log {Level = FTSContext.Level.Info, Message = "There is a big cat"},
                    new FTSContext.Log {Level = FTSContext.Level.Warn, Message = "There is a very big cat"}
                };

                var repo = conn.GetStorageContext<FTSContext.Log>(SQLiteDialect.Instance);
                await repo.Insert(logs);

                var term = Term<FTSContext.Log>.All
                    .And(Match.Any, l => l.Message, "big", "Dog");

                var result = (await conn.Search(term)).ToArray();
                result.Length.ShouldBe(3);
                result[0].Level.ShouldBe(logs[1].Level);
                result[0].Message.ShouldBe(logs[1].Message);

                result[1].Level.ShouldBe(logs[2].Level);
                result[1].Message.ShouldBe(logs[2].Message);

                result[2].Level.ShouldBe(logs[3].Level);
                result[2].Message.ShouldBe(logs[3].Message);
            }
        }

        [Test]
        public async Task When_searching_for_all_records_in_a_table_not_matching_all_of_the_given_keywords()
        {
            using (var conn = new SQLiteInMemoryConnection())
            {
                await Given_a_logTtable_and_an_ftsTable(conn);

                var logs = new[]
                {
                    new FTSContext.Log {Level = FTSContext.Level.Debug, Message = "There is a Cat"},
                    new FTSContext.Log {Level = FTSContext.Level.Info, Message = "There is a Dog"},
                    new FTSContext.Log {Level = FTSContext.Level.Warn, Message = "There is a Cat and a Dog"},
                    new FTSContext.Log {Level = FTSContext.Level.Warn, Message = "There is a Parrot"}
                };

                var repo = conn.GetStorageContext<FTSContext.Log>(SQLiteDialect.Instance);
                await repo.Insert(logs);

                var term = Term<FTSContext.Log>.All
                    .AndNot(Match.All, l => l.Message, "Cat", "Dog");

                var result = (await conn.Search(term)).ToArray();
                result.Length.ShouldBe(3);
                result[0].Level.ShouldBe(logs[0].Level);
                result[0].Message.ShouldBe(logs[0].Message);

                result[1].Level.ShouldBe(logs[1].Level);
                result[1].Message.ShouldBe(logs[1].Message);

                result[2].Level.ShouldBe(logs[3].Level);
                result[2].Message.ShouldBe(logs[3].Message);
            }
        }

        [Test]
        public async Task When_searching_for_all_records_in_a_table_not_matching_any_of_the_given_keywords()
        {
            using (var conn = new SQLiteInMemoryConnection())
            {
                await Given_a_logTtable_and_an_ftsTable(conn);

                var logs = new[]
                {
                    new FTSContext.Log {Level = FTSContext.Level.Debug, Message = "There is a Cat"},
                    new FTSContext.Log {Level = FTSContext.Level.Info, Message = "There is a Dog"},
                    new FTSContext.Log {Level = FTSContext.Level.Warn, Message = "There is a Cat and a Dog"},
                    new FTSContext.Log {Level = FTSContext.Level.Warn, Message = "There is a Parrot"}
                };

                var repo = conn.GetStorageContext<FTSContext.Log>(SQLiteDialect.Instance);
                await repo.Insert(logs);

                var term = Term<FTSContext.Log>.All
                    .AndNot(Match.Any, l => l.Message, "Cat", "Dog");

                var result = (await conn.Search(term)).ToArray();
                result.Length.ShouldBe(1);
                
                result[0].Level.ShouldBe(logs[3].Level);
                result[0].Message.ShouldBe(logs[3].Message);
            }
        }

        [Test]
        public async Task When_searching_for_all_records_in_a_table_matching_multiple_keywords()
        {
            using (var conn = new SQLiteInMemoryConnection())
            {
                await Given_a_logTtable_and_an_ftsTable(conn);

                var logs = new[]
                {
                    new FTSContext.Log {Level = FTSContext.Level.Debug, Message = "Linux can be fun"},
                    new FTSContext.Log {Level = FTSContext.Level.Info, Message = "Windows and linux are two operating systems"},
                    new FTSContext.Log {Level = FTSContext.Level.Warn, Message = "linux is a Unix like operating system"},
                    new FTSContext.Log {Level = FTSContext.Level.Fatal, Message = "software development in Mac OS operating system may be fun!"}
                };

                var repo = conn.GetStorageContext<FTSContext.Log>(SQLiteDialect.Instance);
                await repo.Insert(logs);

                var term = Term<FTSContext.Log>.All;

                var result = (await conn.Search(term)).ToArray();
                result.Length.ShouldBe(4);

                term.AndNot(Match.Any, l => l.Message, "Mac Os");

                result = (await conn.Search(term)).ToArray();
                result.Length.ShouldBe(3);

                result[0].Level.ShouldBe(logs[0].Level);
                result[0].Message.ShouldBe(logs[0].Message);

                result[1].Level.ShouldBe(logs[1].Level);
                result[1].Message.ShouldBe(logs[1].Message);

                result[2].Level.ShouldBe(logs[2].Level);
                result[2].Message.ShouldBe(logs[2].Message);

                term.Clear();

                term
                    .AndNot(Match.Any, l => l.Message, "Mac Os")
                    .And(Match.Any, l => l.Message, "operating system", "fun");
                result = (await conn.Search(term)).ToArray();
                result.Length.ShouldBe(2);

                result[0].Level.ShouldBe(logs[0].Level);
                result[0].Message.ShouldBe(logs[0].Message);

                result[1].Level.ShouldBe(logs[2].Level);
                result[1].Message.ShouldBe(logs[2].Message);

                term.Clear();

                term
                    .AndNot(Match.Any, l => l.Message, "Mac Os")
                    .And(Match.Any, l => l.Message, "operating system*", "fun");
                result = (await conn.Search(term)).ToArray();
                result.Length.ShouldBe(3);

                result[0].Level.ShouldBe(logs[0].Level);
                result[0].Message.ShouldBe(logs[0].Message);

                result[1].Level.ShouldBe(logs[1].Level);
                result[1].Message.ShouldBe(logs[1].Message);

                result[2].Level.ShouldBe(logs[2].Level);
                result[2].Message.ShouldBe(logs[2].Message);
            }
        }

        [Test]
        public async Task When_searching_for_records_in_a_table_for_all_of_the_supplied_keywords()
        {
            using (var conn = new SQLiteInMemoryConnection())
            {
                await Given_a_logTtable_and_an_ftsTable(conn);

                var logs = new[]
                {
                    new FTSContext.Log {Level = FTSContext.Level.Debug, Message = "John is playing football"},
                    new FTSContext.Log {Level = FTSContext.Level.Info, Message = "Mary is playing football"},
                    new FTSContext.Log {Level = FTSContext.Level.Warn, Message = "Someone is playing football"},
                    new FTSContext.Log {Level = FTSContext.Level.Warn, Message = "John with Mary are playing football"},
                    new FTSContext.Log {Level = FTSContext.Level.Warn, Message = "football is awesome"}
                };

                var repo = conn.GetStorageContext<FTSContext.Log>(SQLiteDialect.Instance);
                await repo.Insert(logs);

                var term = Term<FTSContext.Log>.All.And(Match.All, l => l.Message, "John", "Mary", "football");

                var result1 = (await conn.Search(term)).ToArray();
                result1.Length.ShouldBe(1);

                result1[0].Level.ShouldBe(logs[3].Level);
                result1[0].Message.ShouldBe(logs[3].Message);

                term.Clear();
                term.And(Match.All, l => l.Message, "play*", "football");

                var result2 = (await conn.Search(term)).ToArray();
                result2.Length.ShouldBe(4);

                result2[0].Level.ShouldBe(logs[0].Level);
                result2[0].Message.ShouldBe(logs[0].Message);

                result2[1].Level.ShouldBe(logs[1].Level);
                result2[1].Message.ShouldBe(logs[1].Message);

                result2[2].Level.ShouldBe(logs[2].Level);
                result2[2].Message.ShouldBe(logs[2].Message);

                result2[3].Level.ShouldBe(logs[3].Level);
                result2[3].Message.ShouldBe(logs[3].Message);
            }
        }

        [Test]
        public async Task When_searching_for_records_in_a_table_for_any_of_the_supplied_keywords()
        {
            using (var conn = new SQLiteInMemoryConnection())
            {
                await Given_a_logTtable_and_an_ftsTable(conn);

                var logs = new[]
                {
                    new FTSContext.Log {Level = FTSContext.Level.Debug, Message = "John is playing football"},
                    new FTSContext.Log {Level = FTSContext.Level.Info, Message = "Mary is playing football"},
                    new FTSContext.Log {Level = FTSContext.Level.Warn, Message = "Someone is playing football"},
                    new FTSContext.Log {Level = FTSContext.Level.Warn, Message = "John with Mary are playing football"},
                    new FTSContext.Log {Level = FTSContext.Level.Warn, Message = "football is awesome"}
                };

                var repo = conn.GetStorageContext<FTSContext.Log>(SQLiteDialect.Instance);
                await repo.Insert(logs);

                var term = Term<FTSContext.Log>.All.And(Match.Any, l => l.Message, "John", "Mary", "football");

                var result1 = (await conn.Search(term)).ToArray();
                result1.Length.ShouldBe(5);

                term.Clear();
                term.And(Match.Any, l => l.Message, "someone", "mary", "awe*");

                var result2 = (await conn.Search(term)).ToArray();
                result2.Length.ShouldBe(4);

                result2[0].Level.ShouldBe(logs[1].Level);
                result2[0].Message.ShouldBe(logs[1].Message);

                result2[1].Level.ShouldBe(logs[2].Level);
                result2[1].Message.ShouldBe(logs[2].Message);

                result2[2].Level.ShouldBe(logs[3].Level);
                result2[2].Message.ShouldBe(logs[3].Message);

                result2[3].Level.ShouldBe(logs[4].Level);
                result2[3].Message.ShouldBe(logs[4].Message);
            }
        }

        private static async Task Given_a_logTtable_and_an_ftsTable(IDbConnection connection)
        {
            var tableSql = SQLiteSQLGenerator.Table<FTSContext.Log>();
            await connection.ExecuteAsync(tableSql);
            var ftsTableSql = SQLiteSQLGenerator.FTSTable<FTSContext.Log>(FTSTableType.ExternalContent);
            await connection.ExecuteAsync(ftsTableSql);
        }
    }
}