namespace Easy.Storage.Tests.Unit.SQLite.SQLiteFunction
{
    using System;
    using System.Linq;
    using System.Threading.Tasks;
    using Easy.Storage.Common.Extensions;
    using Easy.Storage.SQLite.Connections;
    using Easy.Storage.SQLite.Functions;
    using NUnit.Framework;
    using Shouldly;

    [TestFixture]
    // ReSharper disable once InconsistentNaming
    internal sealed class SQLiteFunctionTests
    {
        [Test]
        public void When_binding_function_to_a_closed_connection()
        {
            using (var conn = new SQLiteInMemoryConnection())
            {
                var func = new SQLiteScalarFunction("Foo", 1, args => null);
                Should.Throw<InvalidOperationException>(() => conn.BindFunction(func))
                    .Message.ShouldBe("Cannot bind a function to a closed.");
            }
        }

        [Test]
        public async Task When_binding_a_scalar_function_to_an_opened_connection()
        {
            using (var conn = new SQLiteInMemoryConnection())
            {
                string receivedArg = null;
                var func = new SQLiteScalarFunction("Funky", 1, args =>
                {
                    receivedArg = args[0].ToString();
                    return "bar";
                });

                conn.Open();
                conn.BindFunction(func);

                var foo = (await conn.QueryAsync<string>("SELECT \"foo\"")).First();
                foo.ShouldBe("foo");

                var bar = (await conn.QueryAsync<string>("SELECT Funky(\"foo\")")).First();
                bar.ShouldBe("bar");
                receivedArg.ShouldBe("foo");
            }
        }

        [Test]
        public async Task When_binding_an_aggregate_function_to_an_opened_connection()
        {
            using (var conn = new SQLiteInMemoryConnection())
            {
                conn.Open();
                await conn.ExecuteAsync("CREATE TABLE Numbers(No INTEGER NOT NULL);");
                await conn.ExecuteAsync("INSERT INTO Numbers VALUES (1), (2), (3), (4);");

                const int InitState = 0;
                Func<object[], int, object, object> step = (objects, i, state) =>
                {
                    var newNo = Convert.ToInt32(objects[0]);
                    return (int)state + newNo;
                };
                Func<object, object> final = finalState => finalState;

                conn.BindFunction(new SQLiteAggregateFunction("FunkySum", 1, InitState, step, final));

                var funkySum = await conn.ExecuteScalarAsync<long>("SELECT FunkySum(No) FROM Numbers;");
                funkySum.ShouldBe(10);
            }
        }

        [Test]
        public async Task When_binding_a_collation_function_to_an_opened_connection_query()
        {
            using (var conn = new SQLiteInMemoryConnection())
            {
                conn.Open();
                await conn.ExecuteAsync("CREATE TABLE Numbers(No TEXT NOT NULL);");
                await conn.ExecuteAsync("INSERT INTO Numbers VALUES (1), (2), (3), (4);");

                Func<string, string, int> compare = (one, two) => 1;

                var noCollation = await conn.QueryAsync<string>("SELECT No FROM Numbers ORDER BY No ASC;");
                noCollation.ShouldBe(new[] { "1", "2", "3", "4" });

                conn.BindFunction(new SQLiteCollationFunction("FunkyCompare", compare));

                var funkySum = await conn.QueryAsync<string>("SELECT No FROM Numbers ORDER BY No COLLATE FunkyCompare ASC;");
                funkySum.ShouldBe(new[] {"4", "3", "2", "1"});
            }
        }

        [Test]
        public async Task When_binding_a_collation_function_to_an_opened_connection_table()
        {
            using (var conn = new SQLiteInMemoryConnection())
            {
                conn.Open();
                Func<string, string, int> compare = (one, two) => 1;
                conn.BindFunction(new SQLiteCollationFunction("FunkyCompare", compare));

                await conn.ExecuteAsync("CREATE TABLE Numbers(No TEXT NOT NULL COLLATE FunkyCompare);");
                await conn.ExecuteAsync("INSERT INTO Numbers VALUES (1), (2), (3), (4);");

                var defaultCollation = await conn.QueryAsync<string>("SELECT No FROM Numbers ORDER BY No ASC;");
                defaultCollation.ShouldBe(new[] { "4", "3", "2", "1" });
            }
        }
    }
}