namespace Easy.Storage.Tests.Unit.Dialect
{
    using Easy.Storage.Common;
    using Easy.Storage.SQLite;
    using NUnit.Framework;
    using Shouldly;

    [TestFixture]
    // ReSharper disable once InconsistentNaming
    internal sealed class SQLiteDialectTests
    {
        [Test]
        public void When_creating_a_sqlite_dialect()
        {
            var instanceOne = SQLiteDialect.Instance;
            var instanceTwo = SQLiteDialect.Instance;

            instanceOne.ShouldBe(instanceTwo);
            instanceOne.ShouldBeSameAs(instanceTwo);

            SQLiteDialect.Instance.Type.ShouldBe(DialectType.SQLite);
        }
    }
}