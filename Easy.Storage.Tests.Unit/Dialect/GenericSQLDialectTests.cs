namespace Easy.Storage.Tests.Unit.Dialect
{
    using Easy.Storage.Common;
    using NUnit.Framework;
    using Shouldly;

    [TestFixture]
    internal sealed class GenericSQLDialectTests
    {
        [Test]
        public void When_creating_a_generic_dialect()
        {
            var instanceOne = GenericSQLDialect.Instance;
            var instanceTwo = GenericSQLDialect.Instance;

            instanceOne.ShouldBe(instanceTwo);
            instanceOne.ShouldBeSameAs(instanceTwo);

            GenericSQLDialect.Instance.Type.ShouldBe(DialectType.Generic);
        }
    }
}