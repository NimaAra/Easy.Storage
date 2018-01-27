namespace Easy.Storage.Tests.Unit.Models
{
    using Easy.Storage.Common.Attributes;

    internal sealed class AnotherPerson
    {
        [Key]
        [Alias("Id")]
        public long SomeId { get; set; }

        [Alias("Name")]
        public string SomeName { get; set; }

        public int Age { get; set; }

        [Ignore]
        public string Foo { get; set; }
    }
}