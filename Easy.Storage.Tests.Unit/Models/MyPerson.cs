namespace Easy.Storage.Tests.Unit.Models
{
    using Easy.Storage.Common.Attributes;

    [Alias("Person")]
    internal sealed class MyPerson
    {
        [PrimaryKey]
        [Alias("Id")]
        public long SomeId { get; set; }

        [Alias("Name")]
        public string SomeName { get; set; }

        public int Age { get; set; }
    }
}