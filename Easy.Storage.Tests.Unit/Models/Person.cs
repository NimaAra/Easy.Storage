namespace Easy.Storage.Tests.Unit.Models
{
    using Easy.Storage.Common.Attributes;

    internal sealed class Person
    {
        [PrimaryKey]
        public long Id { get; set; }

        public string Name { get; set; }
        public int Age { get; set; }
    }
}