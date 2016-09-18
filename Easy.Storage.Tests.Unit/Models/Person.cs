namespace Easy.Storage.Tests.Unit.Models
{
    using Easy.Storage.Common.Attributes;

    internal sealed class Person
    {
        [PrimaryKey]
        public ulong Id { get; set; }

        public string Name { get; set; }
        public uint Age { get; set; }
    }
}