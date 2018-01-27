namespace Easy.Storage.Tests.Unit.Models
{
    using Easy.Storage.Common.Attributes;

    [Alias("Person")]
    internal sealed class ModelWithKeyButNoIdentity
    {
        [Key(false)]
        public long Id { get; set; }

        public string Name { get; set; }
        public int Age { get; set; }

        [Ignore]
        public string Foo { get; set; }
    }
}