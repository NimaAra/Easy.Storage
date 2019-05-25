namespace Easy.Storage.Tests.Unit.Models
{
    using Easy.Storage.Common.Attributes;

    [Alias("Person")]
    internal sealed class PersonWithType : Person
    {
        public SomeType Type { get; set; }
    }

    internal enum SomeType
    {
        TypeA = 0,
        TypeB
    }
}