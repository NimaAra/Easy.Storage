namespace Easy.Storage.Tests.Unit.Models
{
    using Easy.Storage.Common.Attributes;

    internal sealed class Child : Parent
    {
        public string Toy { get; set; }

        [Alias("PetName")]
        public string Pet { get; set; }
    }
}