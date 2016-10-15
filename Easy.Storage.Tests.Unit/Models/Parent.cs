namespace Easy.Storage.Tests.Unit.Models
{
    internal class Parent
    {
        public long Id { get; set; }
        public string Name { get; set; }
        public int Age { get; set; }

        private int PrivateProperty { get; set; }
    }
}