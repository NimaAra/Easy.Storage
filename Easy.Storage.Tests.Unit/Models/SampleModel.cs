namespace Easy.Storage.Tests.Unit.Models
{
    using System;
    using System.Collections.Generic;
    using Easy.Storage.Common.Attributes;

    internal sealed class SampleModel
    {
        public ulong Id { get; set; }
        public string Text { get; set; }
        public uint Int { get; set; }
        public decimal Decimal { get; set; }
        public double Double { get; set; }
        public float Float { get; set; }
        public bool Flag { get; set; }
        public byte[] Binary { get; set; }


        [Alias("Key")]
        public Guid Guid { get; set; }
        public DateTime DateTime { get; set; }
        public DateTimeOffset DateTimeOffset { get; set; }

        [Ignore]
        public IEnumerable<Person> Composite { get; set; }
    }
}