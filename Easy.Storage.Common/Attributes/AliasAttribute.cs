namespace Easy.Storage.Common.Attributes
{
    using System;
    using Easy.Common;

    /// <summary>
    /// Used to specify an alias for a table or column.
    /// </summary>
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Property)]
    public sealed class AliasAttribute : Attribute
    {
        /// <summary>
        /// Creates an instance of the <see cref="AliasAttribute"/>.
        /// </summary>
        public AliasAttribute(string name)
        {
            Name = Ensure.NotNullOrEmptyOrWhiteSpace(name, "Alias cannot be null or empty or whitespace");
        }

        /// <summary>
        /// Name of the table or column
        /// </summary>
        public string Name { get; }
    }
}