namespace Easy.Storage.Common.Attributes
{
    using System;

    /// <summary>
    /// Used to mark a given property as the key and/or identity of the model.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public sealed class KeyAttribute : Attribute
    {
        /// <summary>
        /// Creates an instance of the <see cref="KeyAttribute"/>.
        /// </summary>
        /// <param name="isIdentity">
        /// The flag indicating whether the key acts also as the identity.
        /// </param>
        public KeyAttribute(bool isIdentity = true) => IsIdentity = isIdentity;

        /// <summary>
        /// Gets the flag indicating whether the key acts also as the identity.
        /// </summary>
        public bool IsIdentity { get; }
    }
}