namespace Easy.Storage.Common.Attributes
{
    using System;

    /// <summary>
    /// Used to mark a given property to be allowed to have its value stored as null.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public sealed class NullableAttribute : Attribute { }
}