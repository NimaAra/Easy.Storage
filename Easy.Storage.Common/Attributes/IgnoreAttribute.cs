namespace Easy.Storage.Common.Attributes
{
    using System;

    /// <summary>
    /// Used to mark a given property to be ignored from the mapping to the storage.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public sealed class IgnoreAttribute : Attribute { }
}