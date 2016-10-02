// ReSharper disable UnassignedGetOnlyAutoProperty
namespace Easy.Storage.SqlServer.Models
{
    using System;

    /// <summary>
    /// Represents objects defined in a <c>SQL Server</c> database.
    /// </summary>
    // ReSharper disable once ClassNeverInstantiated.Global
    public sealed class SqlServerObject
    {
        /// <summary>
        /// Gets the object Id.
        /// </summary>
        public uint Id { get; }

        /// <summary>
        /// Gets the object name.
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// Gets the schema Id of the owner of the object.
        /// </summary>
        public ushort SchemaId { get; }

        /// <summary>
        /// Gets the object Id of the parent object.
        /// <example>
        /// The table Id if it is a trigger or constraint.
        /// </example>
        /// </summary>
        public uint ParentId { get; }

        /// <summary>
        /// Gets the <see cref="DateTime"/> the object was created.
        /// </summary>
        public DateTime CreationDate { get; }

        /// <summary>
        /// Gets the type of the object.
        /// </summary>
        public SqlServerObjectType Type { get; }
    }
}