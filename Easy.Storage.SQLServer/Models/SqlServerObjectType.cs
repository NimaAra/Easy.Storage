// ReSharper disable InconsistentNaming
// ReSharper disable UnusedMember.Global
namespace Easy.Storage.SQLServer.Models
{
    /// <summary>
    /// Represents every <c>SQL Server</c> object type.
    /// </summary>
    public enum SQLServerObjectType
    {
        /// <summary>
        /// Aggregate function (CLR).
        /// </summary>
        AF,

        /// <summary>
        /// CHECK constraint.
        /// </summary>
        C,

        /// <summary>
        /// Default or DEFAULT constraint.
        /// </summary>
        D,

        /// <summary>
        /// FOREIGN KEY constraint.
        /// </summary>
        F,

        /// <summary>
        /// Log.
        /// </summary>
        L,

        /// <summary>
        /// Scalar function.
        /// </summary>
        FN,

        /// <summary>
        /// Assembly (CLR) scalar-function.
        /// </summary>
        FS,

        /// <summary>
        /// Assembly (CLR) table-valued function.
        /// </summary>
        FT,

        /// <summary>
        /// In-lined table-function.
        /// </summary>
        IF,

        /// <summary>
        /// Internal table.
        /// </summary>
        IT,

        /// <summary>
        /// Stored procedure.
        /// </summary>
        P,

        /// <summary>
        /// Assembly (CLR) stored-procedure.
        /// </summary>
        PC,

        /// <summary>
        /// PRIMARY KEY constraint (type is K).
        /// </summary>
        PK,

        /// <summary>
        /// Replication filter stored procedure.
        /// </summary>
        RF,

        /// <summary>
        /// System table.
        /// </summary>
        S,

        /// <summary>
        /// Synonym.
        /// </summary>
        SN,

        /// <summary>
        /// Service queue.
        /// </summary>
        SQ,

        /// <summary>
        /// Assembly (CLR) DML trigger.
        /// </summary>
        TA,

        /// <summary>
        /// Table function.
        /// </summary>
        TF,

        /// <summary>
        /// SQL DML Trigger.
        /// </summary>
        TR,

        /// <summary>
        /// Table type.
        /// </summary>
        TT,

        /// <summary>
        /// User table.
        /// </summary>
        U,

        /// <summary>
        /// UNIQUE constraint (type is K).
        /// </summary>
        UQ,

        /// <summary>
        /// View.
        /// </summary>
        V,

        /// <summary>
        /// Extended stored procedure.
        /// </summary>
        X
    }
}