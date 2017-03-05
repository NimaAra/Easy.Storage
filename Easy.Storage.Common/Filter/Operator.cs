namespace Easy.Storage.Common.Filter
{
    /// <summary>
    /// Represents different operations supported by the <see cref="Filter{T}"/>.
    /// </summary>
    public enum Operator
    {
        /// <summary>
        /// Equal.
        /// </summary>
        Equal = 0,

        /// <summary>
        /// Not equal.
        /// </summary>
        NotEqual,

        /// <summary>
        /// Greater than.
        /// </summary>
        GreaterThan,

        /// <summary>
        /// Greater than or equal.
        /// </summary>
        GreaterThanOrEqual,

        /// <summary>
        /// Less than.
        /// </summary>
        LessThan,

        /// <summary>
        /// Less than or equal.
        /// </summary>
        LessThanOrEqual
    }
}