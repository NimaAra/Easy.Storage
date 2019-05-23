namespace Easy.Storage.Common.Filter
{
    /// <summary>
    /// Represents different operations supported by the <see cref="Filter{T}"/>.
    /// </summary>
    public enum Operator
    {
        /// <summary>
        /// Is.
        /// </summary>
        Is = 0,

        /// <summary>
        /// Not equal.
        /// </summary>
        IsNot,

        /// <summary>
        /// Greater than.
        /// </summary>
        GreaterThan,

        /// <summary>
        /// Greater than or is.
        /// </summary>
        GreaterThanOrIs,

        /// <summary>
        /// Less than.
        /// </summary>
        LessThan,

        /// <summary>
        /// Less than or is.
        /// </summary>
        LessThanOrIs
    }
}