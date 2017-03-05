namespace Easy.Storage.Common.Extensions
{
    using System;
    using System.Collections.Generic;
    using Easy.Storage.Common.Filter;

    /// <summary>
    /// Provides a set of helper methods for working with <see cref="Enum"/>.
    /// </summary>
    public static class EnumExtensions
    {
        private static readonly Dictionary<Operator, string> Operands = new Dictionary<Operator, string>
        {
            {Operator.Equal, "="},
            {Operator.NotEqual, "<>"},
            {Operator.GreaterThan, ">"},
            {Operator.GreaterThanOrEqual, ">="},
            {Operator.LessThan, "<"},
            {Operator.LessThanOrEqual, "<="},
        };

        /// <summary>
        /// Returns the string representation of the given <paramref name="operator"/>.
        /// </summary>
        internal static string AsString(this Operator @operator)
        {
            return Operands[@operator];
        }
    }
}