namespace Easy.Storage.Common.Extensions
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Provides a set of helper methods for working with <see cref="Enum"/>.
    /// </summary>
    public static class EnumExtensions
    {
        private static readonly Dictionary<Operand, string> Operands = new Dictionary<Operand, string>
        {
            {Operand.Equal, "="},
            {Operand.NotEqual, "<>"},
            {Operand.GreaterThan, ">"},
            {Operand.GreaterThanOrEqual, ">="},
            {Operand.LessThan, "<"},
            {Operand.LessThanOrEqual, "<="},
        };

        /// <summary>
        /// Returns the string representation of the given <paramref name="operand"/>.
        /// </summary>
        internal static string OperandAsStr(this Operand operand)
        {
            return Operands[operand];
        }
    }
}