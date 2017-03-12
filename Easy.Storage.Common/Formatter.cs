namespace Easy.Storage.Common
{
    using System;

    internal static class Formatter
    {
        internal static readonly string NewLine = Environment.NewLine;
        internal static readonly string Spacer = new string(' ', 4);
        internal static readonly string ColumnSeparator = $",{NewLine}{Spacer}";
        internal static readonly string ColumnSeparatorNoSpace = $",{NewLine}";
        internal static readonly string AndClauseSeparator = $"{NewLine}AND{NewLine}{Spacer}";
        internal static readonly string OrClauseSeparator = $"{NewLine}OR{NewLine}{Spacer}";
        internal static readonly string InClauseSeparator = " IN ";
        internal static readonly string NotInClauseSeparator = " NOT IN ";
    }
}