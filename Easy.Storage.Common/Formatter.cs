namespace Easy.Storage.Common
{
    using System;

    internal static class Formatter
    {
        internal static readonly string NewLine = Environment.NewLine;
        internal static readonly string Spacer = new string(' ', 4);
        internal static readonly string ColumnSeparator = string.Concat(",", NewLine, Spacer);
        internal static readonly string ColumnSeparatorNoSpace = string.Concat(",", NewLine);
        internal static readonly string AndClauseSeparator = string.Concat(NewLine, "AND", NewLine, Spacer);
        internal static readonly string OrClauseSeparator = string.Concat(NewLine, "OR", NewLine, Spacer);
        internal static readonly string InClauseSeparator = " IN ";
        internal static readonly string NotInClauseSeparator = " NOT IN ";
    }
}