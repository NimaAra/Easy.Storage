namespace Easy.Storage.Common.Extensions
{
    using Easy.Common.Extensions;

    internal static class StringExtensions
    {
        internal static string GetAsEscapedSQLName(this string name)
        {
            if (name.IsNullOrEmptyOrWhiteSpace()) { return name; }
            if (!name.StartsWith("[") && !name.EndsWith("]"))
            {
                name = string.Concat("[", name, "]");
            }
            return name;
        }

        internal static string GetNameFromEscapedSQLName(this string name)
        {
            if (name.IsNullOrEmptyOrWhiteSpace()) { return name; }
            if (name.StartsWith("[") && name.EndsWith("]"))
            {
                return name.Substring(1, name.Length - 2);
            }
            return name;
        }
    }
}