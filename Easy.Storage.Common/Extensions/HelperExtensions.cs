namespace Easy.Storage.Common.Extensions
{
    using System.Collections.Generic;
    using Dapper;

    /// <summary>
    /// Provides a set of helpful methods for working with various objects.
    /// </summary>
    internal static class HelperExtensions
    {
        /// <summary>
        /// Converts the given <paramref name="parameters"/> to <see cref="DynamicParameters"/>.
        /// </summary>
        internal static DynamicParameters ToDynamicParameters(this IDictionary<string, object> parameters, object template = null)
        {
            var result = template == null ? new DynamicParameters() : new DynamicParameters(template);
            foreach (var pair in parameters)
            {
                result.Add(pair.Key, pair.Value);
            }
            return result;
        }
    }
}