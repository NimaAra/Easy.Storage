namespace Easy.Storage.Common.Extensions
{
    using System.Collections.Generic;
    using System.Linq;
    using Dapper;

    /// <summary>
    /// Provides a set of helpful methods for working with various objects.
    /// </summary>
    public static class HelperExtensions
    {
        /// <summary>
        /// Converts the given <paramref name="parameters"/> to <see cref="DynamicParameters"/>.
        /// <param name="parameters">The parameters to be converted to <see cref="DynamicParameters"/>.</param>
        /// <param name="template">Can be an anonymous type or a <see cref="DynamicParameters"/> bag.</param>
        /// </summary>
        public static DynamicParameters ToDynamicParameters(
            this IReadOnlyDictionary<string, object> parameters, object template = null)
        {
            var result = template == null ? new DynamicParameters() : new DynamicParameters(template);
            foreach (var pair in parameters)
            {
                result.Add(pair.Key, pair.Value);
            }
            return result;
        }

        internal static IList<T> SpeculativeToList<T>(this IEnumerable<T> source) 
            => source as IList<T> ?? source.ToList();
    }
}