namespace Easy.Storage.Common.Extensions
{
    using System;
    using System.Linq.Expressions;

    /// <summary>
    /// Provides a set of helpful methods for <see cref="Expression"/>.
    /// </summary>
    internal static class ExpressionExtensions
    {
        /// <summary>
        /// Returns the name of the property specified by the <paramref name="selector"/>.
        /// </summary>
        /// <typeparam name="T">The type of the model whose property is selected</typeparam>
        /// <typeparam name="TProperty">The type of the property which should be selected</typeparam>
        internal static string GetPropertyName<T, TProperty>(this Expression<Func<T, TProperty>> selector)
        {
            var memberExpression = selector.Body as MemberExpression;
            return memberExpression?.Member.Name ?? ((MemberExpression)((UnaryExpression)selector.Body).Operand).Member.Name;
        }
    }
}
