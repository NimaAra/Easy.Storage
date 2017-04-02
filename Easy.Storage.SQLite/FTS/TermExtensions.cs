namespace Easy.Storage.SQLite.FTS
{
    using System;
    using System.Linq.Expressions;

    /// <summary>
    /// Provides a set of methods for supporting <c>SQLite Full Text Search</c> based on <see cref="ITerm{T}"/>.
    /// </summary>
    public static class TermExtensions
    {
        /// <summary>
        /// Adds an <c>AND</c> clause for matching the required fields and values.
        /// </summary>
        /// <typeparam name="T">The type of the model for which <c>FTS</c> should be performed.</typeparam>
        /// <typeparam name="TProperty">The property representing the <c>FTS</c> column.</typeparam>
        /// <param name="term">The object representing the <c>FTS</c> term.</param>
        /// <param name="type">The matching type to match all or any of the <paramref name="values"/>.</param>
        /// <param name="selector">The expression to select the column to search on.</param>
        /// <param name="values">The values on which <c>FTS</c> should be based.</param>
        public static ITerm<T> And<T, TProperty>(this ITerm<T> term, Match type, Expression<Func<T, TProperty>> selector, params TProperty[] values)
        {
            var tmpTerm = (Term<T>)term;
            tmpTerm.Add(JoinClauses.Intersect, false, type, selector, values);
            return term;
        }

        /// <summary>
        /// Adds an <c>OR</c> clause for matching the required fields and values.
        /// </summary>
        /// <typeparam name="T">The type of the model for which <c>FTS</c> should be performed.</typeparam>
        /// <typeparam name="TProperty">The property representing the <c>FTS</c> column.</typeparam>
        /// <param name="term">The object representing the <c>FTS</c> term.</param>
        /// <param name="type">The matching type to match all or any of the <paramref name="values"/>.</param>
        /// <param name="selector">The expression to select the column to search on.</param>
        /// <param name="values">The values on which <c>FTS</c> should be based.</param>
        public static ITerm<T> Or<T, TProperty>(this ITerm<T> term, Match type, Expression<Func<T, TProperty>> selector, params TProperty[] values)
        {
            var tmpTerm = (Term<T>)term;
            tmpTerm.Add(JoinClauses.Union, false, type, selector, values);
            return term;
        }

        /// <summary>
        /// Adds an <c>OR NOT</c> clause for matching the required fields and values.
        /// </summary>
        /// <typeparam name="T">The type of the model for which <c>FTS</c> should be performed.</typeparam>
        /// <typeparam name="TProperty">The property representing the <c>FTS</c> column.</typeparam>
        /// <param name="term">The object representing the <c>FTS</c> term.</param>
        /// <param name="type">The matching type to match all or any of the <paramref name="values"/>.</param>
        /// <param name="selector">The expression to select the column to search on.</param>
        /// <param name="values">The values on which <c>FTS</c> should be based.</param>
        public static ITerm<T> AndNot<T, TProperty>(this ITerm<T> term, Match type, Expression<Func<T, TProperty>> selector, params TProperty[] values)
        {
            var tmpTerm = (Term<T>)term;
            tmpTerm.Add(JoinClauses.Intersect, true, type, selector, values);
            return term;
        }

        /// <summary>
        /// Adds an <c>AND</c> clause for matching the required fields and values.
        /// </summary>
        /// <typeparam name="T">The type of the model for which <c>FTS</c> should be performed.</typeparam>
        /// <typeparam name="TProperty">The property representing the <c>FTS</c> column.</typeparam>
        /// <param name="term">The object representing the <c>FTS</c> term.</param>
        /// <param name="type">The matching type to match all or any of the <paramref name="values"/>.</param>
        /// <param name="selector">The expression to select the column to search on.</param>
        /// <param name="values">The values on which <c>FTS</c> should be based.</param>
        public static ITerm<T> OrNot<T, TProperty>(this ITerm<T> term, Match type, Expression<Func<T, TProperty>> selector, params TProperty[] values)
        {
            var tmpTerm = (Term<T>)term;
            tmpTerm.Add(JoinClauses.Union, true, type, selector, values);
            return term;
        }
    }
}