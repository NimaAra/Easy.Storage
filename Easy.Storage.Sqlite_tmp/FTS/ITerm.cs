namespace Easy.Storage.SQLite.FTS
{
    /// <summary>
    /// Specifies the contract for an <see cref="ITerm{T}"/>.
    /// </summary>
    /// <typeparam name="T">The model represented by the <see cref="ITerm{T}"/>.</typeparam>
    // ReSharper disable once UnusedTypeParameter
    public interface ITerm<T>
    {
        /// <summary>
        /// Clears all the <see cref="ITerm{T}"/>.
        /// </summary>
        void Clear();
    }
}