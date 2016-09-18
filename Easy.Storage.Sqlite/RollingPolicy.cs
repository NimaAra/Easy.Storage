namespace Easy.Storage.Sqlite
{
    /// <summary>
    /// Represents how the connection should be rolled. 
    /// </summary>
    public enum RollingPolicy
    {
        /// <summary>
        /// Does not roll.
        /// </summary>
        None = 0,

        /// <summary>
        /// Rolls every second.
        /// </summary>
        Second,

        /// <summary>
        /// Rolls every minute.
        /// </summary>
        Minute,

        /// <summary>
        /// Rolls every hour.
        /// </summary>
        Hour,

        /// <summary>
        /// Rolls every day.
        /// </summary>
        Day,

        /// <summary>
        /// Rolls every week.
        /// </summary>
        Week
    }
}