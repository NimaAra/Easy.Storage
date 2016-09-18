namespace Easy.Storage.Common.TypeHandlers
{
    using System;
    using System.Data;
    using Dapper;

    /// <summary>
    /// A handler for storing and retrieving <see cref="DateTimeOffset"/>s.
    /// </summary>
    internal class DateTimeOffsetHandler : SqlMapper.TypeHandler<DateTimeOffset>
    {
        public override void SetValue(IDbDataParameter parameter, DateTimeOffset value)
        {
            parameter.Value = value.ToString();
        }

        public override DateTimeOffset Parse(object value)
        {
            return DateTimeOffset.Parse(value.ToString());
        }
    }
}