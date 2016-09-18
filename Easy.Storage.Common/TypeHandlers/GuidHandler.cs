namespace Easy.Storage.Common.TypeHandlers
{
    using System;
    using System.Data;
    using Dapper;

    /// <summary>
    /// A handler for storing and retrieving <see cref="Guid"/>s.
    /// </summary>
    internal class GuidHandler : SqlMapper.TypeHandler<Guid>
    {
        public override void SetValue(IDbDataParameter parameter, Guid value)
        {
            parameter.Value = value.ToString();
        }

        public override Guid Parse(object value)
        {
            return Guid.Parse(value.ToString());
        }
    }
}