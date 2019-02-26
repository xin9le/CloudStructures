using StackExchange.Redis;



namespace CloudStructures.Converters
{
    /// <summary>
    /// Provides conversion function to <see cref="RedisValue"/>.
    /// </summary>
    internal interface IRedisValueConverter<T>
    {
        /// <summary>
        /// Serialize to <see cref="RedisValue"/>.
        /// </summary>
        /// <typeparam name="T">Data type</typeparam>
        /// <param name="value"></param>
        /// <returns></returns>
        RedisValue Serialize(T value);


        /// <summary>
        /// Deserialize from <see cref="RedisValue"/>.
        /// </summary>
        /// <typeparam name="T">Data type</typeparam>
        /// <param name="value"></param>
        /// <returns></returns>
        T Deserialize(RedisValue value);
    }
}
