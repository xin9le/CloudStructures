using StackExchange.Redis;



namespace CloudStructures.Converters
{
    /// <summary>
    /// <see cref="RedisValue"/> との変換機能を提供します。
    /// </summary>
    internal interface IRedisValueConverter<T>
    {
        /// <summary>
        /// 直列化します。
        /// </summary>
        /// <typeparam name="T">データ型</typeparam>
        /// <param name="value">値</param>
        /// <returns>直列化された値</returns>
        RedisValue Serialize(T value);


        /// <summary>
        /// 逆直列化します。
        /// </summary>
        /// <typeparam name="T">データ型</typeparam>
        /// <param name="value">直列化された値</param>
        /// <returns>逆直列化された値</returns>
        T Deserialize(RedisValue value);
    }
}