namespace CloudStructures.Converters
{
    /// <summary>
    /// 値の変換機能を提供します。
    /// </summary>
    public interface IValueConverter
    {
        /// <summary>
        /// 直列化します。
        /// </summary>
        /// <typeparam name="T">データ型</typeparam>
        /// <param name="value">値</param>
        /// <returns>直列化された値</returns>
        byte[] Serialize<T>(T value);


        /// <summary>
        /// 逆直列化します。
        /// </summary>
        /// <typeparam name="T">データ型</typeparam>
        /// <param name="value">直列化された値</param>
        /// <returns>逆直列化された値</returns>
        T Deserialize<T>(byte[] value);
    }
}