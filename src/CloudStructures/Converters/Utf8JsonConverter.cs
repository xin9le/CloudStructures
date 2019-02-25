using Utf8Json;
using Utf8Json.Resolvers;



namespace CloudStructures.Converters
{
    /// <summary>
    /// Provides value converter using Utf8Json.
    /// </summary>
    public sealed class Utf8JsonConverter : IValueConverter
    {
        /// <summary>
        /// Serialize value to binary.
        /// </summary>
        /// <typeparam name="T">Data type</typeparam>
        /// <param name="value"></param>
        /// <returns></returns>
        public byte[] Serialize<T>(T value)
            => JsonSerializer.Serialize(value, StandardResolver.AllowPrivate);


        /// <summary>
        /// Deserialize value from binary.
        /// </summary>
        /// <typeparam name="T">Data type</typeparam>
        /// <param name="value"></param>
        /// <returns></returns>
        public T Deserialize<T>(byte[] value)
            => JsonSerializer.Deserialize<T>(value, StandardResolver.AllowPrivate);
    }
}
