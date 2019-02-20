using MessagePack;



namespace CloudStructures.Converters
{
    /// <summary>
    /// Provides value converter using MessagePack for C#.
    /// </summary>
    public sealed class MessagePackConverter : IValueConverter
    {
        /// <summary>
        /// Serialize value to binary.
        /// </summary>
        /// <typeparam name="T">Data type</typeparam>
        /// <param name="value"></param>
        /// <returns></returns>
        public byte[] Serialize<T>(T value)
            => LZ4MessagePackSerializer.Serialize(value);


        /// <summary>
        /// Deserialize value from binary.
        /// </summary>
        /// <typeparam name="T">Data type</typeparam>
        /// <param name="value"></param>
        /// <returns></returns>
        public T Deserialize<T>(byte[] value)
            => LZ4MessagePackSerializer.Deserialize<T>(value);
    }
}

