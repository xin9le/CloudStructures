using StackExchange.Redis;



namespace CloudStructures.Converters
{
    /// <summary>
    /// <see cref="Boolean"/> の変換機能を提供します。
    /// </summary>
    internal sealed class BooleanConverter : IRedisValueConverter<bool>
    {
        public RedisValue Serialize(bool value) => value;
        public bool Deserialize(RedisValue value) => (bool)value;
    }



    /// <summary>
    /// <see cref="Nullable{Boolean}"/> の変換機能を提供します。
    /// </summary>
    internal sealed class NullableBooleanConverter : IRedisValueConverter<bool?>
    {
        public RedisValue Serialize(bool? value) => value;
        public bool? Deserialize(RedisValue value) => (bool?)value;
    }



    /// <summary>
    /// <see cref="Char"/> の変換機能を提供します。
    /// </summary>
    internal sealed class CharConverter : IRedisValueConverter<char>
    {
        public RedisValue Serialize(char value) => value;
        public char Deserialize(RedisValue value) => (char)value;
    }



    /// <summary>
    /// <see cref="Nullable{Char}"/> の変換機能を提供します。
    /// </summary>
    internal sealed class NullableCharConverter : IRedisValueConverter<char?>
    {
        public RedisValue Serialize(char? value) => value;
        public char? Deserialize(RedisValue value) => (char?)value;
    }



    /// <summary>
    /// <see cref="SByte"/> の変換機能を提供します。
    /// </summary>
    internal sealed class SByteConverter : IRedisValueConverter<sbyte>
    {
        public RedisValue Serialize(sbyte value) => value;
        public sbyte Deserialize(RedisValue value) => (sbyte)value;
    }



    /// <summary>
    /// <see cref="Nullable{SByte}"/> の変換機能を提供します。
    /// </summary>
    internal sealed class NullableSByteConverter : IRedisValueConverter<sbyte?>
    {
        public RedisValue Serialize(sbyte? value) => value;
        public sbyte? Deserialize(RedisValue value) => (sbyte?)value;
    }



    /// <summary>
    /// <see cref="Byte"/> の変換機能を提供します。
    /// </summary>
    internal sealed class ByteConverter : IRedisValueConverter<byte>
    {
        public RedisValue Serialize(byte value) => value;
        public byte Deserialize(RedisValue value) => (byte)value;
    }



    /// <summary>
    /// <see cref="Nullable{Byte}"/> の変換機能を提供します。
    /// </summary>
    internal sealed class NullableByteConverter : IRedisValueConverter<byte?>
    {
        public RedisValue Serialize(byte? value) => value;
        public byte? Deserialize(RedisValue value) => (byte?)value;
    }



    /// <summary>
    /// <see cref="Int16"/> の変換機能を提供します。
    /// </summary>
    internal sealed class Int16Converter : IRedisValueConverter<short>
    {
        public RedisValue Serialize(short value) => value;
        public short Deserialize(RedisValue value) => (short)value;
    }



    /// <summary>
    /// <see cref="Nullable{Int16}"/> の変換機能を提供します。
    /// </summary>
    internal sealed class NullableInt16Converter : IRedisValueConverter<short?>
    {
        public RedisValue Serialize(short? value) => value;
        public short? Deserialize(RedisValue value) => (short?)value;
    }



    /// <summary>
    /// <see cref="UInt16"/> の変換機能を提供します。
    /// </summary>
    internal sealed class UInt16Converter : IRedisValueConverter<ushort>
    {
        public RedisValue Serialize(ushort value) => value;
        public ushort Deserialize(RedisValue value) => (ushort)value;
    }



    /// <summary>
    /// <see cref="Nullable{UInt16}"/> の変換機能を提供します。
    /// </summary>
    internal sealed class NullableUInt16Converter : IRedisValueConverter<ushort?>
    {
        public RedisValue Serialize(ushort? value) => value;
        public ushort? Deserialize(RedisValue value) => (ushort?)value;
    }



    /// <summary>
    /// <see cref="Int32"/> の変換機能を提供します。
    /// </summary>
    internal sealed class Int32Converter : IRedisValueConverter<int>
    {
        public RedisValue Serialize(int value) => value;
        public int Deserialize(RedisValue value) => (int)value;
    }



    /// <summary>
    /// <see cref="Nullable{Int32}"/> の変換機能を提供します。
    /// </summary>
    internal sealed class NullableInt32Converter : IRedisValueConverter<int?>
    {
        public RedisValue Serialize(int? value) => value;
        public int? Deserialize(RedisValue value) => (int?)value;
    }



    /// <summary>
    /// <see cref="UInt32"/> の変換機能を提供します。
    /// </summary>
    internal sealed class UInt32Converter : IRedisValueConverter<uint>
    {
        public RedisValue Serialize(uint value) => value;
        public uint Deserialize(RedisValue value) => (uint)value;
    }



    /// <summary>
    /// <see cref="Nullable{UInt32}"/> の変換機能を提供します。
    /// </summary>
    internal sealed class NullableUInt32Converter : IRedisValueConverter<uint?>
    {
        public RedisValue Serialize(uint? value) => value;
        public uint? Deserialize(RedisValue value) => (uint?)value;
    }



    /// <summary>
    /// <see cref="Int64"/> の変換機能を提供します。
    /// </summary>
    internal sealed class Int64Converter : IRedisValueConverter<long>
    {
        public RedisValue Serialize(long value) => value;
        public long Deserialize(RedisValue value) => (long)value;
    }



    /// <summary>
    /// <see cref="Nullable{Int64}"/> の変換機能を提供します。
    /// </summary>
    internal sealed class NullableInt64Converter : IRedisValueConverter<long?>
    {
        public RedisValue Serialize(long? value) => value;
        public long? Deserialize(RedisValue value) => (long?)value;
    }



    /// <summary>
    /// <see cref="UInt64"/> の変換機能を提供します。
    /// </summary>
    internal sealed class UInt64Converter : IRedisValueConverter<ulong>
    {
        public RedisValue Serialize(ulong value) => value;
        public ulong Deserialize(RedisValue value) => (ulong)value;
    }



    /// <summary>
    /// <see cref="Nullable{UInt64}"/> の変換機能を提供します。
    /// </summary>
    internal sealed class NullableUInt64Converter : IRedisValueConverter<ulong?>
    {
        public RedisValue Serialize(ulong? value) => value;
        public ulong? Deserialize(RedisValue value) => (ulong?)value;
    }



    /// <summary>
    /// <see cref="Single"/> の変換機能を提供します。
    /// </summary>
    internal sealed class SingleConverter : IRedisValueConverter<float>
    {
        public RedisValue Serialize(float value) => value;
        public float Deserialize(RedisValue value) => (float)value;
    }



    /// <summary>
    /// <see cref="Nullable{Single}"/> の変換機能を提供します。
    /// </summary>
    internal sealed class NullableSingleConverter : IRedisValueConverter<float?>
    {
        public RedisValue Serialize(float? value) => value;
        public float? Deserialize(RedisValue value) => (float?)value;
    }



    /// <summary>
    /// <see cref="Double"/> の変換機能を提供します。
    /// </summary>
    internal sealed class DoubleConverter : IRedisValueConverter<double>
    {
        public RedisValue Serialize(double value) => value;
        public double Deserialize(RedisValue value) => (double)value;
    }



    /// <summary>
    /// <see cref="Nullable{Double}"/> の変換機能を提供します。
    /// </summary>
    internal sealed class NullableDoubleConverter : IRedisValueConverter<double?>
    {
        public RedisValue Serialize(double? value) => value;
        public double? Deserialize(RedisValue value) => (double?)value;
    }



    /// <summary>
    /// <see cref="string"/> の変換機能を提供します。
    /// </summary>
    internal sealed class StringConverter : IRedisValueConverter<string>
    {
        public RedisValue Serialize(string value) => value;
        public string Deserialize(RedisValue value) => value;
    }



    /// <summary>
    /// <see cref="byte[]"/> の変換機能を提供します。
    /// </summary>
    internal sealed class ByteArrayConverter : IRedisValueConverter<byte[]>
    {
        public RedisValue Serialize(byte[] value) => value;
        public byte[] Deserialize(RedisValue value) => value;
    }
}
