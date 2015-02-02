using StackExchange.Redis;
using System;

namespace CloudStructures
{
    public interface IRedisValueConverter
    {
        RedisValue Serialize(object value, out long resultSize);
        object Deserialize(Type type, RedisValue value, out long valueSize);
    }

    public static class RedisValueConverterExtensions
    {
        public static T Deserialize<T>(this IRedisValueConverter converter, RedisValue value, out long valueSize)
        {
            return (T)converter.Deserialize(typeof(T), value, out valueSize);
        }
    }

    /// <summary>
    /// <para>Bass class of IRedisValueConverter for object serialization.</para>
    /// <para>If target is primitive type(int, double, etc), doesn't pass to core serializer.</para>
    /// </summary>
    public abstract class ObjectRedisValueConverterBase : IRedisValueConverter
    {
        protected abstract object DeserializeCore(Type type, byte[] value);
        protected abstract byte[] SerializeCore(object value, out long resultSize);

        static TypeCode GetNonNullableTypeCode(Type type)
        {
            return Type.GetTypeCode(Nullable.GetUnderlyingType(type) ?? type);
        }

        public object Deserialize(Type type, RedisValue value, out long valueSize)
        {
            if (value.IsNull)
            {
                valueSize = 0;
                return null;
            }
            var code = GetNonNullableTypeCode(type);
            switch (code)
            {
                case TypeCode.Empty:
                case TypeCode.DBNull:
                    valueSize = 0;
                    return null;
                case TypeCode.SByte:
                    valueSize = sizeof(SByte);
                    return (SByte)value;
                case TypeCode.Byte:
                    valueSize = sizeof(Byte);
                    return (Byte)value;
                case TypeCode.Int16:
                    valueSize = sizeof(Int16);
                    return (Int16)value;
                case TypeCode.Int32:
                    valueSize = sizeof(Int32);
                    return (Int32)value;
                case TypeCode.Int64:
                    valueSize = sizeof(Int64);
                    return (Int64)value;
                case TypeCode.UInt16:
                    valueSize = sizeof(UInt16);
                    return (UInt16)value;
                case TypeCode.UInt32:
                    valueSize = sizeof(UInt32);
                    return (UInt32)value;
                case TypeCode.UInt64:
                    valueSize = sizeof(UInt64);
                    return (UInt64)value;
                case TypeCode.Single:
                    valueSize = sizeof(Single);
                    return (Single)value;
                case TypeCode.Double:
                    valueSize = sizeof(Double);
                    return (Double)value;
                case TypeCode.Boolean:
                    valueSize = sizeof(Boolean);
                    return (Boolean)value;
                case TypeCode.Char:
                    valueSize = sizeof(Char);
                    return (Char)value;
                case TypeCode.String:
                default:
                    byte[] buf = value;
                    valueSize = buf.Length;
                    return DeserializeCore(type, buf);
            }
        }

        public RedisValue Serialize(object value, out long resultSize)
        {
            var code = (value == null)
               ? TypeCode.Empty
               : Type.GetTypeCode(value.GetType());

            switch (code)
            {
                case TypeCode.Empty:
                case TypeCode.DBNull:
                    resultSize = 0;
                    return RedisValue.Null;
                case TypeCode.SByte:
                    resultSize = sizeof(SByte);
                    return (sbyte)value;
                case TypeCode.Byte:
                    resultSize = sizeof(byte);
                    return (byte)value;
                case TypeCode.Int16:
                    resultSize = sizeof(Int16);
                    return (Int16)value;
                case TypeCode.Int32:
                    resultSize = sizeof(Int32);
                    return (Int32)value;
                case TypeCode.Int64:
                    resultSize = sizeof(Int64);
                    return (Int64)value;
                case TypeCode.UInt16:
                    resultSize = sizeof(UInt16);
                    return (UInt16)value;
                case TypeCode.UInt32:
                    resultSize = sizeof(UInt32);
                    return (UInt32)value;
                case TypeCode.UInt64:
                    resultSize = sizeof(UInt64);
                    return (UInt64)value;
                case TypeCode.Single:
                    resultSize = sizeof(Single);
                    return (Single)value;
                case TypeCode.Double:
                    resultSize = sizeof(Double);
                    return (Double)value;
                case TypeCode.Boolean:
                    resultSize = sizeof(Boolean);
                    return (Boolean)value;
                case TypeCode.Char:
                    resultSize = sizeof(Char);
                    return (Char)value;
                case TypeCode.String:
                default:
                    return SerializeCore(value, out resultSize);
            }
        }
    }
}