using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CloudStructures.Redis
{
    public interface IRedisValueConverter
    {
        byte[] Serialize(object value);
        object Deserialize(Type type, byte[] value);
    }

    public static class RedisValueConverterExtensions
    {
        public static T Deserialize<T>(this IRedisValueConverter converter, byte[] value)
        {
            return (T)converter.Deserialize(typeof(T), value);
        }
    }

    public class JsonRedisValueConverter : IRedisValueConverter
    {
        public byte[] Serialize(object value)
        {
            return Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(value));
        }

        public object Deserialize(Type type, byte[] value)
        {
            return JsonConvert.DeserializeObject(Encoding.UTF8.GetString(value), type);
        }
    }

    public class ProtoBufRedisValueConverter : IRedisValueConverter
    {
        static TypeCode GetNonNullableTypeCode(Type type)
        {
            var isNullable = type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>);
            return (isNullable)
                ? Type.GetTypeCode(type.GetGenericArguments()[0])
                : Type.GetTypeCode(type);
        }

        public byte[] Serialize(object value)
        {
            var code = (value == null)
                ? TypeCode.Empty
                : Type.GetTypeCode(value.GetType());

            switch (code)
            {
                case TypeCode.Empty:
                case TypeCode.DBNull:
                    return new byte[0];
                case TypeCode.SByte:
                    return Encoding.UTF8.GetBytes(((sbyte)value).ToString());
                case TypeCode.Byte:
                    return Encoding.UTF8.GetBytes(((byte)value).ToString());
                case TypeCode.Int16:
                    return Encoding.UTF8.GetBytes(((Int16)value).ToString());
                case TypeCode.Int32:
                    return Encoding.UTF8.GetBytes(((Int32)value).ToString());
                case TypeCode.Int64:
                    return Encoding.UTF8.GetBytes(((Int64)value).ToString());
                case TypeCode.UInt16:
                    return Encoding.UTF8.GetBytes(((UInt16)value).ToString());
                case TypeCode.UInt32:
                    return Encoding.UTF8.GetBytes(((UInt32)value).ToString());
                case TypeCode.UInt64:
                    return Encoding.UTF8.GetBytes(((UInt64)value).ToString());
                case TypeCode.Single:
                    return Encoding.UTF8.GetBytes(((Single)value).ToString());
                case TypeCode.Double:
                    return Encoding.UTF8.GetBytes(((Double)value).ToString());
                case TypeCode.String: // allow null value
                default:
                    using (var ms = new MemoryStream())
                    {
                        ProtoBuf.Serializer.NonGeneric.Serialize(ms, value);
                        return ms.ToArray();
                    }
            }
        }

        public object Deserialize(Type type, byte[] value)
        {
            if (value == null) return null;

            var code = GetNonNullableTypeCode(type);

            switch (code)
            {
                case TypeCode.Empty:
                case TypeCode.DBNull:
                    return null;
                case TypeCode.SByte:
                    if (value.Length == 0) return null;
                    return SByte.Parse(Encoding.UTF8.GetString(value));
                case TypeCode.Byte:
                    if (value.Length == 0) return null;
                    return Byte.Parse(Encoding.UTF8.GetString(value));
                case TypeCode.Int16:
                    if (value.Length == 0) return null;
                    return Int16.Parse(Encoding.UTF8.GetString(value));
                case TypeCode.Int32:
                    if (value.Length == 0) return null;
                    return Int32.Parse(Encoding.UTF8.GetString(value));
                case TypeCode.Int64:
                    if (value.Length == 0) return null;
                    return Int64.Parse(Encoding.UTF8.GetString(value));
                case TypeCode.UInt16:
                    if (value.Length == 0) return null;
                    return UInt16.Parse(Encoding.UTF8.GetString(value));
                case TypeCode.UInt32:
                    if (value.Length == 0) return null;
                    return UInt32.Parse(Encoding.UTF8.GetString(value));
                case TypeCode.UInt64:
                    if (value.Length == 0) return null;
                    return UInt64.Parse(Encoding.UTF8.GetString(value));
                case TypeCode.Single:
                    if (value.Length == 0) return null;
                    return Single.Parse(Encoding.UTF8.GetString(value));
                case TypeCode.Double:
                    if (value.Length == 0) return null;
                    return Double.Parse(Encoding.UTF8.GetString(value));
                case TypeCode.String: // allow null value
                default:
                    using (var ms = new MemoryStream(value))
                    {
                        return ProtoBuf.Serializer.NonGeneric.Deserialize(type, ms);
                    }
            }
        }
    }
}