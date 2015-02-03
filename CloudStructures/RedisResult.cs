using StackExchange.Redis;
using System;

namespace CloudStructures
{
    internal static class RedisResult
    {
        public static RedisResult<T> Create<T>(T value)
        {
            return new RedisResult<T>(value);
        }

        public static RedisResult<T> FromRedisValue<T>(RedisValue value, RedisSettings settings, out long valueSize)
        {
            if (value.IsNull)
            {
                valueSize = 0;
                return new RedisResult<T>();
            }
            return new RedisResult<T>(settings.ValueConverter.Deserialize<T>(value, out valueSize));
        }
    }

    public struct RedisResult<T>
    {
        public bool HasValue { get; private set; }

        public T Value { get; private set; }

        public RedisResult()
        {
            HasValue = false;
            Value = default(T);
        }

        public RedisResult(T value)
        {
            HasValue = true;
            Value = value;
        }

        public T GetValueOrDefault(T defaultValue)
        {
            return (HasValue) ? Value : defaultValue;
        }

        public T GetValueOrDefault(Func<T> valueFactory)
        {
            return (HasValue) ? Value : valueFactory();
        }

        public override string ToString()
        {
            return (HasValue) ? Value.ToString() : "null";
        }
    }
}