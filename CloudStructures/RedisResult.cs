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
        public static RedisResult<T> NoValue = new RedisResult<T>(false, default(T));

        public bool HasValue { get; private set; }

        readonly T value;
        public T Value
        {
            get
            {
                if (!HasValue) throw new InvalidOperationException("Result no contains value");
                return value;
            }
        }

        public RedisResult(T value)
        {
            this.HasValue = true;
            this.value = value;
        }

        RedisResult(bool hasValue, T value)
        {
            this.HasValue = hasValue;
            this.value = default(T);
        }

        public object GetValueOrNull()
        {
            return (HasValue) ? (object)Value : null;
        }

        public T GetValueOrDefault()
        {
            return (HasValue) ? Value : default(T);
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
            return (HasValue) ? Value.ToString() : "";
        }
    }
}