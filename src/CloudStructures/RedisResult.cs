using System;
using CloudStructures.Converters;
using StackExchange.Redis;



namespace CloudStructures
{
    /// <summary>
    /// Represents generics version of <see cref="RedisResult"/>.
    /// </summary>
    /// <typeparam name="T">Data type</typeparam>
    public readonly struct RedisResult<T>
    {
        #region Properties
        /// <summary>
        /// Gets default value.
        /// </summary>
        public static RedisResult<T> Default { get; } = default;


        /// <summary>
        /// Gets If value exists.
        /// </summary>
        public bool HasValue { get; }


        /// <summary>
        /// Gets value.
        /// </summary>
        public T Value
            => this.HasValue
            ? this.value
            : throw new InvalidOperationException("has no value.");
        private readonly T value;
        #endregion


        #region Constructors
        /// <summary>
        /// Creates instance.
        /// </summary>
        /// <param name="value"></param>
        internal RedisResult(T value)
        {
            this.HasValue = true;
            this.value = value;
        }
        #endregion


        #region override
        /// <summary>
        /// Converts to string.
        /// </summary>
        /// <returns></returns>
        public override string? ToString()
            => this.HasValue ? this.Value?.ToString() : null;
        #endregion


        #region Gets
        /// <summary>
        /// Gets value. Returns null if value doesn't exists.
        /// </summary>
        /// <returns></returns>
        public object? GetValueOrNull()
            => this.HasValue ? this.Value : null;


        /// <summary>
        /// Gets value. Returns default value if value doesn't exists.
        /// </summary>
        /// <param name="default"></param>
        /// <returns></returns>
        public T? GetValueOrDefault(T? @default = default)
            => this.HasValue ? this.Value : @default;


        /// <summary>
        /// Gets value. Returns value which returned from delegate if value doesn't exists.
        /// </summary>
        /// <param name="valueFactory"></param>
        /// <returns></returns>
        public T? GetValueOrDefault(Func<T?> valueFactory)
            => this.HasValue ? this.Value : valueFactory();
        #endregion
    }



    /// <summary>
    /// Represents generics version of <see cref="RedisResult"/> with expiration time.
    /// </summary>
    /// <typeparam name="T">Data type</typeparam>
    public readonly struct RedisResultWithExpiry<T>
    {
        #region Properties
        /// <summary>
        /// Gets default value.
        /// </summary>
        public static RedisResultWithExpiry<T> Default { get; } = default;


        /// <summary>
        /// Gets If value exists.
        /// </summary>
        public bool HasValue { get; }


        /// <summary>
        /// Gets value.
        /// </summary>
        public T Value
            => this.HasValue
            ? this.value
            : throw new InvalidOperationException("has no value.");
        private readonly T value;


        /// <summary>
        /// Gets expiration time.
        /// </summary>
        public TimeSpan? Expiry { get; }
        #endregion


        #region Constructors
        /// <summary>
        /// Creates instance.
        /// </summary>
        /// <param name="value"></param>
        /// <param name="expiry"></param>
        internal RedisResultWithExpiry(T value, TimeSpan? expiry)
        {
            this.HasValue = true;
            this.value = value;
            this.Expiry = expiry;
        }
        #endregion


        #region override
        /// <summary>
        /// Converts to string.
        /// </summary>
        /// <returns></returns>
        public override string? ToString()
            => this.HasValue ? this.Value?.ToString() : null;
        #endregion


        #region Gets
        /// <summary>
        /// Gets value. Returns null if value doesn't exists.
        /// </summary>
        /// <returns></returns>
        public object? GetValueOrNull()
            => this.HasValue ? this.Value : null;


        /// <summary>
        /// Gets value. Returns default value if value doesn't exists.
        /// </summary>
        /// <param name="default"></param>
        /// <returns></returns>
        public T? GetValueOrDefault(T? @default = default)
            => this.HasValue ? this.Value : @default;


        /// <summary>
        /// Gets value. Returns value which returned from delegate if value doesn't exists.
        /// </summary>
        /// <param name="valueFactory"></param>
        /// <returns></returns>
        public T? GetValueOrDefault(Func<T?> valueFactory)
            => this.HasValue ? this.Value : valueFactory();
        #endregion
    }



    /// <summary>
    /// Provides extension methods for <see cref="RedisResult{T}"/> and <seealso cref="RedisResultWithExpiry{T}"/>.
    /// </summary>
    internal static class RedisResultExtensions
    {
        /// <summary>
        /// Converts to <see cref="RedisResult{T}"/>.
        /// </summary>
        /// <typeparam name="T">Data type</typeparam>
        /// <param name="value"></param>
        /// <param name="converter"></param>
        /// <returns></returns>
        public static RedisResult<T> ToResult<T>(this in RedisValue value, ValueConverter converter)
        {
            if (value.IsNull)
                return RedisResult<T>.Default;

            var converted = converter.Deserialize<T>(value);
            return new(converted);
        }


        /// <summary>
        /// Converts to <see cref="RedisResultWithExpiry{T}"/>.
        /// </summary>
        /// <typeparam name="T">Data type</typeparam>
        /// <param name="value"></param>
        /// <param name="converter"></param>
        /// <returns></returns>
        public static RedisResultWithExpiry<T> ToResult<T>(this in RedisValueWithExpiry value, ValueConverter converter)
        {
            if (value.Value.IsNull)
                return RedisResultWithExpiry<T>.Default;

            var converted = converter.Deserialize<T>(value.Value);
            return new(converted, value.Expiry);
        }
    }
}
