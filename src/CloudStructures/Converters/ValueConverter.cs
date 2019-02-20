using System;
using System.Collections.Generic;
using StackExchange.Redis;



namespace CloudStructures.Converters
{
    /// <summary>
    /// 値の変換機能を提供します。
    /// </summary>
    internal sealed class ValueConverter
    {
        #region プロパティ
        /// <summary>
        /// 独自の値変換機能を取得します。
        /// </summary>
        private IValueConverter CustomConverter { get; }
        #endregion


        #region コンストラクタ
        /// <summary>
        /// インスタンスを生成します。
        /// </summary>
        /// <param name="customConverter"></param>
        public ValueConverter(IValueConverter customConverter)
            => this.CustomConverter = customConverter ?? throw new ArgumentNullException(nameof(customConverter));
        #endregion


        #region シリアライズ
        /// <summary>
        /// 直列化します。
        /// </summary>
        /// <typeparam name="T">データ型</typeparam>
        /// <param name="value">値</param>
        /// <returns>直列化された値</returns>
        public RedisValue Serialize<T>(T value)
        {
            var converter = PrimitiveConverterCache<T>.Converter;
            return converter == null
                ? (RedisValue)this.CustomConverter.Serialize(value)
                : converter.Serialize(value);
        }


        /// <summary>
        /// 逆直列化します。
        /// </summary>
        /// <typeparam name="T">データ型</typeparam>
        /// <param name="value">直列化された値</param>
        /// <returns>逆直列化された値</returns>
        public T Deserialize<T>(RedisValue value)
        {
            var converter = PrimitiveConverterCache<T>.Converter;
            return converter == null
                ? this.CustomConverter.Deserialize<T>(value)
                : converter.Deserialize(value);
        }
        #endregion


        #region キャッシュ
        /// <summary>
        /// <see cref="IRedisValueConverter{T}"/> のキャッシュ機構を提供します。
        /// </summary>
        private static class PrimitiveConverterCache
        {
            /// <summary>
            /// 型とコンバーターのマップテーブルを保持します。
            /// </summary>
            public static IDictionary<Type, object> Map { get; } = new Dictionary<Type, object>
            {
                [typeof(bool)] = new BooleanConverter(),
                [typeof(bool?)] = new NullableBooleanConverter(),
                [typeof(char)] = new CharConverter(),
                [typeof(char?)] = new NullableCharConverter(),
                [typeof(sbyte)] = new SByteConverter(),
                [typeof(sbyte?)] = new NullableSByteConverter(),
                [typeof(byte)] = new ByteConverter(),
                [typeof(byte?)] = new NullableByteConverter(),
                [typeof(short)] = new Int16Converter(),
                [typeof(short?)] = new NullableInt16Converter(),
                [typeof(ushort)] = new UInt16Converter(),
                [typeof(ushort?)] = new NullableUInt16Converter(),
                [typeof(int)] = new Int32Converter(),
                [typeof(int?)] = new NullableInt32Converter(),
                [typeof(uint)] = new UInt32Converter(),
                [typeof(uint?)] = new NullableUInt32Converter(),
                [typeof(long)] = new Int64Converter(),
                [typeof(long?)] = new NullableInt64Converter(),
                [typeof(ulong)] = new UInt64Converter(),
                [typeof(ulong?)] = new NullableUInt64Converter(),
                [typeof(float)] = new SingleConverter(),
                [typeof(float?)] = new NullableSingleConverter(),
                [typeof(double)] = new DoubleConverter(),
                [typeof(double?)] = new NullableDoubleConverter(),
                [typeof(string)] = new StringConverter(),
                [typeof(byte[])] = new ByteArrayConverter(),
            };
        }


        /// <summary>
        /// <see cref="IRedisValueConverter{T}"/> のキャッシュ機構を提供します。
        /// </summary>
        /// <typeparam name="T">データ型</typeparam>
        private static class PrimitiveConverterCache<T>
        {
            /// <summary>
            /// コンバーターを取得します。
            /// </summary>
            public static IRedisValueConverter<T> Converter { get; }


            /// <summary>
            /// 
            /// </summary>
            static PrimitiveConverterCache()
            {
                Converter
                    = PrimitiveConverterCache.Map.TryGetValue(typeof(T), out var converter)
                    ? (IRedisValueConverter<T>)converter
                    : null;
            }
        }
        #endregion
    }
}
