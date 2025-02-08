﻿using System;
using System.Collections.Generic;
using StackExchange.Redis;

namespace CloudStructures.Converters;



/// <summary>
/// Provides data conversion function.
/// </summary>
internal sealed class ValueConverter(IValueConverter? customConverter)
{
    #region Properties
    /// <summary>
    /// Gets custom conversion function.
    /// </summary>
    private IValueConverter CustomConverter { get; } = customConverter ?? new SystemTextJsonConverter();
    #endregion


    #region Serialization
    /// <summary>
    /// Serialize to <see cref="RedisValue"/>.
    /// </summary>
    /// <typeparam name="T">Data type</typeparam>
    /// <param name="value"></param>
    /// <returns></returns>
    public RedisValue Serialize<T>(T value)
    {
        var converter = PrimitiveConverterCache<T>.Converter;
        return converter is null
            ? this.CustomConverter.Serialize(value)
            : converter.Serialize(value);
    }


    /// <summary>
    /// Deserialize from <see cref="RedisValue"/>.
    /// </summary>
    /// <typeparam name="T">Data type</typeparam>
    /// <param name="value"></param>
    /// <returns></returns>
    public T Deserialize<T>(RedisValue value)
    {
        var converter = PrimitiveConverterCache<T>.Converter;
        return converter is null
            ? this.CustomConverter.Deserialize<T>(value!)  // forgive
            : converter.Deserialize(value);
    }
    #endregion


    #region Cache
    /// <summary>
    /// Provides primitive value converter cache mecanism.
    /// </summary>
    private static class PrimitiveConverterCache
    {
        /// <summary>
        /// Hold type and converter mapping table.
        /// </summary>
        public static IReadOnlyDictionary<Type, object> Map { get; } = new Dictionary<Type, object>
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
            [typeof(Memory<byte>)] = new MemoryByteConverter(),
            [typeof(ReadOnlyMemory<byte>)] = new ReadOnlyMemoryByteConverter(),
        };
    }


    /// <summary>
    /// Provides <see cref="IRedisValueConverter{T}"/> cache mecanism.
    /// </summary>
    /// <typeparam name="T">Data type</typeparam>
    private static class PrimitiveConverterCache<T>
    {
        /// <summary>
        /// Gets converter.
        /// </summary>
        public static IRedisValueConverter<T>? Converter { get; }


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
