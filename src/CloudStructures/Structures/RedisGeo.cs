﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CloudStructures.Converters;
using CloudStructures.Internals;
using StackExchange.Redis;

namespace CloudStructures.Structures;



/// <summary>
/// Provides geometry related commands.
/// </summary>
/// <typeparam name="T">Data type</typeparam>
public readonly struct RedisGeo<T>(RedisConnection connection, RedisKey key, TimeSpan? defaultExpiry) : IRedisStructureWithExpiry
{
    #region IRedisStructureWithExpiry implementations
    /// <summary>
    /// Gets connection.
    /// </summary>
    public RedisConnection Connection { get; } = connection;


    /// <summary>
    /// Gets key.
    /// </summary>
    public RedisKey Key { get; } = key;


    /// <summary>
    /// Gets default expiration time.
    /// </summary>
    public TimeSpan? DefaultExpiry { get; } = defaultExpiry;
    #endregion


    #region Commands
    //- [x] GeoAddAsync
    //- [x] GeoDistanceAsync
    //- [x] GeoHashAsync
    //- [x] GeoPositionAsync
    //- [x] GeoRadiusAsync
    //- [x] GeoRemoveAsync


    /// <summary>
    /// GEOADD : <a href="https://redis.io/commands/geoadd"></a>
    /// </summary>
    public Task<bool> AddAsync(RedisGeoEntry<T> value, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
    {
        expiry ??= this.DefaultExpiry;
        var entry = value.ToNonGenerics(this.Connection.Converter);
        return this.ExecuteWithExpiryAsync
        (
            static (db, state) => db.GeoAddAsync(state.key, state.entry, state.flags),
            state: (key: this.Key, entry, flags),
            expiry,
            flags
        );
    }


    /// <summary>
    /// GEOADD : <a href="https://redis.io/commands/geoadd"></a>
    /// </summary>
    public Task<long> AddAsync(IEnumerable<RedisGeoEntry<T>> values, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
    {
        expiry ??= this.DefaultExpiry;
        var entries = values.Select(this.Connection.Converter, static (x, c) => x.ToNonGenerics(c)).ToArray();
        return this.ExecuteWithExpiryAsync
        (
            static (db, state) => db.GeoAddAsync(state.key, state.entries, state.flags),
            state: (key: this.Key, entries, flags),
            expiry,
            flags
        );
    }


    /// <summary>
    /// GEOADD : <a href="https://redis.io/commands/geoadd"></a>
    /// </summary>
    public Task<bool> AddAsync(double longitude, double latitude, T member, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
    {
        expiry ??= this.DefaultExpiry;
        var entry = new RedisGeoEntry<T>(longitude, latitude, member);
        return this.AddAsync(entry, expiry, flags);
    }


    /// <summary>
    /// GEODIST : <a href="https://redis.io/commands/geodist"></a>
    /// </summary>
    public Task<double?> DistanceAsync(T member1, T member2, GeoUnit unit = GeoUnit.Meters, CommandFlags flags = CommandFlags.None)
    {
        var value1 = this.Connection.Converter.Serialize(member1);
        var value2 = this.Connection.Converter.Serialize(member2);
        return this.Connection.Database.GeoDistanceAsync(this.Key, value1, value2, unit, flags);
    }


    /// <summary>
    /// GEOHASH : <a href="https://redis.io/commands/geohash"></a>
    /// </summary>
    public Task<string?> HashAsync(T member, CommandFlags flags = CommandFlags.None)
    {
        var value = this.Connection.Converter.Serialize(member);
        return this.Connection.Database.GeoHashAsync(this.Key, value, flags);
    }


    /// <summary>
    /// GEOHASH : <a href="https://redis.io/commands/geohash"></a>
    /// </summary>
    public Task<string?[]> HashAsync(IEnumerable<T> members, CommandFlags flags = CommandFlags.None)
    {
        var values = members.Select(this.Connection.Converter.Serialize).ToArray();
        return this.Connection.Database.GeoHashAsync(this.Key, values, flags);
    }


    /// <summary>
    /// GEOPOS : <a href="https://redis.io/commands/geopos"></a>
    /// </summary>
    public Task<GeoPosition?> PositionAsync(T member, CommandFlags flags = CommandFlags.None)
    {
        var value = this.Connection.Converter.Serialize(member);
        return this.Connection.Database.GeoPositionAsync(this.Key, value, flags);
    }


    /// <summary>
    /// GEOPOS : <a href="https://redis.io/commands/geopos"></a>
    /// </summary>
    public Task<GeoPosition?[]> PositionAsync(IEnumerable<T> members, CommandFlags flags = CommandFlags.None)
    {
        var values = members.Select(this.Connection.Converter.Serialize).ToArray();
        return this.Connection.Database.GeoPositionAsync(this.Key, values, flags);
    }


    /// <summary>
    /// GEORADIUS : <a href="https://redis.io/commands/georadius"></a>
    /// </summary>
    public async Task<RedisGeoRadiusResult<T>[]> RadiusAsync(double longitude, double latitude, double radius, GeoUnit unit = GeoUnit.Meters, int count = -1, Order? order = null, GeoRadiusOptions options = GeoRadiusOptions.Default, CommandFlags flags = CommandFlags.None)
    {
        var results = await this.Connection.Database.GeoRadiusAsync(this.Key, longitude, latitude, radius, unit, count, order, options, flags).ConfigureAwait(false);
        return results.Select(this.Connection.Converter, static (x, c) => x.ToGenerics<T>(c)).ToArray();
    }


    /// <summary>
    /// GEORADIUSBYMEMBER : <a href="https://redis.io/commands/georadiusbymember"></a>
    /// </summary>
    public async Task<RedisGeoRadiusResult<T>[]> RadiusAsync(T member, double radius, GeoUnit unit = GeoUnit.Meters, int count = -1, Order? order = null, GeoRadiusOptions options = GeoRadiusOptions.Default, CommandFlags flags = CommandFlags.None)
    {
        var value = this.Connection.Converter.Serialize(member);
        var results = await this.Connection.Database.GeoRadiusAsync(this.Key, value, radius, unit, count, order, options, flags).ConfigureAwait(false);
        return results.Select(this.Connection.Converter, static (x, c) => x.ToGenerics<T>(c)).ToArray();
    }


    /// <summary>
    /// ZREM : <a href="https://redis.io/commands/zrem"></a>
    /// </summary>
    /// <remarks>There is no GEODEL command.</remarks>
    public Task<bool> RemoveAsync(T member, CommandFlags flags = CommandFlags.None)
    {
        var value = this.Connection.Converter.Serialize(member);
        return this.Connection.Database.GeoRemoveAsync(this.Key, value, flags);
    }
    #endregion
}



/// <summary>
/// Represents <see cref="RedisGeo{T}"/> element.
/// </summary>
/// <typeparam name="T">Data type</typeparam>
public readonly struct RedisGeoEntry<T>(double longitude, double latitude, T member)
{
    #region Properties
    /// <summary>
    /// Gets longitude.
    /// </summary>
    public double Longitude { get; } = longitude;


    /// <summary>
    /// Gets latitude.
    /// </summary>
    public double Latitude { get; } = latitude;


    /// <summary>
    /// Gets member.
    /// </summary>
    public T Member { get; } = member;
    #endregion
}



/// <summary>
/// Provides extension methods for <see cref="RedisGeoEntry{T}"/>.
/// </summary>
internal static class RedisGeoEntryExtensions
{
    /// <summary>
    /// Converts to <see cref="GeoEntry"/>.
    /// </summary>
    /// <typeparam name="T">Data type</typeparam>
    /// <param name="entry"></param>
    /// <param name="converter"></param>
    /// <returns></returns>
    public static GeoEntry ToNonGenerics<T>(this in RedisGeoEntry<T> entry, ValueConverter converter)
    {
        var member = converter.Serialize(entry.Member);
        return new GeoEntry(entry.Longitude, entry.Latitude, member);
    }
}



/// <summary>
/// Represents <see cref="RedisGeo{T}"/>.RadiusAsync result.
/// </summary>
/// <typeparam name="T">Data type</typeparam>
public readonly struct RedisGeoRadiusResult<T>(in RedisResult<T> member, double? distance, long? hash, GeoPosition? position)
{
    #region Properties
    /// <summary>
    /// Gets member.
    /// </summary>
    public RedisResult<T> Member { get; } = member;


    /// <summary>
    /// Gets distance.
    /// </summary>
    public double? Distance { get; } = distance;


    /// <summary>
    /// Gets hash.
    /// </summary>
    public long? Hash { get; } = hash;


    /// <summary>
    /// Gets position.
    /// </summary>
    public GeoPosition? Position { get; } = position;
    #endregion
}



/// <summary>
/// Provides extension methods for <see cref="RedisGeoRadiusResult{T}"/>.
/// </summary>
internal static class RedisGeoRadiusResultExtensions
{
    /// <summary>
    /// Converts to <see cref="RedisGeoRadiusResult{T}"/>.
    /// </summary>
    /// <typeparam name="T">Data type</typeparam>
    /// <param name="result"></param>
    /// <param name="converter"></param>
    /// <returns></returns>
    public static RedisGeoRadiusResult<T> ToGenerics<T>(this in GeoRadiusResult result, ValueConverter converter)
    {
        var member = result.Member.ToResult<T>(converter);
        return new RedisGeoRadiusResult<T>(member, result.Distance, result.Hash, result.Position);
    }
}
