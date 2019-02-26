using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CloudStructures.Converters;
using CloudStructures.Internals;
using StackExchange.Redis;



namespace CloudStructures.Structures
{
    /// <summary>
    /// Provides geometry related commands.
    /// </summary>
    /// <typeparam name="T">Data type</typeparam>
    public readonly struct RedisGeo<T> : IRedisStructure
    {
        #region IRedisStructure implementations
        /// <summary>
        /// Gets connection.
        /// </summary>
        public RedisConnection Connection { get; }


        /// <summary>
        /// Gets key.
        /// </summary>
        public RedisKey Key { get; }


        /// <summary>
        /// Gets default expiration time.
        /// </summary>
        public TimeSpan? DefaultExpiry { get; }
        #endregion


        #region Constructors
        /// <summary>
        /// Creates instance.
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="key"></param>
        /// <param name="defaultExpiry"></param>
        public RedisGeo(RedisConnection connection, RedisKey key, TimeSpan? defaultExpiry)
        {
            this.Connection = connection ?? throw new ArgumentNullException(nameof(connection));
            this.Key = key;
            this.DefaultExpiry = defaultExpiry;
        }
        #endregion


        #region Commands
        //- [x] GeoAddAsync
        //- [x] GeoDistanceAsync
        //- [x] GeoHashAsync
        //- [x] GeoPositionAsync
        //- [x] GeoRadiusAsync
        //- [x] GeoRemoveAsync


        /// <summary>
        /// GEOADD : https://redis.io/commands/geoadd
        /// </summary>
        public Task<bool> Add(RedisGeoEntry<T> value, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
        {
            expiry = expiry ?? this.DefaultExpiry;
            var entry = value.ToNonGenerics(this.Connection.Converter);
            return this.ExecuteWithExpiry
            (
                (db, a) => db.GeoAddAsync(a.key, a.entry, a.flags),
                (key: this.Key, entry, flags),
                expiry,
                flags
            );
        }


        /// <summary>
        /// GEOADD : https://redis.io/commands/geoadd
        /// </summary>
        public Task<long> Add(IEnumerable<RedisGeoEntry<T>> values, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
        {
            expiry = expiry ?? this.DefaultExpiry;
            var entries = values.Select(this.Connection.Converter, (x, c) => x.ToNonGenerics(c)).ToArray();
            return this.ExecuteWithExpiry
            (
                (db, a) => db.GeoAddAsync(a.key, a.entries, a.flags),
                (key: this.Key, entries, flags),
                expiry,
                flags
            );
        }


        /// <summary>
        /// GEOADD : https://redis.io/commands/geoadd
        /// </summary>
        public Task<bool> Add(double longitude, double latitude, T member, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
        {
            expiry = expiry ?? this.DefaultExpiry;
            var entry = new RedisGeoEntry<T>(longitude, latitude, member);
            return this.Add(entry, expiry, flags);
        }


        /// <summary>
        /// GEODIST : https://redis.io/commands/geodist
        /// </summary>
        public Task<double?> Distance(T member1, T member2, GeoUnit unit = GeoUnit.Meters, CommandFlags flags = CommandFlags.None)
        {
            var value1 = this.Connection.Converter.Serialize(member1);
            var value2 = this.Connection.Converter.Serialize(member2);
            return this.Connection.Database.GeoDistanceAsync(this.Key, value1, value2, unit, flags);
        }


        /// <summary>
        /// GEOHASH : https://redis.io/commands/geohash
        /// </summary>
        public Task<string> Hash(T member, CommandFlags flags = CommandFlags.None)
        {
            var value = this.Connection.Converter.Serialize(member);
            return this.Connection.Database.GeoHashAsync(this.Key, value, flags);
        }


        /// <summary>
        /// GEOHASH : https://redis.io/commands/geohash
        /// </summary>
        public Task<string[]> Hash(IEnumerable<T> members, CommandFlags flags = CommandFlags.None)
        {
            var values = members.Select(this.Connection.Converter.Serialize).ToArray();
            return this.Connection.Database.GeoHashAsync(this.Key, values, flags);
        }

        
        /// <summary>
        /// GEOPOS : https://redis.io/commands/geopos
        /// </summary>
        public Task<GeoPosition?> Position(T member, CommandFlags flags = CommandFlags.None)
        {
            var value = this.Connection.Converter.Serialize(member);
            return this.Connection.Database.GeoPositionAsync(this.Key, value, flags);
        }


        /// <summary>
        /// GEOPOS  https://redis.io/commands/geopos
        /// </summary>
        public Task<GeoPosition?[]> Position(IEnumerable<T> members, CommandFlags flags = CommandFlags.None)
        {
            var values = members.Select(this.Connection.Converter.Serialize).ToArray();
            return this.Connection.Database.GeoPositionAsync(this.Key, values, flags);
        }


        /// <summary>
        /// GEORADIUS : https://redis.io/commands/georadius
        /// </summary>
        public async Task<RedisGeoRadiusResult<T>[]> Radius(RedisKey key, double longitude, double latitude, double radius, GeoUnit unit = GeoUnit.Meters, int count = -1, Order? order = null, GeoRadiusOptions options = GeoRadiusOptions.Default, CommandFlags flags = CommandFlags.None)
        {
            var results = await this.Connection.Database.GeoRadiusAsync(this.Key, longitude, latitude, radius, unit, count, order, options, flags).ConfigureAwait(false);
            return results.Select(this.Connection.Converter, (x, c) => x.ToGenerics<T>(c)).ToArray();
        }

        
        /// <summary>
        /// GEORADIUSBYMEMBER : https://redis.io/commands/georadiusbymember
        /// </summary>
        public async Task<RedisGeoRadiusResult<T>[]> Radius(T member, double radius, GeoUnit unit = GeoUnit.Meters, int count = -1, Order? order = null, GeoRadiusOptions options = GeoRadiusOptions.Default, CommandFlags flags = CommandFlags.None)
        {
            var value = this.Connection.Converter.Serialize(member);
            var results = await this.Connection.Database.GeoRadiusAsync(this.Key, value, radius, unit, count, order, options, flags).ConfigureAwait(false);
            return results.Select(this.Connection.Converter, (x, c) => x.ToGenerics<T>(c)).ToArray();
        }


        /// <summary>
        /// ZREM : https://redis.io/commands/zrem
        /// </summary>
        /// <remarks>There is no GEODEL command.</remarks>
        public Task<bool> Remove(T member, CommandFlags flags = CommandFlags.None)
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
    public readonly struct RedisGeoEntry<T>
    {
        #region Properties
        /// <summary>
        /// Gets longitude.
        /// </summary>
        public double Longitude { get; }


        /// <summary>
        /// Gets latitude.
        /// </summary>
        public double Latitude { get; }


        /// <summary>
        /// Gets member.
        /// </summary>
        public T Member { get; }
        #endregion


        #region Constructors
        /// <summary>
        /// Creates instance.
        /// </summary>
        /// <param name="longitude"></param>
        /// <param name="latitude"></param>
        /// <param name="member"></param>
        public RedisGeoEntry(double longitude, double latitude, T member)
        {
            this.Longitude = longitude;
            this.Latitude = latitude;
            this.Member = member;
        }
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
    /// Represents <see cref="RedisGeo{T}.Radius"/> result.
    /// </summary>
    /// <typeparam name="T">Data type</typeparam>
    public readonly struct RedisGeoRadiusResult<T>
    {
        #region Properties
        /// <summary>
        /// Gets member.
        /// </summary>
        public RedisResult<T> Member { get; }


        /// <summary>
        /// Gets distance.
        /// </summary>
        public double? Distance { get; }


        /// <summary>
        /// Gets hash.
        /// </summary>
        public long? Hash { get; }


        /// <summary>
        /// Gets position.
        /// </summary>
        public GeoPosition? Position { get; }
        #endregion


        #region Constructors
        /// <summary>
        /// Creates instance.
        /// </summary>
        /// <param name="member"></param>
        /// <param name="distance"></param>
        /// <param name="hash"></param>
        /// <param name="position"></param>
        internal RedisGeoRadiusResult(RedisResult<T> member, double? distance, long? hash, GeoPosition? position)
        {
            this.Member = member;
            this.Distance = distance;
            this.Hash = hash;
            this.Position = position;
        }
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
}
