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
    /// Geo 関連のコマンドを提供します。
    /// </summary>
    /// <typeparam name="T">データ型</typeparam>
    public readonly struct RedisGeo<T> : IRedisStructure
    {
        #region IRedisStructure implementations
        /// <summary>
        /// 接続を取得します。
        /// </summary>
        public RedisConnection Connection { get; }


        /// <summary>
        /// キーを取得します。
        /// </summary>
        public RedisKey Key { get; }


        /// <summary>
        /// 既定の有効期限を取得します。
        /// </summary>
        public TimeSpan? DefaultExpiry { get; }
        #endregion


        #region コンストラクタ
        /// <summary>
        /// インスタンスを生成します。
        /// </summary>
        /// <param name="connection">接続</param>
        /// <param name="key">キー</param>
        /// <param name="defaultExpiry">既定の有効期限</param>
        public RedisGeo(RedisConnection connection, RedisKey key, TimeSpan? defaultExpiry)
        {
            this.Connection = connection ?? throw new ArgumentNullException(nameof(connection));
            this.Key = key;
            this.DefaultExpiry = defaultExpiry;
        }
        #endregion


        #region コマンド
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
    /// <see cref="RedisGeo{T}"/> の要素を表します。
    /// </summary>
    /// <typeparam name="T">データの型</typeparam>
    public readonly struct RedisGeoEntry<T>
    {
        #region プロパティ
        /// <summary>
        /// 経度を取得します。
        /// </summary>
        public double Longitude { get; }


        /// <summary>
        /// 緯度を取得します。
        /// </summary>
        public double Latitude { get; }


        /// <summary>
        /// メンバーを取得します。
        /// </summary>
        public T Member { get; }
        #endregion


        #region コンストラクタ
        /// <summary>
        /// インスタンスを生成します。
        /// </summary>
        /// <param name="longitude">経度</param>
        /// <param name="latitude">緯度</param>
        /// <param name="member">メンバー</param>
        public RedisGeoEntry(double longitude, double latitude, T member)
        {
            this.Longitude = longitude;
            this.Latitude = latitude;
            this.Member = member;
        }
        #endregion
    }



    /// <summary>
    /// <see cref="RedisGeoEntry{T}"/> の拡張機能を提供します。
    /// </summary>
    internal static class RedisGeoEntryExtensions
    {
        /// <summary>
        /// <see cref="GeoEntry"/> に変換します。
        /// </summary>
        /// <typeparam name="T">データの型</typeparam>
        /// <param name="entry">要素</param>
        /// <param name="converter">値変換機能</param>
        /// <returns></returns>
        public static GeoEntry ToNonGenerics<T>(this in RedisGeoEntry<T> entry, ValueConverter converter)
        {
            var member = converter.Serialize(entry.Member);
            return new GeoEntry(entry.Longitude, entry.Latitude, member);
        }
    }



    /// <summary>
    /// <see cref="RedisGeo{T}.Radius"/> の結果型を表します。
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public readonly struct RedisGeoRadiusResult<T>
    {
        #region プロパティ
        /// <summary>
        /// メンバーを取得します。
        /// </summary>
        public RedisResult<T> Member { get; }


        /// <summary>
        /// 距離を取得します。
        /// </summary>
        public double? Distance { get; }


        /// <summary>
        /// ハッシュを取得します。
        /// </summary>
        public long? Hash { get; }


        /// <summary>
        /// 位置を取得します。
        /// </summary>
        public GeoPosition? Position { get; }
        #endregion


        #region コンストラクタ
        /// <summary>
        /// インスタンスを生成します。
        /// </summary>
        /// <param name="member">メンバー</param>
        /// <param name="distance">距離</param>
        /// <param name="hash">ハッシュ</param>
        /// <param name="position">位置</param>
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
    /// <see cref="RedisGeoRadiusResult{T}"/> の拡張機能を提供します。
    /// </summary>
    internal static class RedisGeoRadiusResultExtensions
    {
        /// <summary>
        /// <see cref="RedisGeoRadiusResult{T}"/> に変換します。
        /// </summary>
        /// <typeparam name="T">データ型</typeparam>
        /// <param name="result">結果</param>
        /// <param name="converter">値変換機能</param>
        /// <returns></returns>
        public static RedisGeoRadiusResult<T> ToGenerics<T>(this in GeoRadiusResult result, ValueConverter converter)
        {
            var member = result.Member.ToResult<T>(converter);
            return new RedisGeoRadiusResult<T>(member, result.Distance, result.Hash, result.Position);
        }
    }
}
