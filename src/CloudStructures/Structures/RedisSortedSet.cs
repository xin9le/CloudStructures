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
    /// SortedSet 関連のコマンドを提供します。
    /// </summary>
    /// <typeparam name="T">データ型</typeparam>
    public readonly struct RedisSortedSet<T> : IRedisStructure
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
        public RedisSortedSet(RedisConnection connection, RedisKey key, TimeSpan? defaultExpiry)
        {
            this.Connection = connection ?? throw new ArgumentNullException(nameof(connection));
            this.Key = key;
            this.DefaultExpiry = defaultExpiry;
        }
        #endregion


        #region コマンド
        //- [x] SortedSetAddAsync
        //- [x] SortedSetCombineAndStoreAsync
        //- [x] SortedSetDecrementAsync
        //- [x] SortedSetIncrementAsync
        //- [x] SortedSetLengthAsync
        //- [x] SortedSetLengthByValueAsync
        //- [x] SortedSetRangeByRankAsync
        //- [x] SortedSetRangeByRankWithScoresAsync
        //- [x] SortedSetRangeByScoreAsync
        //- [x] SortedSetRangeByScoreWithScoresAsync
        //- [x] SortedSetRangeByValueAsync
        //- [x] SortedSetRankAsync
        //- [x] SortedSetRemoveAsync
        //- [x] SortedSetRemoveRangeByRankAsync
        //- [x] SortedSetRemoveRangeByScoreAsync
        //- [x] SortedSetRemoveRangeByValueAsync
        //- [x] SortedSetScoreAsync
        //- [x] SortAndStoreAsync
        //- [x] SortAsync


        /// <summary>
        /// ZADD : http://redis.io/commands/zadd
        /// </summary>
        public Task<bool> Add(T value, double score, TimeSpan? expiry = null, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            expiry = expiry ?? this.DefaultExpiry;
            var serialized = this.Connection.Converter.Serialize(value);
            return this.ExecuteWithExpiry
            (
                (db, a) => db.SortedSetAddAsync(a.key, a.serialized, a.score, a.when, a.flags),
                (key: this.Key, serialized, score, when, flags),
                expiry,
                flags
            );
        }


        /// <summary>
        /// ZADD : http://redis.io/commands/zadd
        /// </summary>
        public Task<long> Add(IEnumerable<RedisSortedSetEntry<T>> entries, TimeSpan? expiry = null, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            expiry = expiry ?? this.DefaultExpiry;
            var values
                = entries
                .Select(this.Connection.Converter, (x, c) => x.ToNonGenerics(c))
                .ToArray();
            return this.ExecuteWithExpiry
            (
                (db, a) => db.SortedSetAddAsync(a.key, a.values, a.when, a.flags),
                (key: this.Key, values, when, flags),
                expiry,
                flags
            );
        }


        /// <summary>
        /// ZUNIONSTORE : https://redis.io/commands/zunionstore
        /// ZINTERSTORE : https://redis.io/commands/zinterstore
        /// </summary>
        public Task<long> CombineAndStore(SetOperation operation, RedisSortedSet<T> destination, RedisSortedSet<T> other, Aggregate aggregate = Aggregate.Sum, CommandFlags flags = CommandFlags.None)
            => this.Connection.Database.SortedSetCombineAndStoreAsync(operation, destination.Key, this.Key, other.Key, aggregate, flags);


        /// <summary>
        /// ZUNIONSTORE : https://redis.io/commands/zunionstore
        /// ZINTERSTORE : https://redis.io/commands/zinterstore
        /// </summary>
        public Task<long> CombineAndStore(SetOperation operation, RedisSortedSet<T> destination, IReadOnlyCollection<RedisSortedSet<T>> others, double[] weights = default, Aggregate aggregate = Aggregate.Sum, CommandFlags flags = CommandFlags.None)
        {
            if (others == null) throw new ArgumentNullException(nameof(others));
            if (others.Count == 0) throw new ArgumentNullException("others length is 0.");

            var keys = others.Select(x => x.Key).Concat(new []{ this.Key }).ToArray();
            return this.Connection.Database.SortedSetCombineAndStoreAsync(operation, destination.Key, keys, weights, aggregate, flags);
        }


        /// <summary>
        /// ZINCRBY : http://redis.io/commands/zincrby
        /// </summary>
        public Task<double> Decrement(T member, double value, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
        {
            expiry = expiry ?? this.DefaultExpiry;
            var serialized = this.Connection.Converter.Serialize(member);
            return this.ExecuteWithExpiry
            (
                (db, a) => db.SortedSetDecrementAsync(a.key, a.serialized, a.value, a.flags),
                (key: this.Key, serialized, value, flags),
                expiry,
                flags
            );
        }


        /// <summary>
        /// ZINCRBY : http://redis.io/commands/zincrby
        /// </summary>
        public Task<double> Increment(T member, double value, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
        {
            expiry = expiry ?? this.DefaultExpiry;
            var serialized = this.Connection.Converter.Serialize(member);
            return this.ExecuteWithExpiry
            (
                (db, a) => db.SortedSetIncrementAsync(a.key, a.serialized, a.value, a.flags),
                (key: this.Key, serialized, value, flags),
                expiry,
                flags
            );
        }


        /// <summary>
        /// ZCARD  : http://redis.io/commands/zcard
        /// ZCOUNT : http://redis.io/commands/zcount
        /// </summary>
        public Task<long> Length(double min = double.NegativeInfinity, double max = double.PositiveInfinity, Exclude exclude = Exclude.None, CommandFlags flags = CommandFlags.None)
            => this.Connection.Database.SortedSetLengthAsync(this.Key, min, max, exclude, flags);


        /// <summary>
        /// ZCARD  : http://redis.io/commands/zcard
        /// ZCOUNT : http://redis.io/commands/zcount
        /// </summary>
        public Task<long> LengthByValue(T min, T max, Exclude exclude = Exclude.None, CommandFlags flags = CommandFlags.None)
        {
            var serializedMin = this.Connection.Converter.Serialize(min);
            var serializedMax = this.Connection.Converter.Serialize(max);
            return this.Connection.Database.SortedSetLengthByValueAsync(this.Key, serializedMin, serializedMax, exclude, flags);
        }


        /// <summary>
        /// ZRANGE    : https://redis.io/commands/zrange
        /// ZREVRANGE : https://redis.io/commands/zrevrange
        /// </summary>
        public async Task<T[]> RangeByRank(long start = 0, long stop = -1, Order order = Order.Ascending, CommandFlags flags = CommandFlags.None)
        {
            var values = await this.Connection.Database.SortedSetRangeByRankAsync(this.Key, start, stop, order, flags).ConfigureAwait(false);
            return values
                .Select(this.Connection.Converter, (x, c) => c.Deserialize<T>(x))
                .ToArray();
        }


        /// <summary>
        /// ZRANGE    : https://redis.io/commands/zrange
        /// ZREVRANGE : https://redis.io/commands/zrevrange
        /// </summary>
        public async Task<RedisSortedSetEntry<T>[]> RangeByRankWithScores(long start = 0, long stop = -1, Order order = Order.Ascending, CommandFlags flags = CommandFlags.None)
        {
            var values = await this.Connection.Database.SortedSetRangeByRankWithScoresAsync(this.Key, start, stop, order, flags).ConfigureAwait(false);
            return values
                .Select(this.Connection.Converter, (x, c) => x.ToGenerics<T>(c))
                .ToArray();
        }


        /// <summary>
        /// ZRANGEBYSCORE    : https://redis.io/commands/zrangebyscore
        /// ZREVRANGEBYSCORE : https://redis.io/commands/zrevrangebyscore
        /// </summary>
        public async Task<T[]> RangeByScore(double start = double.NegativeInfinity, double stop = double.PositiveInfinity, Exclude exclude = Exclude.None, Order order = Order.Ascending, long skip = 0, long take = -1, CommandFlags flags = CommandFlags.None)
        {
            var values = await this.Connection.Database.SortedSetRangeByScoreAsync(this.Key, start, stop, exclude, order, skip, take, flags).ConfigureAwait(false);
            return values
                .Select(this.Connection.Converter, (x, c) => c.Deserialize<T>(x))
                .ToArray();
        }


        /// <summary>
        /// ZRANGEBYSCORE    : https://redis.io/commands/zrangebyscore
        /// ZREVRANGEBYSCORE : https://redis.io/commands/zrevrangebyscore
        /// </summary>
        public async Task<RedisSortedSetEntry<T>[]> RangeByScoreWithScores(double start = double.NegativeInfinity, double stop = double.PositiveInfinity, Exclude exclude = Exclude.None, Order order = Order.Ascending, long skip = 0, long take = -1, CommandFlags flags = CommandFlags.None)
        {
            var values = await this.Connection.Database.SortedSetRangeByScoreWithScoresAsync(this.Key, start, stop, exclude, order, skip, take, flags).ConfigureAwait(false);
            return values
                .Select(this.Connection.Converter, (x, c) => x.ToGenerics<T>(c))
                .ToArray();
        }


        /// <summary>
        /// ZRANGEBYLEX    : https://redis.io/commands/zrangebylex
        /// ZREVRANGEBYLEX : https://redis.io/commands/zrevrangebylex
        /// </summary>
        public async Task<T[]> RangeByValue(T min = default, T max = default, Exclude exclude = Exclude.None, long skip = 0, long take = -1, CommandFlags flags = CommandFlags.None)
        {
            var minValue = this.Connection.Converter.Serialize(min);
            var maxValue = this.Connection.Converter.Serialize(max);
            var values = await this.Connection.Database.SortedSetRangeByValueAsync(this.Key, minValue, maxValue, exclude, skip, take, flags).ConfigureAwait(false);
            return values
                .Select(this.Connection.Converter, (x, c) => c.Deserialize<T>(x))
                .ToArray();
        }


        /// <summary>
        /// ZRANK : https://redis.io/commands/zrank
        /// </summary>
        public Task<long?> Rank(T member, Order order = Order.Ascending, CommandFlags flags = CommandFlags.None)
        {
            var serialized = this.Connection.Converter.Serialize(member);
            return this.Connection.Database.SortedSetRankAsync(this.Key, serialized, order, flags);
        }


        /// <summary>
        /// ZREM : https://redis.io/commands/zrem
        /// </summary>
        public Task<bool> Remove(T member, CommandFlags flags = CommandFlags.None)
        {
            var serialized = this.Connection.Converter.Serialize(member);
            return this.Connection.Database.SortedSetRemoveAsync(this.Key, serialized, flags);
        }


        /// <summary>
        /// ZREM : https://redis.io/commands/zrem
        /// </summary>
        public Task<long> Remove(IEnumerable<T> members, CommandFlags flags = CommandFlags.None)
        {
            var serialized = members.Select(this.Connection.Converter.Serialize).ToArray();
            return this.Connection.Database.SortedSetRemoveAsync(this.Key, serialized, flags);
        }


        /// <summary>
        /// ZREMRANGEBYRANK : http://redis.io/commands/zremrangebyrank
        /// </summary>
        public Task<long> RemoveRangeByRank(long start, long stop, CommandFlags flags = CommandFlags.None)
            => this.Connection.Database.SortedSetRemoveRangeByRankAsync(this.Key, start, stop, flags);


        /// <summary>
        /// ZREMRANGEBYSCORE : https://redis.io/commands/zremrangebyscore
        /// </summary>
        public Task<long> RemoveRangeByScore(double start, double stop, Exclude exclude = Exclude.None, CommandFlags flags = CommandFlags.None)
            => this.Connection.Database.SortedSetRemoveRangeByScoreAsync(this.Key, start, stop, exclude, flags);


        /// <summary>
        /// ZREMRANGEBYLEX : https://redis.io/commands/zremrangebylex
        /// </summary>
        public Task<long> RemoveRangeByValue(T min, T max, Exclude exclude = Exclude.None, CommandFlags flags = CommandFlags.None)
        {
            var minValue = this.Connection.Converter.Serialize(min);
            var maxValue = this.Connection.Converter.Serialize(max);
            return this.Connection.Database.SortedSetRemoveRangeByValueAsync(this.Key, minValue, maxValue, exclude, flags);
        }   


        /// <summary>
        /// ZSCORE : https://redis.io/commands/zscore
        /// </summary>
        public Task<double?> Score(T member, CommandFlags flags = CommandFlags.None)
        {
            var serialized = this.Connection.Converter.Serialize(member);
            return this.Connection.Database.SortedSetScoreAsync(this.Key, serialized, flags);
        }


        /// <summary>
        /// SORT : https://redis.io/commands/sort
        /// </summary>
        public Task<long> SortAndStore(RedisSortedSet<T> destination, long skip = 0, long take = -1, Order order = Order.Ascending, SortType sortType = SortType.Numeric, /*RedisValue by = default, RedisValue[] get = null,*/ CommandFlags flags = CommandFlags.None)
        {
            //--- シリアライズが必要かどうか分からないから、とりあえず既定値固定にする
            RedisValue by = default;
            RedisValue[] get = default;
            return this.Connection.Database.SortAndStoreAsync(destination.Key, this.Key, skip, take, order, sortType, by, get, flags);
        }


        /// <summary>
        /// SORT : https://redis.io/commands/sort
        /// </summary>
        public async Task<T[]> Sort(long skip = 0, long take = -1, Order order = Order.Ascending, SortType sortType = SortType.Numeric, /*RedisValue by = default, RedisValue[] get = null,*/ CommandFlags flags = CommandFlags.None)
        {
            //--- シリアライズが必要かどうか分からないから、とりあえず既定値固定にする
            RedisValue by = default;
            RedisValue[] get = default;
            var values = await this.Connection.Database.SortAsync(this.Key, skip, take, order, sortType, by, get, flags).ConfigureAwait(false);
            return values.Select(this.Connection.Converter, (x, c) => c.Deserialize<T>(x)).ToArray();
        }
        #endregion


        #region カスタムコマンド
        /// <summary>
        /// LUA Script including zincrby, zadd
        /// </summary>
        public async Task<double> IncrementLimitByMin(T member, double value, double min, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
        {
            expiry = expiry ?? this.DefaultExpiry;
            var script =
@"local mem = ARGV[1]
local inc = tonumber(ARGV[2])
local min = tonumber(ARGV[3])
local x = tonumber(redis.call('zincrby', KEYS[1], inc, mem))
if(x < min) then
    redis.call('zadd', KEYS[1], min, mem)
    x = min
end
return tostring(x)";
            var keys = new[] { this.Key };
            var serialized = this.Connection.Converter.Serialize(member);
            var values = new RedisValue[] { serialized, value, min };
            var result
                = await this.ExecuteWithExpiry
                (
                    (db, a) => db.ScriptEvaluateAsync(a.script, a.keys, a.values, a.flags),
                    (script, keys, values, flags),
                    expiry,
                    flags
                )
                .ConfigureAwait(false);
            return double.Parse((string)result);
        }


        /// <summary>
        /// LUA Script including zincrby, zadd
        /// </summary>
        public async Task<double> IncrementLimitByMax(T member, double value, double max, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
        {
            expiry = expiry ?? this.DefaultExpiry;
            var script =
@"local mem = ARGV[1]
local inc = tonumber(ARGV[2])
local max = tonumber(ARGV[3])
local x = tonumber(redis.call('zincrby', KEYS[1], inc, mem))
if(x > max) then
    redis.call('zadd', KEYS[1], max, mem)
    x = max
end
return tostring(x)";
            var keys = new[] { this.Key };
            var serialized = this.Connection.Converter.Serialize(member);
            var values = new RedisValue[] { serialized, value, max };
            var result
                = await this.ExecuteWithExpiry
                (
                    (db, a) => db.ScriptEvaluateAsync(a.script, a.keys, a.values, a.flags),
                    (script, keys, values, flags),
                    expiry,
                    flags
                )
                .ConfigureAwait(false);
            return double.Parse((string)result);
        }
        #endregion
    }



    /// <summary>
    /// <see cref="RedisSortedSet{T}"/> の要素を表します。
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public readonly struct RedisSortedSetEntry<T>
    {
        #region プロパティ
        /// <summary>
        /// 値を取得します。
        /// </summary>
        public T Value { get; }


        /// <summary>
        /// スコアを取得します。
        /// </summary>
        public double Score { get; }
        #endregion


        #region コンストラクタ
        /// <summary>
        /// インスタンスを生成します。
        /// </summary>
        /// <param name="value">値</param>
        /// <param name="score">スコア</param>
        public RedisSortedSetEntry(T value, double score)
        {
            this.Value = value;
            this.Score = score;
        }
        #endregion
    }



    /// <summary>
    /// <see cref="RedisSortedSetEntry{T}"/> の拡張機能を提供します。
    /// </summary>
    internal static class RedisSortedSetEntryExtensions
    {
        /// <summary>
        /// <see cref="SortedSetEntry"/> に変換します。
        /// </summary>
        /// <typeparam name="T">データの型</typeparam>
        /// <param name="entry">要素</param>
        /// <param name="converter">値変換機能</param>
        /// <returns></returns>
        public static SortedSetEntry ToNonGenerics<T>(this in RedisSortedSetEntry<T> entry, ValueConverter converter)
        {
            var value = converter.Serialize(entry.Value);
            return new SortedSetEntry(value, entry.Score);
        }


        /// <summary>
        /// <see cref="RedisSortedSetEntry{T}"/> に変換します。
        /// </summary>
        /// <typeparam name="T">データの型</typeparam>
        /// <param name="entry">要素</param>
        /// <param name="converter">値変換機能</param>
        /// <returns></returns>
        public static RedisSortedSetEntry<T> ToGenerics<T>(this in SortedSetEntry entry, ValueConverter converter)
        {
            var value = converter.Deserialize<T>(entry.Element);
            return new RedisSortedSetEntry<T>(value, entry.Score);
        }
    }
}
