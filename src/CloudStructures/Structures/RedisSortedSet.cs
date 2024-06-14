using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CloudStructures.Converters;
using CloudStructures.Internals;
using StackExchange.Redis;

namespace CloudStructures.Structures;



/// <summary>
/// Provides sorted set related commands.
/// </summary>
/// <typeparam name="T">Data type</typeparam>
public readonly struct RedisSortedSet<T> : IRedisStructureWithExpiry
{
    #region IRedisStructureWithExpiry implementations
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
    public RedisSortedSet(RedisConnection connection, RedisKey key, TimeSpan? defaultExpiry)
    {
        this.Connection = connection;
        this.Key = key;
        this.DefaultExpiry = defaultExpiry;
    }
    #endregion


    #region Commands
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
    /// ZADD : <a href="http://redis.io/commands/zadd"></a>
    /// </summary>
    public Task<bool> AddAsync(T value, double score, TimeSpan? expiry = null, When when = When.Always, CommandFlags flags = CommandFlags.None)
    {
        expiry ??= this.DefaultExpiry;
        var serialized = this.Connection.Converter.Serialize(value);
        return this.ExecuteWithExpiryAsync
        (
            static (db, state) => db.SortedSetAddAsync(state.key, state.serialized, state.score, state.when, state.flags),
            state: (key: this.Key, serialized, score, when, flags),
            expiry,
            flags
        );
    }


    /// <summary>
    /// ZADD : <a href="http://redis.io/commands/zadd"></a>
    /// </summary>
    public Task<long> AddAsync(IEnumerable<RedisSortedSetEntry<T>> entries, TimeSpan? expiry = null, When when = When.Always, CommandFlags flags = CommandFlags.None)
    {
        expiry ??= this.DefaultExpiry;
        var values
            = entries
            .Select(this.Connection.Converter, static (x, c) => x.ToNonGenerics(c))
            .ToArray();
        return this.ExecuteWithExpiryAsync
        (
            static (db, state) => db.SortedSetAddAsync(state.key, state.values, state.when, state.flags),
            state: (key: this.Key, values, when, flags),
            expiry,
            flags
        );
    }


    /// <summary>
    /// ZUNIONSTORE : <a href="https://redis.io/commands/zunionstore"></a><br/>
    /// ZINTERSTORE : <a href="https://redis.io/commands/zinterstore"></a>
    /// </summary>
    public Task<long> CombineAndStoreAsync(SetOperation operation, RedisSortedSet<T> destination, RedisSortedSet<T> other, Aggregate aggregate = Aggregate.Sum, CommandFlags flags = CommandFlags.None)
        => this.Connection.Database.SortedSetCombineAndStoreAsync(operation, destination.Key, this.Key, other.Key, aggregate, flags);


    /// <summary>
    /// ZUNIONSTORE : <a href="https://redis.io/commands/zunionstore"></a><br/>
    /// ZINTERSTORE : <a href="https://redis.io/commands/zinterstore"></a>
    /// </summary>
    public Task<long> CombineAndStoreAsync(SetOperation operation, RedisSortedSet<T> destination, IReadOnlyCollection<RedisSortedSet<T>> others, double[]? weights = default, Aggregate aggregate = Aggregate.Sum, CommandFlags flags = CommandFlags.None)
    {
        if (others.Count == 0)
            throw new ArgumentException("others length is 0.");

#if NETSTANDARD2_1 || NET5_0_OR_GREATER
        var keys = others.Select(static x => x.Key).Append(this.Key).ToArray();
#else
        var keys = others.Select(x => x.Key).Concat(new[] { this.Key }).ToArray();
#endif
        return this.Connection.Database.SortedSetCombineAndStoreAsync(operation, destination.Key, keys, weights, aggregate, flags);
    }


    /// <summary>
    /// ZINCRBY : <a href="http://redis.io/commands/zincrby"></a>
    /// </summary>
    public Task<double> DecrementAsync(T member, double value, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
    {
        expiry ??= this.DefaultExpiry;
        var serialized = this.Connection.Converter.Serialize(member);
        return this.ExecuteWithExpiryAsync
        (
            static (db, state) => db.SortedSetDecrementAsync(state.key, state.serialized, state.value, state.flags),
            state: (key: this.Key, serialized, value, flags),
            expiry,
            flags
        );
    }


    /// <summary>
    /// ZINCRBY : <a href="http://redis.io/commands/zincrby"></a>
    /// </summary>
    public Task<double> IncrementAsync(T member, double value, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
    {
        expiry ??= this.DefaultExpiry;
        var serialized = this.Connection.Converter.Serialize(member);
        return this.ExecuteWithExpiryAsync
        (
            static (db, state) => db.SortedSetIncrementAsync(state.key, state.serialized, state.value, state.flags),
            state: (key: this.Key, serialized, value, flags),
            expiry,
            flags
        );
    }


    /// <summary>
    /// ZCARD  : <a href="http://redis.io/commands/zcard"></a><br/>
    /// ZCOUNT : <a href="http://redis.io/commands/zcount"></a>
    /// </summary>
    public Task<long> LengthAsync(double min = double.NegativeInfinity, double max = double.PositiveInfinity, Exclude exclude = Exclude.None, CommandFlags flags = CommandFlags.None)
        => this.Connection.Database.SortedSetLengthAsync(this.Key, min, max, exclude, flags);


    /// <summary>
    /// ZCARD  : <a href="http://redis.io/commands/zcard"></a><br/>
    /// ZCOUNT : <a href="http://redis.io/commands/zcount"></a>
    /// </summary>
    public Task<long> LengthByValueAsync(T min, T max, Exclude exclude = Exclude.None, CommandFlags flags = CommandFlags.None)
    {
        var serializedMin = this.Connection.Converter.Serialize(min);
        var serializedMax = this.Connection.Converter.Serialize(max);
        return this.Connection.Database.SortedSetLengthByValueAsync(this.Key, serializedMin, serializedMax, exclude, flags);
    }


    /// <summary>
    /// ZRANGE    : <a href="https://redis.io/commands/zrange"></a><br/>
    /// ZREVRANGE : <a href="https://redis.io/commands/zrevrange"></a>
    /// </summary>
    public async Task<T[]> RangeByRankAsync(long start = 0, long stop = -1, Order order = Order.Ascending, CommandFlags flags = CommandFlags.None)
    {
        var values = await this.Connection.Database.SortedSetRangeByRankAsync(this.Key, start, stop, order, flags).ConfigureAwait(false);
        return values
            .Select(this.Connection.Converter, static (x, c) => c.Deserialize<T>(x))
            .ToArray();
    }


    /// <summary>
    /// ZRANGE    : <a href="https://redis.io/commands/zrange"></a><br/>
    /// ZREVRANGE : <a href="https://redis.io/commands/zrevrange"></a>
    /// </summary>
    public async Task<RedisSortedSetEntry<T>[]> RangeByRankWithScoresAsync(long start = 0, long stop = -1, Order order = Order.Ascending, CommandFlags flags = CommandFlags.None)
    {
        var values = await this.Connection.Database.SortedSetRangeByRankWithScoresAsync(this.Key, start, stop, order, flags).ConfigureAwait(false);
        return values
            .Select(this.Connection.Converter, static (x, c) => x.ToGenerics<T>(c))
            .ToArray();
    }


    /// <summary>
    /// ZRANGEBYSCORE    : <a href="https://redis.io/commands/zrangebyscore"></a><br/>
    /// ZREVRANGEBYSCORE : <a href="https://redis.io/commands/zrevrangebyscore"></a>
    /// </summary>
    public async Task<T[]> RangeByScoreAsync(double start = double.NegativeInfinity, double stop = double.PositiveInfinity, Exclude exclude = Exclude.None, Order order = Order.Ascending, long skip = 0, long take = -1, CommandFlags flags = CommandFlags.None)
    {
        var values = await this.Connection.Database.SortedSetRangeByScoreAsync(this.Key, start, stop, exclude, order, skip, take, flags).ConfigureAwait(false);
        return values
            .Select(this.Connection.Converter, static (x, c) => c.Deserialize<T>(x))
            .ToArray();
    }


    /// <summary>
    /// ZRANGEBYSCORE    : <a href="https://redis.io/commands/zrangebyscore"></a><br/>
    /// ZREVRANGEBYSCORE : <a href="https://redis.io/commands/zrevrangebyscore"></a>
    /// </summary>
    public async Task<RedisSortedSetEntry<T>[]> RangeByScoreWithScoresAsync(double start = double.NegativeInfinity, double stop = double.PositiveInfinity, Exclude exclude = Exclude.None, Order order = Order.Ascending, long skip = 0, long take = -1, CommandFlags flags = CommandFlags.None)
    {
        var values = await this.Connection.Database.SortedSetRangeByScoreWithScoresAsync(this.Key, start, stop, exclude, order, skip, take, flags).ConfigureAwait(false);
        return values
            .Select(this.Connection.Converter, static (x, c) => x.ToGenerics<T>(c))
            .ToArray();
    }


    /// <summary>
    /// ZRANGEBYLEX    : <a href="https://redis.io/commands/zrangebylex"></a><br/>
    /// ZREVRANGEBYLEX : <a href="https://redis.io/commands/zrevrangebylex"></a>
    /// </summary>
    public async Task<T[]> RangeByValueAsync(T min, T max, Exclude exclude, long skip, long take = -1, CommandFlags flags = CommandFlags.None)
    {
        var minValue = this.Connection.Converter.Serialize(min);
        var maxValue = this.Connection.Converter.Serialize(max);
        var values = await this.Connection.Database.SortedSetRangeByValueAsync(this.Key, minValue, maxValue, exclude, skip, take, flags).ConfigureAwait(false);
        return values
            .Select(this.Connection.Converter, static (x, c) => c.Deserialize<T>(x))
            .ToArray();
    }


    /// <summary>
    /// ZRANGEBYLEX    : <a href="https://redis.io/commands/zrangebylex"></a><br/>
    /// ZREVRANGEBYLEX : <a href="https://redis.io/commands/zrevrangebylex"></a>
    /// </summary>
    public async Task<T[]> RangeByValueAsync(T? min = default, T? max = default, Exclude exclude = Exclude.None, Order order = Order.Ascending, long skip = 0, long take = -1, CommandFlags flags = CommandFlags.None)
    {
        var minValue = this.Connection.Converter.Serialize(min);
        var maxValue = this.Connection.Converter.Serialize(max);
        var values = await this.Connection.Database.SortedSetRangeByValueAsync(this.Key, minValue, maxValue, exclude, order, skip, take, flags).ConfigureAwait(false);
        return values
            .Select(this.Connection.Converter, static (x, c) => c.Deserialize<T>(x))
            .ToArray();
    }


    /// <summary>
    /// ZRANK : <a href="https://redis.io/commands/zrank"></a>
    /// </summary>
    public Task<long?> RankAsync(T member, Order order = Order.Ascending, CommandFlags flags = CommandFlags.None)
    {
        var serialized = this.Connection.Converter.Serialize(member);
        return this.Connection.Database.SortedSetRankAsync(this.Key, serialized, order, flags);
    }


    /// <summary>
    /// ZREM : <a href="https://redis.io/commands/zrem"></a>
    /// </summary>
    public Task<bool> RemoveAsync(T member, CommandFlags flags = CommandFlags.None)
    {
        var serialized = this.Connection.Converter.Serialize(member);
        return this.Connection.Database.SortedSetRemoveAsync(this.Key, serialized, flags);
    }


    /// <summary>
    /// ZREM : <a href="https://redis.io/commands/zrem"></a>
    /// </summary>
    public Task<long> RemoveAsync(IEnumerable<T> members, CommandFlags flags = CommandFlags.None)
    {
        var serialized = members.Select(this.Connection.Converter.Serialize).ToArray();
        return this.Connection.Database.SortedSetRemoveAsync(this.Key, serialized, flags);
    }


    /// <summary>
    /// ZREMRANGEBYRANK : <a href="http://redis.io/commands/zremrangebyrank"></a>
    /// </summary>
    public Task<long> RemoveRangeByRankAsync(long start, long stop, CommandFlags flags = CommandFlags.None)
        => this.Connection.Database.SortedSetRemoveRangeByRankAsync(this.Key, start, stop, flags);


    /// <summary>
    /// ZREMRANGEBYSCORE : <a href="https://redis.io/commands/zremrangebyscore"></a>
    /// </summary>
    public Task<long> RemoveRangeByScoreAsync(double start, double stop, Exclude exclude = Exclude.None, CommandFlags flags = CommandFlags.None)
        => this.Connection.Database.SortedSetRemoveRangeByScoreAsync(this.Key, start, stop, exclude, flags);


    /// <summary>
    /// ZREMRANGEBYLEX : <a href="https://redis.io/commands/zremrangebylex"></a>
    /// </summary>
    public Task<long> RemoveRangeByValueAsync(T min, T max, Exclude exclude = Exclude.None, CommandFlags flags = CommandFlags.None)
    {
        var minValue = this.Connection.Converter.Serialize(min);
        var maxValue = this.Connection.Converter.Serialize(max);
        return this.Connection.Database.SortedSetRemoveRangeByValueAsync(this.Key, minValue, maxValue, exclude, flags);
    }


    /// <summary>
    /// ZSCORE : <a href="https://redis.io/commands/zscore"></a>
    /// </summary>
    public Task<double?> ScoreAsync(T member, CommandFlags flags = CommandFlags.None)
    {
        var serialized = this.Connection.Converter.Serialize(member);
        return this.Connection.Database.SortedSetScoreAsync(this.Key, serialized, flags);
    }


    /// <summary>
    /// SORT : <a href="https://redis.io/commands/sort"></a>
    /// </summary>
    public Task<long> SortAndStoreAsync(RedisSortedSet<T> destination, long skip = 0, long take = -1, Order order = Order.Ascending, SortType sortType = SortType.Numeric, /*RedisValue by = default, RedisValue[] get = null,*/ CommandFlags flags = CommandFlags.None)
    {
        //--- I don't know if serialization is necessary or not, so I will fix the default value.
        RedisValue by = default;
        RedisValue[]? get = default;
        return this.Connection.Database.SortAndStoreAsync(destination.Key, this.Key, skip, take, order, sortType, by, get, flags);
    }


    /// <summary>
    /// SORT : <a href="https://redis.io/commands/sort"></a>
    /// </summary>
    public async Task<T[]> SortAsync(long skip = 0, long take = -1, Order order = Order.Ascending, SortType sortType = SortType.Numeric, /*RedisValue by = default, RedisValue[] get = null,*/ CommandFlags flags = CommandFlags.None)
    {
        //--- I don't know if serialization is necessary or not, so I will fix the default value.
        RedisValue by = default;
        RedisValue[]? get = default;
        var values = await this.Connection.Database.SortAsync(this.Key, skip, take, order, sortType, by, get, flags).ConfigureAwait(false);
        return values.Select(this.Connection.Converter, static (x, c) => c.Deserialize<T>(x)).ToArray();
    }
    #endregion


    #region Custom Commands
    /// <summary>
    /// LUA Script including zincrby, zadd
    /// </summary>
    public async Task<double> IncrementLimitByMinAsync(T member, double value, double min, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
    {
        expiry ??= this.DefaultExpiry;
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
            = await this.ExecuteWithExpiryAsync
            (
                static (db, state) => db.ScriptEvaluateAsync(state.script, state.keys, state.values, state.flags),
                state: (script, keys, values, flags),
                expiry,
                flags
            )
            .ConfigureAwait(false);
        return (double)result;
    }


    /// <summary>
    /// LUA Script including zincrby, zadd
    /// </summary>
    public async Task<double> IncrementLimitByMaxAsync(T member, double value, double max, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
    {
        expiry ??= this.DefaultExpiry;
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
            = await this.ExecuteWithExpiryAsync
            (
                static (db, state) => db.ScriptEvaluateAsync(state.script, state.keys, state.values, state.flags),
                state: (script, keys, values, flags),
                expiry,
                flags
            )
            .ConfigureAwait(false);
        return (double)result;
    }
    #endregion
}



/// <summary>
/// Represents <see cref="RedisSortedSet{T}"/> element.
/// </summary>
/// <typeparam name="T">Data type</typeparam>
public readonly struct RedisSortedSetEntry<T>
{
    #region Properties
    /// <summary>
    /// Gets value.
    /// </summary>
    public T Value { get; }


    /// <summary>
    /// Gets score.
    /// </summary>
    public double Score { get; }
    #endregion


    #region Constructors
    /// <summary>
    /// Creates instance.
    /// </summary>
    /// <param name="value"></param>
    /// <param name="score"></param>
    public RedisSortedSetEntry(T value, double score)
    {
        this.Value = value;
        this.Score = score;
    }
    #endregion
}



/// <summary>
/// Provides extension methods for <see cref="RedisSortedSetEntry{T}"/>.
/// </summary>
internal static class RedisSortedSetEntryExtensions
{
    /// <summary>
    /// Converts to <see cref="SortedSetEntry"/>.
    /// </summary>
    /// <typeparam name="T">Data type</typeparam>
    /// <param name="entry"></param>
    /// <param name="converter"></param>
    /// <returns></returns>
    public static SortedSetEntry ToNonGenerics<T>(this in RedisSortedSetEntry<T> entry, ValueConverter converter)
    {
        var value = converter.Serialize(entry.Value);
        return new(value, entry.Score);
    }


    /// <summary>
    /// Converts to <see cref="RedisSortedSetEntry{T}"/>.
    /// </summary>
    /// <typeparam name="T">Data type</typeparam>
    /// <param name="entry"></param>
    /// <param name="converter"></param>
    /// <returns></returns>
    public static RedisSortedSetEntry<T> ToGenerics<T>(this in SortedSetEntry entry, ValueConverter converter)
    {
        var value = converter.Deserialize<T>(entry.Element);
        return new(value, entry.Score);
    }
}
