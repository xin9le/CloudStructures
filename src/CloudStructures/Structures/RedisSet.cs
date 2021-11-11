using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CloudStructures.Internals;
using StackExchange.Redis;

namespace CloudStructures.Structures;



/// <summary>
/// Provides set related commands.
/// </summary>
/// <typeparam name="T">Data type</typeparam>
public readonly struct RedisSet<T> : IRedisStructureWithExpiry
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
    public RedisSet(RedisConnection connection, RedisKey key, TimeSpan? defaultExpiry)
    {
        this.Connection = connection;
        this.Key = key;
        this.DefaultExpiry = defaultExpiry;
    }
    #endregion


    #region Commands
    //- [x] SetAddAsync
    //- [x] SetCombineAndStoreAsync
    //- [x] SetCombineAsync
    //- [x] SetContainsAsync
    //- [x] SetLengthAsync
    //- [x] SetMembersAsync
    //- [x] SetMoveAsync
    //- [x] SetPopAsync
    //- [x] SetRandomMemberAsync
    //- [x] SetRandomMembersAsync
    //- [x] SetRemoveAsync
    //- [x] SortAndStoreAsync
    //- [x] SortAsync


    /// <summary>
    /// SADD : <a href="http://redis.io/commands/sadd"></a>
    /// </summary>
    public Task<bool> AddAsync(T value, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
    {
        expiry ??= this.DefaultExpiry;
        var serialised = this.Connection.Converter.Serialize(value);
        return this.ExecuteWithExpiryAsync
        (
            static (db, state) => db.SetAddAsync(state.key, state.serialised, state.flags),
            state: (key: this.Key, serialised, flags),
            expiry,
            flags
        );
    }


    /// <summary>
    /// SADD : <a href="http://redis.io/commands/sadd"></a>
    /// </summary>
    public Task<long> AddAsync(IEnumerable<T> values, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
    {
        expiry ??= this.DefaultExpiry;
        var serialised = values.Select(this.Connection.Converter.Serialize).ToArray();
        return this.ExecuteWithExpiryAsync
        (
            static (db, state) => db.SetAddAsync(state.key, state.serialised, state.flags),
            state: (key: this.Key, serialised, flags),
            expiry,
            flags
        );
    }


    /// <summary>
    /// SDIFFSTORE  : <a href="https://redis.io/commands/sdiffstore"></a><br/>
    /// SINTERSTORE : <a href="https://redis.io/commands/sinterstore"></a><br/>
    /// SUNIONSTORE : <a href="https://redis.io/commands/sunionstore"></a>
    /// </summary>
    /// <remarks>
    /// Combine self and other, then save it to the destination.
    /// It does not work unless you pass keys located the same server.
    /// </remarks>
    public Task<long> CombineAndStoreAsync(SetOperation operation, RedisSet<T> destination, RedisSet<T> other, CommandFlags flags = CommandFlags.None)
        => this.Connection.Database.SetCombineAndStoreAsync(operation, destination.Key, this.Key, other.Key, flags);


    /// <summary>
    /// SDIFFSTORE  : <a href="https://redis.io/commands/sdiffstore"></a><br/>
    /// SINTERSTORE : <a href="https://redis.io/commands/sinterstore"></a><br/>
    /// SUNIONSTORE : <a href="https://redis.io/commands/sunionstore"></a>
    /// </summary>
    /// <remarks>
    /// Combine self and other, then save it to the destination.
    /// It does not work unless you pass keys located the same server.
    /// </remarks>
    public Task<long> CombineAndStoreAsync(SetOperation operation, RedisSet<T> destination, IReadOnlyCollection<RedisSet<T>> others, CommandFlags flags = CommandFlags.None)
    {
        if (others.Count == 0)
            throw new ArgumentException("others length is 0.");

#if NETSTANDARD2_1 || NET5_0_OR_GREATER
        var keys = others.Select(static x => x.Key).Append(this.Key).ToArray();
#else
        var keys = others.Select(static x => x.Key).Concat(new[] { this.Key }).ToArray();
#endif
        return this.Connection.Database.SetCombineAndStoreAsync(operation, destination.Key, keys, flags);
    }


    /// <summary>
    /// SDIFF  : <a href="https://redis.io/commands/sdiff"></a><br/>
    /// SINTER : <a href="https://redis.io/commands/sinter"></a><br/>
    /// SUNION : <a href="https://redis.io/commands/sunion"></a>
    /// </summary>
    /// <remarks>It does not work unless you pass keys located the same server.</remarks>
    public async Task<T[]> CombineAsync(SetOperation operation, RedisSet<T> other, CommandFlags flags = CommandFlags.None)
    {
        var values = await this.Connection.Database.SetCombineAsync(operation, this.Key, other.Key, flags).ConfigureAwait(false);
        return values.Select(this.Connection.Converter, static (x, c) => c.Deserialize<T>(x)).ToArray();
    }


    /// <summary>
    /// SDIFF  : <a href="https://redis.io/commands/sdiff"></a><br/>
    /// SINTER : <a href="https://redis.io/commands/sinter"></a><br/>
    /// SUNION : <a href="https://redis.io/commands/sunion"></a>
    /// </summary>
    /// <remarks>It does not work unless you pass keys located the same server.</remarks>
    public async Task<T[]> CombineAsync(SetOperation operation, IReadOnlyCollection<RedisSet<T>> others, CommandFlags flags = CommandFlags.None)
    {
        if (others.Count == 0)
            throw new ArgumentException("others length is 0.");

#if NETSTANDARD2_1 || NET5_0_OR_GREATER
        var keys = others.Select(static x => x.Key).Append(this.Key).ToArray();
#else
        var keys = others.Select(static x => x.Key).Concat(new[] { this.Key }).ToArray();
#endif
        var values = await this.Connection.Database.SetCombineAsync(operation, keys, flags).ConfigureAwait(false);
        return values.Select(this.Connection.Converter, static (x, c) => c.Deserialize<T>(x)).ToArray();
    }


    /// <summary>
    /// SISMEMBER : <a href="http://redis.io/commands/sismember"></a>
    /// </summary>
    public Task<bool> ContainsAsync(T value, CommandFlags flags = CommandFlags.None)
    {
        var serialized = this.Connection.Converter.Serialize(value);
        return this.Connection.Database.SetContainsAsync(this.Key, serialized, flags);
    }


    /// <summary>
    /// SCARD : <a href="http://redis.io/commands/scard"></a>
    /// </summary>
    public Task<long> LengthAsync(CommandFlags flags = CommandFlags.None)
        => this.Connection.Database.SetLengthAsync(this.Key, flags);


    /// <summary>
    /// SMEMBERS : <a href="https://redis.io/commands/smembers"></a>
    /// </summary>
    public async Task<T[]> MembersAsync(CommandFlags flags = CommandFlags.None)
    {
        var members = await this.Connection.Database.SetMembersAsync(this.Key, flags).ConfigureAwait(false);
        return members
            .Select(this.Connection.Converter, static (x, c) => c.Deserialize<T>(x))
            .ToArray();
    }


    /// <summary>
    /// SMOVE : <a href="https://redis.io/commands/smove"></a>
    /// </summary>
    public Task<bool> MoveAsync(RedisSet<T> destination, T value, CommandFlags flags = CommandFlags.None)
    {
        var serialized = this.Connection.Converter.Serialize(value);
        return this.Connection.Database.SetMoveAsync(this.Key, destination.Key, serialized, flags);
    }


    /// <summary>
    /// SPOP : <a href="http://redis.io/commands/spop"></a>
    /// </summary>
    public async Task<RedisResult<T>> PopAsync(CommandFlags flags = CommandFlags.None)
    {
        var value = await this.Connection.Database.SetPopAsync(this.Key, flags).ConfigureAwait(false);
        return value.ToResult<T>(this.Connection.Converter);
    }


    /// <summary>
    /// SRANDMEMBER : <a href="https://redis.io/commands/srandmember"></a>
    /// </summary>
    public async Task<RedisResult<T>> RandomMemberAsync(CommandFlags flags = CommandFlags.None)
    {
        var value = await this.Connection.Database.SetRandomMemberAsync(this.Key, flags).ConfigureAwait(false);
        return value.ToResult<T>(this.Connection.Converter);
    }


    /// <summary>
    /// SRANDMEMBER : <a href="https://redis.io/commands/srandmember"></a>
    /// </summary>
    public async Task<T[]> RandomMemberAsync(long count, CommandFlags flags = CommandFlags.None)
    {
        var values = await this.Connection.Database.SetRandomMembersAsync(this.Key, count, flags).ConfigureAwait(false);
        return values
            .Select(this.Connection.Converter, static (x, c) => c.Deserialize<T>(x))
            .ToArray();
    }


    /// <summary>
    /// SREM : <a href="http://redis.io/commands/srem"></a>
    /// </summary>
    public Task<bool> RemoveAsync(T value, CommandFlags flags = CommandFlags.None)
    {
        var serialized = this.Connection.Converter.Serialize(value);
        return this.Connection.Database.SetRemoveAsync(this.Key, serialized, flags);
    }


    /// <summary>
    /// SORT : <a href="https://redis.io/commands/sort"></a>
    /// </summary>
    public Task<long> SortAndStoreAsync(RedisSet<T> destination, long skip = 0, long take = -1, Order order = Order.Ascending, SortType sortType = SortType.Numeric, /*RedisValue by = default, RedisValue[] get = null,*/ CommandFlags flags = CommandFlags.None)
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
}
