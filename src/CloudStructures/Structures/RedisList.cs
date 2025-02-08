﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CloudStructures.Internals;
using StackExchange.Redis;

namespace CloudStructures.Structures;



/// <summary>
/// Provides list related commands.
/// </summary>
/// <typeparam name="T">Data type</typeparam>
public readonly struct RedisList<T>(RedisConnection connection, RedisKey key, TimeSpan? defaultExpiry) : IRedisStructureWithExpiry
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
    //- [x] ListGetByIndexAsync
    //- [x] ListInsertAfterAsync
    //- [x] ListInsertBeforeAsync
    //- [x] ListLeftPopAsync
    //- [x] ListLeftPushAsync
    //- [x] ListLengthAsync
    //- [x] ListRangeAsync
    //- [x] ListRemoveAsync
    //- [x] ListRightPopAsync
    //- [x] ListRightPopLeftPushAsync
    //- [x] ListRightPushAsync
    //- [x] ListSetByIndexAsync
    //- [x] ListTrimAsync
    //- [x] SortAndStoreAsync
    //- [x] SortAsync


    /// <summary>
    /// LINDEX : <a href="https://redis.io/commands/lindex"></a>
    /// </summary>
    public async Task<RedisResult<T>> GetByIndexAsync(long index, CommandFlags flags = CommandFlags.None)
    {
        var value = await this.Connection.Database.ListGetByIndexAsync(this.Key, index, flags).ConfigureAwait(false);
        return value.ToResult<T>(this.Connection.Converter);
    }


    /// <summary>
    /// LINSERT : <a href="https://redis.io/commands/linsert"></a>
    /// </summary>
    public Task<long> InsertAfterAsync(T pivot, T value, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
    {
        expiry ??= this.DefaultExpiry;
        var p = this.Connection.Converter.Serialize(pivot);
        var v = this.Connection.Converter.Serialize(value);
        return this.ExecuteWithExpiryAsync
        (
            static (db, state) => db.ListInsertAfterAsync(state.key, state.p, state.v, state.flags),
            state: (key: this.Key, p, v, flags),
            expiry,
            flags
        );
    }


    /// <summary>
    /// LINSERT : <a href="https://redis.io/commands/linsert"></a>
    /// </summary>
    public Task<long> InsertBeforeAsync(T pivot, T value, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
    {
        expiry ??= this.DefaultExpiry;
        var p = this.Connection.Converter.Serialize(pivot);
        var v = this.Connection.Converter.Serialize(value);
        return this.ExecuteWithExpiryAsync
        (
            static (db, state) => db.ListInsertBeforeAsync(state.key, state.p, state.v, state.flags),
            state: (key: this.Key, p, v, flags),
            expiry,
            flags
        );
    }


    /// <summary>
    /// LPOP : <a href="https://redis.io/commands/lpop"></a>
    /// </summary>
    public async Task<RedisResult<T>> LeftPopAsync(CommandFlags flags = CommandFlags.None)
    {
        var value = await this.Connection.Database.ListLeftPopAsync(this.Key, flags).ConfigureAwait(false);
        return value.ToResult<T>(this.Connection.Converter);
    }


    /// <summary>
    /// LPOP : <a href="https://redis.io/commands/lpop"></a>
    /// </summary>
    public async Task<T[]?> LeftPopAsync(long count, CommandFlags flags = CommandFlags.None)
    {
        var values = await this.Connection.Database.ListLeftPopAsync(this.Key, count, flags).ConfigureAwait(false);
        return values?.Select(this.Connection.Converter, static (x, c) => c.Deserialize<T>(x)).ToArray();
    }


    /// <summary>
    /// LPUSH : <a href="https://redis.io/commands/lpush"></a>
    /// </summary>
    public Task<long> LeftPushAsync(T value, TimeSpan? expiry = null, When when = When.Always, CommandFlags flags = CommandFlags.None)
    {
        expiry ??= this.DefaultExpiry;
        var serialized = this.Connection.Converter.Serialize(value);
        return this.ExecuteWithExpiryAsync
        (
            static (db, state) => db.ListLeftPushAsync(state.key, state.serialized, state.when, state.flags),
            state: (key: this.Key, serialized, when, flags),
            expiry,
            flags
        );
    }


    /// <summary>
    /// LPUSH : <a href="https://redis.io/commands/lpush"></a>
    /// </summary>
    public Task<long> LeftPushAsync(IEnumerable<T> values, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
    {
        expiry ??= this.DefaultExpiry;
        var serialized = values.Select(this.Connection.Converter.Serialize).ToArray();
        return this.ExecuteWithExpiryAsync
        (
            static (db, state) => db.ListLeftPushAsync(state.key, state.serialized, state.flags),
            state: (key: this.Key, serialized, flags),
            expiry,
            flags
        );
    }


    /// <summary>
    /// LLEN : <a href="https://redis.io/commands/llen"></a>
    /// </summary>
    public Task<long> LengthAsync(CommandFlags flags = CommandFlags.None)
        => this.Connection.Database.ListLengthAsync(this.Key, flags);


    /// <summary>
    /// LRANGE : <a href="https://redis.io/commands/lrange"></a>
    /// </summary>
    public async Task<T[]> RangeAsync(long start = 0, long stop = -1, CommandFlags flags = CommandFlags.None)
    {
        var values = await this.Connection.Database.ListRangeAsync(this.Key, start, stop, flags).ConfigureAwait(false);
        return values.Select(this.Connection.Converter, static (x, c) => c.Deserialize<T>(x)).ToArray();
    }


    /// <summary>
    /// LREM : <a href="http://redis.io/commands/lrem"></a>
    /// </summary>
    /// <param name="value">Value to be deleted</param>
    /// <param name="count">Number of items to be deleted
    /// <para>
    /// - count &gt; 0 : Delete while searching from the beginning to the end.<br/>
    /// - count &lt; 0 : Delete while searching from the end to the beginning.<br/>
    /// - count = 0 : Delete all matches.
    /// </para>
    /// </param>
    /// <param name="flags"></param>
    public Task<long> RemoveAsync(T value, long count = 0, CommandFlags flags = CommandFlags.None)
    {
        var serialized = this.Connection.Converter.Serialize(value);
        return this.Connection.Database.ListRemoveAsync(this.Key, serialized, count, flags);
    }


    /// <summary>
    /// RPOP : <a href="https://redis.io/commands/rpop"></a>
    /// </summary>
    public async Task<RedisResult<T>> RightPopAsync(CommandFlags flags = CommandFlags.None)
    {
        var value = await this.Connection.Database.ListRightPopAsync(this.Key, flags).ConfigureAwait(false);
        return value.ToResult<T>(this.Connection.Converter);
    }


    /// <summary>
    /// RPOP : <a href="https://redis.io/commands/rpop"></a>
    /// </summary>
    public async Task<T[]?> RightPopAsync(long count, CommandFlags flags = CommandFlags.None)
    {
        var values = await this.Connection.Database.ListRightPopAsync(this.Key, count, flags).ConfigureAwait(false);
        return values?.Select(this.Connection.Converter, static (x, c) => c.Deserialize<T>(x)).ToArray();
    }


    /// <summary>
    /// RPOPLPUSH : <a href="https://redis.io/commands/rpoplpush"></a>
    /// </summary>
    public async Task<RedisResult<T>> RightPopLeftPushAsync(RedisList<T> destination, CommandFlags flags = CommandFlags.None)
    {
        var value = await this.Connection.Database.ListRightPopLeftPushAsync(this.Key, destination.Key, flags).ConfigureAwait(false);
        return value.ToResult<T>(this.Connection.Converter);
    }


    /// <summary>
    /// RPUSH : <a href="https://redis.io/commands/rpush"></a>
    /// </summary>
    public Task<long> RightPushAsync(T value, TimeSpan? expiry = null, When when = When.Always, CommandFlags flags = CommandFlags.None)
    {
        expiry ??= this.DefaultExpiry;
        var serialized = this.Connection.Converter.Serialize(value);
        return this.ExecuteWithExpiryAsync
        (
            static (db, state) => db.ListRightPushAsync(state.key, state.serialized, state.when, state.flags),
            state: (key: this.Key, serialized, when, flags),
            expiry,
            flags
        );
    }


    /// <summary>
    /// RPUSH : <a href="https://redis.io/commands/rpush"></a>
    /// </summary>
    public Task<long> RightPushAsync(IEnumerable<T> values, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
    {
        expiry ??= this.DefaultExpiry;
        var serialized = values.Select(this.Connection.Converter.Serialize).ToArray();
        return this.ExecuteWithExpiryAsync
        (
            static (db, state) => db.ListRightPushAsync(state.key, state.serialized, state.flags),
            state: (key: this.Key, serialized, flags),
            expiry,
            flags
        );
    }


    /// <summary>
    /// LSET : <a href="https://redis.io/commands/lset"></a>
    /// </summary>
    public Task SetByIndexAsync(long index, T value, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
    {
        expiry ??= this.DefaultExpiry;
        var serialized = this.Connection.Converter.Serialize(value);
        return this.ExecuteWithExpiryAsync
        (
            static (db, state) => db.ListSetByIndexAsync(state.key, state.index, state.serialized, state.flags),
            state: (key: this.Key, index, serialized, flags),
            expiry,
            flags
        );
    }


    /// <summary>
    /// LTRIM : <a href="https://redis.io/commands/ltrim"></a>
    /// </summary>
    public Task TrimAsync(long start, long stop, CommandFlags flags = CommandFlags.None)
        => this.Connection.Database.ListTrimAsync(this.Key, start, stop, flags);


    /// <summary>
    /// SORT : <a href="https://redis.io/commands/sort"></a>
    /// </summary>
    public Task<long> SortAndStoreAsync(RedisList<T> destination, long skip = 0, long take = -1, Order order = Order.Ascending, SortType sortType = SortType.Numeric, /*RedisValue by = default, RedisValue[] get = null,*/ CommandFlags flags = CommandFlags.None)
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
    /// First LPUSH, then LTRIM to the specified list length.
    /// </summary>
    public async Task<long> FixedLengthLeftPushAsync(T value, long length, TimeSpan? expiry = null, When when = When.Always, CommandFlags flags = CommandFlags.None)
    {
        expiry ??= this.DefaultExpiry;
        var serialized = this.Connection.Converter.Serialize(value);

        //--- execute multiple commands in transaction
        var t = this.Connection.Transaction;
        var leftPush = t.ListLeftPushAsync(this.Key, serialized, when, flags);
        _ = t.ListTrimAsync(this.Key, 0, length - 1, flags);  // forget
        if (expiry.HasValue)
            _ = t.KeyExpireAsync(this.Key, expiry.Value, flags);  // forget

        //--- commit
        await t.ExecuteAsync(flags).ConfigureAwait(false);

        //--- get result
        var pushLength = await leftPush.ConfigureAwait(false);
        return Math.Min(pushLength, length);
    }


    /// <summary>
    /// First LPUSH, then LTRIM to the specified list length.
    /// </summary>
    public async Task<long> FixedLengthLeftPushAsync(IEnumerable<T> values, long length, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
    {
        expiry ??= this.DefaultExpiry;
        var serialized = values.Select(this.Connection.Converter.Serialize).ToArray();

        //--- execute multiple commands in transaction
        var t = this.Connection.Transaction;
        var leftPush = t.ListLeftPushAsync(this.Key, serialized, flags);
        _ = t.ListTrimAsync(this.Key, 0, length - 1, flags);  // forget
        if (expiry.HasValue)
            _ = t.KeyExpireAsync(this.Key, expiry.Value, flags);  // forget

        //--- commit
        await t.ExecuteAsync(flags).ConfigureAwait(false);

        //--- get result
        var pushLength = await leftPush.ConfigureAwait(false);
        return Math.Min(pushLength, length);
    }
    #endregion
}
