﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CloudStructures.Internals;
using StackExchange.Redis;

namespace CloudStructures.Structures;



/// <summary>
/// Provides bit related commands.
/// </summary>
public readonly struct RedisBit(RedisConnection connection, RedisKey key, TimeSpan? defaultExpiry) : IRedisStructureWithExpiry
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
    //- [x] StringBitCountAsync
    //- [x] StringBitOperationAsync
    //- [x] StringBitPositionAsync
    //- [x] StringGetBitAsync
    //- [x] StringSetBitAsync


    /// <summary>
    /// BITCOUNT : <a href="http://redis.io/commands/bitcount"></a>
    /// </summary>
    public Task<long> CountAsync(long start = 0, long end = -1, CommandFlags flags = CommandFlags.None)
        => this.Connection.Database.StringBitCountAsync(this.Key, start, end, flags);


    /// <summary>
    /// BITOP : <a href="https://redis.io/commands/bitop"></a>
    /// </summary>
    public Task<long> OperationAsync(Bitwise operation, RedisBit first, RedisBit? second = null, CommandFlags flags = CommandFlags.None)
    {
        var firstKey = first.Key;
        var secondKey = second?.Key ?? default;
        return this.Connection.Database.StringBitOperationAsync(operation, this.Key, firstKey, secondKey, flags);
    }


    /// <summary>
    /// BITOP : <a href="https://redis.io/commands/bitop"></a>
    /// </summary>
    public Task<long> OperationAsync(Bitwise operation, IReadOnlyCollection<RedisBit> bits, CommandFlags flags = CommandFlags.None)
    {
        if (bits.Count == 0)
            throw new ArgumentException("bits length is 0.");

        var keys = bits.Select(static x => x.Key).ToArray();
        return this.Connection.Database.StringBitOperationAsync(operation, this.Key, keys, flags);
    }


    /// <summary>
    /// BITPOSITION : <a href="http://redis.io/commands/bitpos"></a>
    /// </summary>
    public Task<long> PositionAsync(bool bit, long start = 0, long end = -1, CommandFlags flags = CommandFlags.None)
        => this.Connection.Database.StringBitPositionAsync(this.Key, bit, start, end, flags);


    /// <summary>
    /// GETBIT : <a href="http://redis.io/commands/getbit"></a>
    /// </summary>
    public Task<bool> GetAsync(long offset, CommandFlags flags = CommandFlags.None)
        => this.Connection.Database.StringGetBitAsync(this.Key, offset, flags);


    /// <summary>
    /// SETBIT : <a href="http://redis.io/commands/setbit"></a>
    /// </summary>
    public Task<bool> SetAsync(long offset, bool bit, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
    {
        expiry ??= this.DefaultExpiry;
        return this.ExecuteWithExpiryAsync
        (
            static (db, state) => db.StringSetBitAsync(state.key, state.offset, state.bit, state.flags),
            state: (key: this.Key, offset, bit, flags),
            expiry,
            flags
        );
    }
    #endregion
}
