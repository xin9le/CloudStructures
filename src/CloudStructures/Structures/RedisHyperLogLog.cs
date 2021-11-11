using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CloudStructures.Internals;
using StackExchange.Redis;

namespace CloudStructures.Structures;



/// <summary>
/// Provides HyperLogLog related commands.
/// </summary>
/// <typeparam name="T">Data type</typeparam>
public readonly struct RedisHyperLogLog<T> : IRedisStructureWithExpiry
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
    public RedisHyperLogLog(RedisConnection connection, RedisKey key, TimeSpan? defaultExpiry)
    {
        this.Connection = connection;
        this.Key = key;
        this.DefaultExpiry = defaultExpiry;
    }
    #endregion


    #region Commands
    //- [x] HyperLogLogAddAsync
    //- [x] HyperLogLogLengthAsync
    //- [x] HyperLogLogMergeAsync


    /// <summary>
    /// PFADD : <a href="http://redis.io/commands/pfadd"></a>
    /// </summary>
    public Task<bool> AddAsync(T value, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
    {
        expiry ??= this.DefaultExpiry;
        var serialized = this.Connection.Converter.Serialize(value);
        return this.ExecuteWithExpiryAsync
        (
            (db, a) => db.HyperLogLogAddAsync(a.key, a.serialized, a.flags),
            (key: this.Key, serialized, flags),
            expiry,
            flags
        );
    }


    /// <summary>
    /// PFADD : <a href="http://redis.io/commands/pfadd"></a>
    /// </summary>
    public Task<bool> AddAsync(IEnumerable<T> values, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
    {
        expiry ??= this.DefaultExpiry;
        var serialized = values.Select(this.Connection.Converter.Serialize).ToArray();
        return this.ExecuteWithExpiryAsync
        (
            (db, a) => db.HyperLogLogAddAsync(a.key, a.serialized, a.flags),
            (key: this.Key, serialized, flags),
            expiry,
            flags
        );
    }


    /// <summary>
    /// PFCOUNT : <a href="http://redis.io/commands/pfcount"></a>
    /// </summary>
    public Task<long> LengthAsync(CommandFlags flags = CommandFlags.None)
        => this.Connection.Database.HyperLogLogLengthAsync(this.Key, flags);


    /// <summary>
    /// PFMERGE : <a href="https://redis.io/commands/pfmerge"></a>
    /// </summary>
    public Task MergeAsync(RedisKey first, RedisKey second, CommandFlags flags = CommandFlags.None)
        => this.Connection.Database.HyperLogLogMergeAsync(this.Key, first, second, flags);


    /// <summary>
    /// PFMERGE : <a href="https://redis.io/commands/pfmerge"></a>
    /// </summary>
    public Task MergeAsync(RedisKey[] sourceKeys, CommandFlags flags = CommandFlags.None)
        => this.Connection.Database.HyperLogLogMergeAsync(this.Key, sourceKeys, flags);
    #endregion
}
