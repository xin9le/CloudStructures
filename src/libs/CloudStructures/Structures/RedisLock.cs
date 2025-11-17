using System;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace CloudStructures.Structures;



/// <summary>
/// Provides lock related commands.
/// </summary>
/// <typeparam name="T">Data type</typeparam>
public readonly struct RedisLock<T>(RedisConnection connection, RedisKey key) : IRedisStructure
{
    #region IRedisStructure implementations
    /// <summary>
    /// Gets connection.
    /// </summary>
    public RedisConnection Connection { get; } = connection;


    /// <summary>
    /// Gets key.
    /// </summary>
    public RedisKey Key { get; } = key;
    #endregion


    #region Commands
    //- [x] LockExtendAsync
    //- [x] LockQueryAsync
    //- [x] LockReleaseAsync
    //- [x] LockTakeAsync


    /// <summary>
    /// Extends a lock, if the token value is correct.
    /// </summary>
    public Task<bool> ExtendAsync(T value, TimeSpan expiry, CommandFlags flags = CommandFlags.None)
    {
        var serialized = this.Connection.Converter.Serialize(value);
        return this.Connection.Database.LockExtendAsync(this.Key, serialized, expiry, flags);
    }


    /// <summary>
    /// Queries the token held against a lock.
    /// </summary>
    public async Task<RedisResult<T>> QueryAsync(CommandFlags flags = CommandFlags.None)
    {
        var value = await this.Connection.Database.LockQueryAsync(this.Key, flags).ConfigureAwait(false);
        return value.ToResult<T>(this.Connection.Converter);
    }


    /// <summary>
    /// Releases a lock, if the token value is correct.
    /// </summary>
    public Task<bool> ReleaseAsync(T value, CommandFlags flags = CommandFlags.None)
    {
        var serialized = this.Connection.Converter.Serialize(value);
        return this.Connection.Database.LockReleaseAsync(this.Key, serialized, flags);
    }


    /// <summary>
    /// Takes a lock (specifying a token value) if it is not already taken.
    /// </summary>
    public Task<bool> TakeAsync(T value, TimeSpan expiry, CommandFlags flags = CommandFlags.None)
    {
        var serialized = this.Connection.Converter.Serialize(value);
        return this.Connection.Database.LockTakeAsync(this.Key, serialized, expiry, flags);
    }
    #endregion
}
