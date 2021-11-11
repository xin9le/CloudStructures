using System;
using System.Threading.Tasks;
using CloudStructures.Structures;
using StackExchange.Redis;

namespace CloudStructures.Internals;



/// <summary>
/// Provides helper methods for Redis operation.
/// </summary>
internal static class RedisOperationHelpers
{
    /// <summary>
    /// Execute specified command with expiration time.
    /// </summary>
    /// <typeparam name="TRedis"></typeparam>
    /// <typeparam name="TState"></typeparam>
    /// <param name="structure"></param>
    /// <param name="command"></param>
    /// <param name="state"></param>
    /// <param name="expiry"></param>
    /// <param name="flags"></param>
    /// <returns></returns>
    public static async Task ExecuteWithExpiryAsync<TRedis, TState>(this TRedis structure, Func<IDatabaseAsync, TState, Task> command, TState state, TimeSpan? expiry, CommandFlags flags)
        where TRedis : IRedisStructure
    {
        if (expiry.HasValue)
        {
            //--- Execute multiple commands in tracsaction
            var t = structure.Connection.Transaction;
            _ = command(t, state);  // forget
            _ = t.KeyExpireAsync(structure.Key, expiry.Value, flags);  // forget

            //--- commit
            await t.ExecuteAsync(flags).ConfigureAwait(false);
        }
        else
        {
            var database = structure.Connection.Database;
            await command(database, state).ConfigureAwait(false);
        }
    }


    /// <summary>
    /// Execute specified command with expiration time.
    /// </summary>
    /// <typeparam name="TRedis"></typeparam>
    /// <typeparam name="TState"></typeparam>
    /// <typeparam name="TResult"></typeparam>
    /// <param name="structure"></param>
    /// <param name="command"></param>
    /// <param name="state"></param>
    /// <param name="expiry"></param>
    /// <param name="flags"></param>
    /// <returns></returns>
    public static async Task<TResult> ExecuteWithExpiryAsync<TRedis, TState, TResult>(this TRedis structure, Func<IDatabaseAsync, TState, Task<TResult>> command, TState state, TimeSpan? expiry, CommandFlags flags)
        where TRedis : IRedisStructure
    {
        if (expiry.HasValue)
        {
            //--- Execute multiple commands in tracsaction
            var t = structure.Connection.Transaction;
            var result = command(t, state);
            _ = t.KeyExpireAsync(structure.Key, expiry.Value, flags);  // forget

            //--- commit
            await t.ExecuteAsync(flags).ConfigureAwait(false);

            //--- gets result value
            return await result.ConfigureAwait(false);
        }
        else
        {
            var database = structure.Connection.Database;
            return await command(database, state).ConfigureAwait(false);
        }
    }
}
