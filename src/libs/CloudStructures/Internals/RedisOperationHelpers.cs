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
    extension<TRedis>(TRedis @this)
        where TRedis : IRedisStructure
    {
        /// <summary>
        /// Execute specified command with expiration time.
        /// </summary>
        /// <typeparam name="TState"></typeparam>
        /// <param name="command"></param>
        /// <param name="state"></param>
        /// <param name="expiry"></param>
        /// <param name="flags"></param>
        /// <returns></returns>
        public async Task ExecuteWithExpiryAsync<TState>(Func<IDatabaseAsync, TState, Task> command, TState state, TimeSpan? expiry, CommandFlags flags)
        {
            if (expiry.HasValue)
            {
                //--- Execute multiple commands in tracsaction
                var t = @this.Connection.Transaction;
                _ = command(t, state);  // forget
                _ = t.KeyExpireAsync(@this.Key, expiry.Value, flags);  // forget

                //--- commit
                await t.ExecuteAsync(flags).ConfigureAwait(false);
            }
            else
            {
                var database = @this.Connection.Database;
                await command(database, state).ConfigureAwait(false);
            }
        }


        /// <summary>
        /// Execute specified command with expiration time.
        /// </summary>
        /// <typeparam name="TState"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="command"></param>
        /// <param name="state"></param>
        /// <param name="expiry"></param>
        /// <param name="flags"></param>
        /// <returns></returns>
        public async Task<TResult> ExecuteWithExpiryAsync<TState, TResult>(Func<IDatabaseAsync, TState, Task<TResult>> command, TState state, TimeSpan? expiry, CommandFlags flags)
        {
            if (expiry.HasValue)
            {
                //--- Execute multiple commands in tracsaction
                var t = @this.Connection.Transaction;
                var result = command(t, state);
                _ = t.KeyExpireAsync(@this.Key, expiry.Value, flags);  // forget

                //--- commit
                await t.ExecuteAsync(flags).ConfigureAwait(false);

                //--- gets result value
                return await result.ConfigureAwait(false);
            }
            else
            {
                var database = @this.Connection.Database;
                return await command(database, state).ConfigureAwait(false);
            }
        }
    }
}
