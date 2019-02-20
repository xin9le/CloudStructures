using System;
using System.Threading.Tasks;
using CloudStructures.Structures;
using StackExchange.Redis;



namespace CloudStructures.Internals
{
    /// <summary>
    /// Provides helper methods for Redis operation.
    /// </summary>
    internal static class RedisOperationHelpers
    {
        /// <summary>
        /// Execute specified command with expiration time.
        /// </summary>
        /// <typeparam name="TRedis"></typeparam>
        /// <typeparam name="TArgs"></typeparam>
        /// <param name="structure"></param>
        /// <param name="command"></param>
        /// <param name="expiry"></param>
        /// <param name="flags"></param>
        /// <returns></returns>
        public static async Task ExecuteWithExpiry<TRedis, TArgs>(this TRedis structure, Func<IDatabaseAsync, TArgs, Task> command, TArgs args, TimeSpan? expiry, CommandFlags flags)
            where TRedis : IRedisStructure
        {
            if (structure == null) throw new ArgumentNullException(nameof(structure));
            if (command == null) throw new ArgumentNullException(nameof(command));

            if (expiry.HasValue)
            {
                //--- Execute multiple commands in tracsaction
                var t = structure.Connection.Transaction;
                _ = command(t, args);  // forget
                _ = t.KeyExpireAsync(structure.Key, expiry.Value, flags);  // forget

                //--- commit
                await t.ExecuteAsync(flags).ConfigureAwait(false);
            }
            else
            {
                var database = structure.Connection.Database;
                await command(database, args).ConfigureAwait(false);
            }
        }


        /// <summary>
        /// Execute specified command with expiration time.
        /// </summary>
        /// <typeparam name="TRedis"></typeparam>
        /// <typeparam name="TArgs"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="structure"></param>
        /// <param name="command"></param>
        /// <param name="expiry"></param>
        /// <param name="flags"></param>
        /// <returns></returns>
        public static async Task<TResult> ExecuteWithExpiry<TRedis, TArgs, TResult>(this TRedis structure, Func<IDatabaseAsync, TArgs, Task<TResult>> command, TArgs args, TimeSpan? expiry, CommandFlags flags)
            where TRedis : IRedisStructure
        {
            if (structure == null) throw new ArgumentNullException(nameof(structure));
            if (command == null) throw new ArgumentNullException(nameof(command));

            if (expiry.HasValue)
            {
                //--- Execute multiple commands in tracsaction
                var t = structure.Connection.Transaction;
                var result = command(t, args);
                _ = t.KeyExpireAsync(structure.Key, expiry.Value, flags);  // forget

                //--- commit
                await t.ExecuteAsync(flags).ConfigureAwait(false);

                //--- gets result value
                return await result.ConfigureAwait(false);
            }
            else
            {
                var database = structure.Connection.Database;
                return await command(database, args).ConfigureAwait(false);
            }
        }
    }
}
