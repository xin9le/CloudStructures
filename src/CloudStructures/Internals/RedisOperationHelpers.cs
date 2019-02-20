using System;
using System.Threading.Tasks;
using CloudStructures.Structures;
using StackExchange.Redis;



namespace CloudStructures.Internals
{
    /// <summary>
    /// Redis の操作に関する補助機能を提供します。
    /// </summary>
    internal static class RedisOperationHelpers
    {
        /// <summary>
        /// 有効期限を設定しつつ、指定されたコマンドを実行します。
        /// </summary>
        /// <typeparam name="TRedis">Redis 構造</typeparam>
        /// <typeparam name="TArgs">コマンド引数のデータ型</typeparam>
        /// <param name="structure">データ構造</param>
        /// <param name="command">コマンド</param>
        /// <param name="expiry">有効期限</param>
        /// <param name="flags">フラグ</param>
        /// <returns></returns>
        public static async Task ExecuteWithExpiry<TRedis, TArgs>(this TRedis structure, Func<IDatabaseAsync, TArgs, Task> command, TArgs args, TimeSpan? expiry, CommandFlags flags)
            where TRedis : IRedisStructure
        {
            if (structure == null) throw new ArgumentNullException(nameof(structure));
            if (command == null) throw new ArgumentNullException(nameof(command));

            if (expiry.HasValue)
            {
                //--- トランザクション内で複数のコマンドを実行
                var t = structure.Connection.Transaction;
                var _ = command(t, args);  // forget
                var __ = t.KeyExpireAsync(structure.Key, expiry.Value, flags);  // forget

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
        /// 有効期限を設定しつつ、指定されたコマンドを実行します。
        /// </summary>
        /// <typeparam name="TRedis">Redis 構造</typeparam>
        /// <typeparam name="TArgs">コマンド引数のデータ型</typeparam>
        /// <typeparam name="TResult">戻り値のデータ型</typeparam>
        /// <param name="structure">データ構造</param>
        /// <param name="command">コマンド</param>
        /// <param name="expiry">有効期限</param>
        /// <param name="flags">フラグ</param>
        /// <returns></returns>
        public static async Task<TResult> ExecuteWithExpiry<TRedis, TArgs, TResult>(this TRedis structure, Func<IDatabaseAsync, TArgs, Task<TResult>> command, TArgs args, TimeSpan? expiry, CommandFlags flags)
            where TRedis : IRedisStructure
        {
            if (structure == null) throw new ArgumentNullException(nameof(structure));
            if (command == null) throw new ArgumentNullException(nameof(command));

            if (expiry.HasValue)
            {
                //--- トランザクション内で複数のコマンドを実行
                var t = structure.Connection.Transaction;
                var result = command(t, args);
                var _ = t.KeyExpireAsync(structure.Key, expiry.Value, flags);  // forget

                //--- commit
                await t.ExecuteAsync(flags).ConfigureAwait(false);

                //--- 結果を取り出す
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
