using System;
using System.Threading.Tasks;
using StackExchange.Redis;



namespace CloudStructures.Structures
{
    /// <summary>
    /// Lua スクリプティング関連のコマンドを提供します。
    /// </summary>
    /// <typeparam name="T">データ型</typeparam>
    public readonly struct RedisLua : IRedisStructure
    {
        #region IRedisStructure implementations
        /// <summary>
        /// 接続を取得します。
        /// </summary>
        public RedisConnection Connection { get; }


        /// <summary>
        /// キーを取得します。
        /// </summary>
        public RedisKey Key { get; }


        /// <summary>
        /// 既定の有効期限を取得します。
        /// </summary>
        public TimeSpan? DefaultExpiry { get; }
        #endregion


        #region コンストラクタ
        /// <summary>
        /// インスタンスを生成します。
        /// </summary>
        /// <param name="connection">接続</param>
        /// <param name="key">キー</param>
        /// <param name="defaultExpiry">既定の有効期限</param>
        public RedisLua(RedisConnection connection, RedisKey key, TimeSpan? defaultExpiry)
        {
            this.Connection = connection ?? throw new ArgumentNullException(nameof(connection));
            this.Key = key;
            this.DefaultExpiry = defaultExpiry;
        }
        #endregion


        #region コマンド
        // - [x] ScriptEvaluateAsync


        /// <summary>
        /// EVALSHA : http://redis.io/commands/evalsha
        /// </summary>
        public Task ScriptEvaluate(string script, RedisKey[] keys = null, RedisValue[] values = null, CommandFlags flags = CommandFlags.None)
            => this.Connection.Database.ScriptEvaluateAsync(script, keys, values, flags);


        /// <summary>
        /// EVALSHA : http://redis.io/commands/evalsha
        /// </summary>
        public async Task<RedisResult<T>> ScriptEvaluate<T>(string script, RedisKey[] keys = null, RedisValue[] values = null, CommandFlags flags = CommandFlags.None)
        {
            var result = await this.Connection.Database.ScriptEvaluateAsync(script, keys, values, flags).ConfigureAwait(false);
            if (result.IsNull)
            {
                return RedisResult<T>.Default;
            }
            else
            {
                var v = (RedisValue)result;
                return v.ToResult<T>(this.Connection.Converter);
            }
        }
        #endregion
    }
}
