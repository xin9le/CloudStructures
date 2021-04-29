using System.Threading.Tasks;
using StackExchange.Redis;



namespace CloudStructures.Structures
{
    /// <summary>
    /// Provides Lua scripting related commands.
    /// </summary>
    /// <typeparam name="T">Data type</typeparam>
    public readonly struct RedisLua : IRedisStructure
    {
        #region IRedisStructure implementations
        /// <summary>
        /// Gets connection.
        /// </summary>
        public RedisConnection Connection { get; }


        /// <summary>
        /// Gets key.
        /// </summary>
        public RedisKey Key { get; }
        #endregion


        #region Constructors
        /// <summary>
        /// Creates instance.
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="key"></param>
        public RedisLua(RedisConnection connection, RedisKey key)
        {
            this.Connection = connection;
            this.Key = key;
        }
        #endregion


        #region Commands
        // - [x] ScriptEvaluateAsync


        /// <summary>
        /// EVALSHA : http://redis.io/commands/evalsha
        /// </summary>
        public Task ScriptEvaluateAsync(string script, RedisKey[]? keys = null, RedisValue[]? values = null, CommandFlags flags = CommandFlags.None)
            => this.Connection.Database.ScriptEvaluateAsync(script, keys, values, flags);


        /// <summary>
        /// EVALSHA : http://redis.io/commands/evalsha
        /// </summary>
        public async Task<RedisResult<T>> ScriptEvaluateAsync<T>(string script, RedisKey[]? keys = null, RedisValue[]? values = null, CommandFlags flags = CommandFlags.None)
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
