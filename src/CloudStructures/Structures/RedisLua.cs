using System;
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
        public RedisLua(RedisConnection connection, RedisKey key, TimeSpan? defaultExpiry)
        {
            this.Connection = connection ?? throw new ArgumentNullException(nameof(connection));
            this.Key = key;
            this.DefaultExpiry = defaultExpiry;
        }
        #endregion


        #region Commands
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
