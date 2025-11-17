using System.Threading.Tasks;
using StackExchange.Redis;

namespace CloudStructures.Structures;



/// <summary>
/// Provides Lua scripting related commands.
/// </summary>
public readonly struct RedisLua(RedisConnection connection, RedisKey key) : IRedisStructure
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
    // - [x] ScriptEvaluateAsync


    /// <summary>
    /// EVALSHA : <a href="http://redis.io/commands/evalsha"></a>
    /// </summary>
    public Task ScriptEvaluateAsync(string script, RedisKey[]? keys = null, RedisValue[]? values = null, CommandFlags flags = CommandFlags.None)
        => this.Connection.Database.ScriptEvaluateAsync(script, keys, values, flags);


    /// <summary>
    /// EVALSHA : <a href="http://redis.io/commands/evalsha"></a>
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
