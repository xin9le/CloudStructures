using System;
using System.Threading.Tasks;
using CloudStructures.Internals;
using StackExchange.Redis;



namespace CloudStructures.Structures
{
    /// <summary>
    /// Provides string related commands.
    /// </summary>
    /// <typeparam name="T">Data type</typeparam>
    public readonly struct RedisString<T> : IRedisStructureWithExpiry
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
        public RedisString(RedisConnection connection, in RedisKey key, TimeSpan? defaultExpiry)
        {
            this.Connection = connection ?? throw new ArgumentNullException(nameof(connection));
            this.Key = key;
            this.DefaultExpiry = defaultExpiry;
        }
        #endregion


        #region Commands
        //- [] StringAppendAsync
        //- [x] StringDecrementAsync
        //- [x] StringGetAsync
        //- [] StringGetRangeAsync
        //- [x] StringGetSetAsync
        //- [x] StringGetWithExpiryAsync
        //- [x] StringIncrementAsync
        //- [x] StringLengthAsync
        //- [x] StringSetAsync
        //- [] StringSetRangeAsync


        /// <summary>
        /// DECRBY : http://redis.io/commands/decrby
        /// </summary>
        public Task<long> DecrementAsync(long value = 1, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
        {
            expiry ??= this.DefaultExpiry;
            return this.ExecuteWithExpiryAsync
            (
                (db, a) => db.StringDecrementAsync(a.key, a.value, a.flags),
                (key: this.Key, value, flags),
                expiry,
                flags
            );
        }


        /// <summary>
        /// INCRBYFLOAT : http://redis.io/commands/incrbyfloat
        /// </summary>
        public Task<double> DecrementAsync(double value, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
        {
            expiry ??= this.DefaultExpiry;
            return this.ExecuteWithExpiryAsync
            (
                (db, a) => db.StringDecrementAsync(a.key, a.value, a.flags),
                (key: this.Key, value, flags),
                expiry,
                flags
            );
        }

        
        /// <summary>
        /// GET : http://redis.io/commands/get
        /// </summary>
        public async Task<RedisResult<T>> GetAsync(CommandFlags flags = CommandFlags.None)
        {
            var value = await this.Connection.Database.StringGetAsync(Key, flags).ConfigureAwait(false);
            return value.ToResult<T>(this.Connection.Converter);
        }


        /// <summary>
        /// GETSET : http://redis.io/commands/getset
        /// </summary>
        public async Task<RedisResult<T>> GetSetAsync(T value, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
        {
            expiry ??= this.DefaultExpiry;
            var serialized = this.Connection.Converter.Serialize(value);
            var result
                = await this.ExecuteWithExpiryAsync
                (
                    (db, a) => db.StringGetSetAsync(a.key, a.serialized, a.flags),
                    (key: this.Key, serialized, flags),
                    expiry,
                    flags
                )
                .ConfigureAwait(false);
            return result.ToResult<T>(this.Connection.Converter);
        }


        /// <summary>
        /// GET : http://redis.io/commands/get
        /// </summary>
        public async Task<RedisResultWithExpiry<T>> GetWithExpiryAsync(CommandFlags flags = CommandFlags.None)
        {
            var value = await this.Connection.Database.StringGetWithExpiryAsync(this.Key, flags).ConfigureAwait(false);
            return value.ToResult<T>(this.Connection.Converter);
        }


        /// <summary>
        /// INCRBY : http://redis.io/commands/incrby
        /// </summary>
        public Task<long> IncrementAsync(long value = 1, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
        {
            expiry ??= this.DefaultExpiry;
            return this.ExecuteWithExpiryAsync
            (
                (db, a) => db.StringIncrementAsync(a.key, a.value, a.flags),
                (key: this.Key, value, flags),
                expiry,
                flags
            );
        }


        /// <summary>
        /// INCRBYFLOAT : http://redis.io/commands/incrbyfloat
        /// </summary>
        public Task<double> IncrementAsync(double value, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
        {
            expiry ??= this.DefaultExpiry;
            return this.ExecuteWithExpiryAsync
            (
                (db, a) => db.StringIncrementAsync(a.key, a.value, a.flags),
                (key: this.Key, value, flags),
                expiry,
                flags
            );
        }


        /// <summary>
        /// STRLEN : https://redis.io/commands/strlen
        /// </summary>
        public Task<long> LengthAsync(CommandFlags flags = CommandFlags.None)
            => this.Connection.Database.StringLengthAsync(this.Key, flags);


        /// <summary>
        /// SET : http://redis.io/commands/set
        /// </summary>
        public Task<bool> SetAsync(T value, TimeSpan? expiry = null, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            expiry ??= this.DefaultExpiry;
            var serialized = this.Connection.Converter.Serialize(value);
            return this.Connection.Database.StringSetAsync(this.Key, serialized, expiry, when, flags);
        }
        #endregion


        #region Custom Commands
        /// <summary>
        /// GET : http://redis.io/commands/get
        /// SET : http://redis.io/commands/set
        /// </summary>
        public async Task<T> GetOrSetAsync(Func<T> valueFactory, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
        {
            expiry ??= this.DefaultExpiry;
            var value = await this.GetAsync(flags).ConfigureAwait(false);
            if (value.HasValue)
            {
                return value.Value;
            }
            else
            {
                var newValue = valueFactory();
                await this.SetAsync(newValue, expiry, When.Always, flags).ConfigureAwait(false);
                return newValue;
            }
        }


        /// <summary>
        /// GET : http://redis.io/commands/get
        /// SET : http://redis.io/commands/set
        /// </summary>
        public async Task<T> GetOrSetAsync(Func<Task<T>> valueFactory, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
        {
            expiry ??= this.DefaultExpiry;
            var value = await this.GetAsync(flags).ConfigureAwait(false);
            if (value.HasValue)
            {
                return value.Value;
            }
            else
            {
                var newValue = await valueFactory().ConfigureAwait(false);
                await this.SetAsync(newValue, expiry, When.Always, flags).ConfigureAwait(false);
                return newValue;
            }
        }


        /// <summary>
        /// LUA Script including incrby, set
        /// </summary>
        public async Task<long> IncrementLimitByMaxAsync(long value, long max, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
        {
            expiry ??= this.DefaultExpiry;
            var script =
@"local inc = tonumber(ARGV[1])
local max = tonumber(ARGV[2])
local x = redis.call('incrby', KEYS[1], inc)
if(x > max) then
    redis.call('set', KEYS[1], max)
    x = max
end
return x";
            var keys = new[] { this.Key };
            var values = new RedisValue[] { value, max };
            var result
                = await this.ExecuteWithExpiryAsync
                (
                    (db, a) => db.ScriptEvaluateAsync(a.script, a.keys, a.values, a.flags),
                    (script, keys, values, flags),
                    expiry,
                    flags
                )
                .ConfigureAwait(false);
            return (long)result;
        }


        /// <summary>
        /// LUA Script including incrbyfloat, set
        /// </summary>
        public async Task<double> IncrementLimitByMaxAsync(double value, double max, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
        {
            expiry ??= this.DefaultExpiry;
            var script =
@"local inc = tonumber(ARGV[1])
local max = tonumber(ARGV[2])
local x = tonumber(redis.call('incrbyfloat', KEYS[1], inc))
if(x > max) then
    redis.call('set', KEYS[1], max)
    x = max
end
return tostring(x)";
            var keys = new[] { this.Key };
            var values = new RedisValue[] { value, max };
            var result
                = await this.ExecuteWithExpiryAsync
                (
                    (db, a) => db.ScriptEvaluateAsync(a.script, a.keys, a.values, a.flags),
                    (script, keys, values, flags),
                    expiry,
                    flags
                )
                .ConfigureAwait(false);
            return double.Parse((string)result);
        }


        /// <summary>
        /// LUA Script including incrby, set
        /// </summary>
        public async Task<long> IncrementLimitByMinAsync(long value, long min, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
        {
            expiry ??= this.DefaultExpiry;
            var script =
@"local inc = tonumber(ARGV[1])
local min = tonumber(ARGV[2])
local x = redis.call('incrby', KEYS[1], inc)
if(x < min) then
    redis.call('set', KEYS[1], min)
    x = min
end
return x";
            var keys = new[] { this.Key };
            var values = new RedisValue[] { value, min };
            var result
                = await this.ExecuteWithExpiryAsync
                (
                    (db, a) => db.ScriptEvaluateAsync(a.script, a.keys, a.values, a.flags),
                    (script, keys, values, flags),
                    expiry,
                    flags
                )
                .ConfigureAwait(false);
            return (long)result;
        }


        /// <summary>
        /// LUA Script including incrbyfloat, set
        /// </summary>
        public async Task<double> IncrementLimitByMinAsync(double value, double min, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
        {
            expiry ??= this.DefaultExpiry;
            var script =
@"local inc = tonumber(ARGV[1])
local min = tonumber(ARGV[2])
local x = tonumber(redis.call('incrbyfloat', KEYS[1], inc))
if(x < min) then
    redis.call('set', KEYS[1], min)
    x = min
end
return tostring(x)";
            var keys = new[] { this.Key };
            var values = new RedisValue[] { value, min };
            var result
                = await this.ExecuteWithExpiryAsync
                (
                    (db, a) => db.ScriptEvaluateAsync(a.script, a.keys, a.values, a.flags),
                    (script, keys, values, flags),
                    expiry,
                    flags
                )
                .ConfigureAwait(false);
            return double.Parse((string)result);
        }
        #endregion
    }
}
