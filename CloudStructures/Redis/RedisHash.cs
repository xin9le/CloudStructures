using BookSleeve;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

// RedisDictionary/Hash/Class
namespace CloudStructures.Redis
{
    internal class HashScript
    {
        public const string IncrementLimitByMax = @"
local inc = tonumber(ARGV[1])
local max = tonumber(ARGV[2])
local x = redis.call('hincrby', KEYS[1], KEYS[2], inc)
if(x > max) then
    redis.call('hset', KEYS[1], KEYS[2], max)
    x = max
end
return x";

        public const string IncrementLimitByMin = @"
local inc = tonumber(ARGV[1])
local min = tonumber(ARGV[2])
local x = redis.call('hincrby', KEYS[1], KEYS[2], inc)
if(x < min) then
    redis.call('hset', KEYS[1], KEYS[2], min)
    x = min
end
return x";

        public const string IncrementFloatLimitByMax = @"
local inc = tonumber(ARGV[1])
local max = tonumber(ARGV[2])
local x = tonumber(redis.call('hincrbyfloat', KEYS[1], KEYS[2], inc))
if(x > max) then
    redis.call('hset', KEYS[1], KEYS[2], max)
    x = max
end
return tostring(x)";

        public const string IncrementFloatLimitByMin = @"
local inc = tonumber(ARGV[1])
local min = tonumber(ARGV[2])
local x = tonumber(redis.call('hincrbyfloat', KEYS[1], KEYS[2], inc))
if(x < min) then
    redis.call('hset', KEYS[1], KEYS[2], min)
    x = min
end
return tostring(x)";
    }

    public class RedisDictionary<T>
    {
        const string CallType = "RedisDictionary";

        public string Key { get; private set; }
        public RedisSettings Settings { get; private set; }


        public RedisDictionary(RedisSettings settings, string hashKey)
        {
            this.Settings = settings;
            this.Key = hashKey;
        }

        public RedisDictionary(RedisGroup connectionGroup, string hashKey)
            : this(connectionGroup.GetSettings(hashKey), hashKey)
        {
        }

        protected RedisConnection Connection
        {
            get
            {
                return Settings.GetConnection();
            }
        }

        protected IHashCommands Command
        {
            get
            {
                return Connection.Hashes;
            }
        }

        /// <summary>
        /// HEXISTS http://redis.io/commands/hexists
        /// </summary>
        public async Task<bool> Exists(string field, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                return await Command.Exists(Settings.Db, Key, field, queueJump).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// HGET http://redis.io/commands/hget
        /// </summary>
        public async Task<T> Get(string field, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                var v = await Command.Get(Settings.Db, Key, field, queueJump).ConfigureAwait(false);
                return Settings.ValueConverter.Deserialize<T>(v);
            }
        }

        /// <summary>
        /// HMGET http://redis.io/commands/hmget
        /// </summary>
        public async Task<T[]> Get(string[] fields, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                var v = await Command.Get(Settings.Db, Key, fields, queueJump).ConfigureAwait(false);
                return v.Select(Settings.ValueConverter.Deserialize<T>).ToArray();
            }
        }

        /// <summary>
        /// HGETALL http://redis.io/commands/hgetall
        /// </summary>
        public async Task<Dictionary<string, T>> GetAll(bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                var v = await Command.GetAll(Settings.Db, Key, queueJump).ConfigureAwait(false);
                return v.ToDictionary(x => x.Key, x => Settings.ValueConverter.Deserialize<T>(x.Value));
            }
        }

        /// <summary>
        /// HKEYS http://redis.io/commands/hkeys
        /// </summary>
        public async Task<string[]> GetKeys(bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                return await Command.GetKeys(Settings.Db, Key, queueJump).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// HLEN http://redis.io/commands/hlen
        /// </summary>
        public async Task<long> GetLength(bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                return await Command.GetLength(Settings.Db, Key, queueJump).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// HVALS http://redis.io/commands/hvals
        /// </summary>
        public async Task<T[]> GetValues(bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                var v = await Command.GetValues(Settings.Db, Key, queueJump).ConfigureAwait(false);
                return v.Select(Settings.ValueConverter.Deserialize<T>).ToArray();
            }
        }

        /// <summary>
        /// HINCRBY http://redis.io/commands/hincrby
        /// </summary>
        public async Task<long> Increment(string field, int value = 1, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                return await Command.Increment(Settings.Db, Key, field, value, queueJump).ConfigureAwait(false);
            }
        }

        public async Task<long> IncrementLimitByMax(string field, int value, int max, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                var v = Connection.Scripting.Eval(Settings.Db, HashScript.IncrementLimitByMax, new[] { Key, field }, new object[] { value, max }, useCache: true, inferStrings: true, queueJump: queueJump);
                return (long)(await v.ConfigureAwait(false));
            }
        }

        /// <summary>
        /// HINCRBYFLOAT http://redis.io/commands/hincrbyfloat
        /// </summary>
        public async Task<double> Increment(string field, double value, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                return await Command.Increment(Settings.Db, Key, field, value, queueJump).ConfigureAwait(false);
            }
        }

        public async Task<double> IncrementLimitByMax(string field, double value, double max, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                var v = Connection.Scripting.Eval(Settings.Db, HashScript.IncrementFloatLimitByMax, new[] { Key, field }, new object[] { value, max }, useCache: true, inferStrings: true, queueJump: queueJump);

                return double.Parse((string)(await v.ConfigureAwait(false)));
            }
        }

        public async Task<long> IncrementLimitByMin(string field, int value, int min, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                var v = Connection.Scripting.Eval(Settings.Db, HashScript.IncrementLimitByMin, new[] { Key, field }, new object[] { value, min }, useCache: true, inferStrings: true, queueJump: queueJump);
                return (long)(await v.ConfigureAwait(false));
            }
        }

        public async Task<double> IncrementLimitByMin(string field, double value, double min, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                var v = Connection.Scripting.Eval(Settings.Db, HashScript.IncrementFloatLimitByMin, new[] { Key, field }, new object[] { value, min }, useCache: true, inferStrings: true, queueJump: queueJump);

                return double.Parse((string)(await v.ConfigureAwait(false)));
            }
        }

        /// <summary>
        /// HDEL http://redis.io/commands/hdel
        /// </summary>
        public async Task<bool> Remove(string field, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                return await Command.Remove(Settings.Db, Key, field, queueJump).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// HDEL http://redis.io/commands/hdel
        /// </summary>
        public async Task<long> Remove(string[] fields, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                return await Command.Remove(Settings.Db, Key, fields, queueJump).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// HMSET http://redis.io/commands/hmset
        /// </summary>
        public async Task Set(Dictionary<string, T> values, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                var v = values.ToDictionary(x => x.Key, x => Settings.ValueConverter.Serialize(x.Value));
                await Command.Set(Settings.Db, Key, v, queueJump).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// HSET http://redis.io/commands/hset
        /// </summary>
        public async Task<bool> Set(string field, T value, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                return await Command.Set(Settings.Db, Key, field, Settings.ValueConverter.Serialize(value), queueJump).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// HSETNX http://redis.io/commands/hsetnx
        /// </summary>
        public async Task<bool> SetIfNotExists(string field, T value, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                return await Command.SetIfNotExists(Settings.Db, Key, field, Settings.ValueConverter.Serialize(value), queueJump).ConfigureAwait(false);
            }
        }

        public async Task<bool> Clear(bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                return await Connection.Keys.Remove(Settings.Db, Key, queueJump).ConfigureAwait(false);
            }
        }
    }

    public class RedisHash
    {
        const string CallType = "RedisHash";

        public string Key { get; private set; }
        public RedisSettings Settings { get; private set; }

        public RedisHash(RedisSettings settings, string hashKey)
        {
            this.Settings = settings;
            this.Key = hashKey;
        }

        public RedisHash(RedisGroup connectionGroup, string hashKey)
            : this(connectionGroup.GetSettings(hashKey), hashKey)
        {
        }

        protected RedisConnection Connection
        {
            get
            {
                return Settings.GetConnection();
            }
        }

        protected IHashCommands Command
        {
            get
            {
                return Connection.Hashes;
            }
        }

        /// <summary>
        /// HEXISTS http://redis.io/commands/hexists
        /// </summary>
        public async Task<bool> Exists(string field, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                return await Command.Exists(Settings.Db, Key, field, queueJump).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// HGET http://redis.io/commands/hget
        /// </summary>
        public async Task<T> Get<T>(string field, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                var v = await Command.Get(Settings.Db, Key, field, queueJump).ConfigureAwait(false);
                return Settings.ValueConverter.Deserialize<T>(v);
            }
        }

        /// <summary>
        /// HMGET http://redis.io/commands/hmget
        /// </summary>
        public async Task<T[]> Get<T>(string[] fields, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                var v = await Command.Get(Settings.Db, Key, fields, queueJump).ConfigureAwait(false);
                return v.Select(Settings.ValueConverter.Deserialize<T>).ToArray();
            }
        }

        /// <summary>
        /// HGETALL http://redis.io/commands/hgetall
        /// </summary>
        public async Task<Dictionary<string, T>> GetAll<T>(bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                var v = await Command.GetAll(Settings.Db, Key, queueJump).ConfigureAwait(false);
                return v.ToDictionary(x => x.Key, x => Settings.ValueConverter.Deserialize<T>(x.Value));
            }
        }

        /// <summary>
        /// HKEYS http://redis.io/commands/hkeys
        /// </summary>
        public async Task<string[]> GetKeys(bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                return await Command.GetKeys(Settings.Db, Key, queueJump).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// HLEN http://redis.io/commands/hlen
        /// </summary>
        public async Task<long> GetLength(bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                return await Command.GetLength(Settings.Db, Key, queueJump).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// HVALS http://redis.io/commands/hvals
        /// </summary>
        public async Task<T[]> GetValues<T>(bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                var v = await Command.GetValues(Settings.Db, Key, queueJump).ConfigureAwait(false);
                return v.Select(Settings.ValueConverter.Deserialize<T>).ToArray();
            }
        }

        /// <summary>
        /// HINCRBY http://redis.io/commands/hincrby
        /// </summary>
        public async Task<long> Increment(string field, int value = 1, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                return await Command.Increment(Settings.Db, Key, field, value, queueJump).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// HINCRBYFLOAT http://redis.io/commands/hincrbyfloat
        /// </summary>
        public async Task<double> Increment(string field, double value, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                return await Command.Increment(Settings.Db, Key, field, value, queueJump).ConfigureAwait(false);
            }
        }

        public async Task<long> IncrementLimitByMax(string field, int value, int max, bool queueJump = false)
        {

            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                var v = Connection.Scripting.Eval(Settings.Db, HashScript.IncrementLimitByMax, new[] { Key, field }, new object[] { value, max }, useCache: true, inferStrings: true, queueJump: queueJump);
                return (long)(await v.ConfigureAwait(false));
            }
        }

        public async Task<double> IncrementLimitByMax(string field, double value, double max, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                var v = Connection.Scripting.Eval(Settings.Db, HashScript.IncrementFloatLimitByMax, new[] { Key, field }, new object[] { value, max }, useCache: true, inferStrings: true, queueJump: queueJump);

                return double.Parse((string)(await v.ConfigureAwait(false)));
            }
        }

        public async Task<long> IncrementLimitByMin(string field, int value, int min, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                var v = Connection.Scripting.Eval(Settings.Db, HashScript.IncrementLimitByMin, new[] { Key, field }, new object[] { value, min }, useCache: true, inferStrings: true, queueJump: queueJump);
                return (long)(await v.ConfigureAwait(false));
            }
        }

        public async Task<double> IncrementLimitByMin(string field, double value, double min, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                var v = Connection.Scripting.Eval(Settings.Db, HashScript.IncrementFloatLimitByMin, new[] { Key, field }, new object[] { value, min }, useCache: true, inferStrings: true, queueJump: queueJump);

                return double.Parse((string)(await v.ConfigureAwait(false)));
            }
        }

        /// <summary>
        /// HDEL http://redis.io/commands/hdel
        /// </summary>
        public async Task<bool> Remove(string field, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                return await Command.Remove(Settings.Db, Key, field, queueJump);
            }
        }
        /// <summary>
        /// HDEL http://redis.io/commands/hdel
        /// </summary>
        public async Task<long> Remove(string[] fields, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                return await Command.Remove(Settings.Db, Key, fields, queueJump);
            }
        }

        /// <summary>
        /// HMSET http://redis.io/commands/hmset
        /// </summary>
        public async Task Set(Dictionary<string, object> values, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                var v = values.ToDictionary(x => x.Key, x => Settings.ValueConverter.Serialize(x.Value));
                await Command.Set(Settings.Db, Key, v, queueJump);
            }
        }

        /// <summary>
        /// HSET http://redis.io/commands/hset
        /// </summary>
        public async Task<bool> Set(string field, object value, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                return await Command.Set(Settings.Db, Key, field, Settings.ValueConverter.Serialize(value), queueJump);
            }
        }

        /// <summary>
        /// HSETNX http://redis.io/commands/hsetnx
        /// </summary>
        public async Task<bool> SetIfNotExists(string field, object value, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                return await Command.SetIfNotExists(Settings.Db, Key, field, Settings.ValueConverter.Serialize(value), queueJump);
            }
        }

        public async Task<bool> Clear(bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                return await Connection.Keys.Remove(Settings.Db, Key, queueJump);
            }
        }
    }


    /// <summary>
    /// Class mapped RedisHash
    /// </summary>
    public class RedisClass<T> where T : class, new()
    {
        const string CallType = "RedisClass";

        public string Key { get; private set; }
        public RedisSettings Settings { get; private set; }

        public RedisClass(RedisSettings settings, string hashKey)
        {
            this.Settings = settings;
            this.Key = hashKey;
        }

        public RedisClass(RedisGroup connectionGroup, string hashKey)
            : this(connectionGroup.GetSettings(hashKey), hashKey)
        {
        }

        protected RedisConnection Connection
        {
            get
            {
                return Settings.GetConnection();
            }
        }

        protected IHashCommands Command
        {
            get
            {
                return Connection.Hashes;
            }
        }

        public async Task<T> GetValue(bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                var data = await Command.GetAll(Settings.Db, Key, queueJump).ConfigureAwait(false);
                if (data == null || data.Count == 0)
                {
                    return null;
                }

                var accessor = FastMember.TypeAccessor.Create(typeof(T), allowNonPublicAccessors: false);
                var result = (T)accessor.CreateNew();

                foreach (var member in accessor.GetMembers())
                {
                    byte[] value;
                    if (data.TryGetValue(member.Name, out value))
                    {
                        accessor[result, member.Name] = Settings.ValueConverter.Deserialize(member.Type, value);
                    }
                }

                return result;
            }
        }

        /// <summary>
        /// expire subtract Datetime.Now
        /// </summary>
        public Task<T> GetValueOrSet(Func<T> valueFactory, DateTime expire, bool configureAwait = true, bool queueJump = false)
        {
            return GetValueOrSet(valueFactory, expire - DateTime.Now, configureAwait, queueJump);
        }

        public Task<T> GetValueOrSet(Func<T> valueFactory, TimeSpan expire, bool configureAwait = true, bool queueJump = false)
        {
            return GetValueOrSet(valueFactory, (int)expire.TotalSeconds, configureAwait, queueJump);
        }

        public async Task<T> GetValueOrSet(Func<T> valueFactory, int? expirySeconds = null, bool configureAwait = true, bool queueJump = false)
        {
            var value = await GetValue(queueJump).ConfigureAwait(configureAwait); // keep valueFactory synchronization context
            if (value == null)
            {
                value = valueFactory();
                if (expirySeconds != null)
                {
                    var a = SetValue(value);
                    var b = SetExpire(expirySeconds.Value, queueJump);
                    await Task.WhenAll(a, b).ConfigureAwait(false);
                }
                else
                {
                    await SetValue(value).ConfigureAwait(false);
                }
            }

            return value;
        }

        /// <summary>
        /// expire subtract Datetime.Now
        /// </summary>
        public Task<T> GetValueOrSet(Func<Task<T>> valueFactory, DateTime expire, bool configureAwait = true, bool queueJump = false)
        {
            return GetValueOrSet(valueFactory, expire - DateTime.Now, configureAwait, queueJump);
        }

        public Task<T> GetValueOrSet(Func<Task<T>> valueFactory, TimeSpan expire, bool configureAwait = true, bool queueJump = false)
        {
            return GetValueOrSet(valueFactory, (int)expire.TotalSeconds, configureAwait, queueJump);
        }

        public async Task<T> GetValueOrSet(Func<Task<T>> valueFactory, int? expirySeconds = null, bool configureAwait = true, bool queueJump = false)
        {
            var value = await GetValue(queueJump).ConfigureAwait(configureAwait);
            if (value == null)
            {
                value = await valueFactory().ConfigureAwait(false);
                if (expirySeconds != null)
                {
                    var a = SetValue(value);
                    var b = SetExpire(expirySeconds.Value, queueJump);
                    await Task.WhenAll(a, b).ConfigureAwait(false);
                }
                else
                {
                    await SetValue(value).ConfigureAwait(false);
                }
            }

            return value;
        }

        public async Task SetValue(T value, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                var accessor = FastMember.TypeAccessor.Create(typeof(T), allowNonPublicAccessors: false);
                var members = accessor.GetMembers();
                var values = new Dictionary<string, byte[]>(members.Count);
                foreach (var member in members)
                {
                    values.Add(member.Name, Settings.ValueConverter.Serialize(accessor[value, member.Name]));
                }

                await Command.Set(Settings.Db, Key, values, queueJump).ConfigureAwait(false);
            }
        }

        public async Task<bool> SetField(string field, object value, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                return await Command.Set(Settings.Db, Key, field, Settings.ValueConverter.Serialize(value), queueJump).ConfigureAwait(false);
            }
        }

        public async Task SetFields(IEnumerable<KeyValuePair<string, object>> fields, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                var values = fields.ToDictionary(x => x.Key, x => Settings.ValueConverter.Serialize(x.Value));

                await Command.Set(Settings.Db, Key, values, queueJump).ConfigureAwait(false);
            }
        }

        public async Task<TField> GetField<TField>(string field, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                var v = await Command.Get(Settings.Db, Key, field, queueJump).ConfigureAwait(false);
                return Settings.ValueConverter.Deserialize<TField>(v);
            }
        }

        public async Task<long> Increment(string field, int value = 1, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                return await Command.Increment(Settings.Db, Key, field, value, queueJump).ConfigureAwait(false);
            }
        }

        public async Task<double> Increment(string field, double value, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                return await Command.Increment(Settings.Db, Key, field, value, queueJump).ConfigureAwait(false);
            }
        }

        public async Task<long> IncrementLimitByMax(string field, int value, int max, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                var v = Connection.Scripting.Eval(Settings.Db, HashScript.IncrementLimitByMax, new[] { Key, field }, new object[] { value, max }, useCache: true, inferStrings: true, queueJump: queueJump);
                return (long)(await v.ConfigureAwait(false));
            }
        }

        public async Task<double> IncrementLimitByMax(string field, double value, double max, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                var v = Connection.Scripting.Eval(Settings.Db, HashScript.IncrementFloatLimitByMax, new[] { Key, field }, new object[] { value, max }, useCache: true, inferStrings: true, queueJump: queueJump);

                return double.Parse((string)(await v.ConfigureAwait(false)));
            }
        }

        public async Task<long> IncrementLimitByMin(string field, int value, int min, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                var v = Connection.Scripting.Eval(Settings.Db, HashScript.IncrementLimitByMin, new[] { Key, field }, new object[] { value, min }, useCache: true, inferStrings: true, queueJump: queueJump);
                return (long)(await v.ConfigureAwait(false));
            }
        }

        public async Task<double> IncrementLimitByMin(string field, double value, double min, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                var v = Connection.Scripting.Eval(Settings.Db, HashScript.IncrementFloatLimitByMin, new[] { Key, field }, new object[] { value, min }, useCache: true, inferStrings: true, queueJump: queueJump);

                return double.Parse((string)(await v.ConfigureAwait(false)));
            }
        }

        /// <summary>
        /// expire subtract Datetime.Now
        /// </summary>
        public Task<bool> SetExpire(DateTime expire, bool queueJump = false)
        {
            return SetExpire(expire - DateTime.Now, queueJump);
        }

        public Task<bool> SetExpire(TimeSpan expire, bool queueJump = false)
        {
            return SetExpire((int)expire.TotalSeconds, queueJump);
        }

        public async Task<bool> SetExpire(int seconds, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                return await Connection.Keys.Expire(Settings.Db, Key, seconds, queueJump).ConfigureAwait(false);
            }
        }

        public async Task<bool> Clear(bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                return await Connection.Keys.Remove(Settings.Db, Key, queueJump).ConfigureAwait(false);
            }
        }
    }
}