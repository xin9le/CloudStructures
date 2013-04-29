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
        public Task<bool> Exists(string field, bool queueJump = false)
        {
            return Command.Exists(Settings.Db, Key, field, queueJump);
        }

        /// <summary>
        /// HGET http://redis.io/commands/hget
        /// </summary>
        public async Task<T> Get(string field, bool queueJump = false)
        {
            var v = await Command.Get(Settings.Db, Key, field, queueJump).ConfigureAwait(false);
            return Settings.ValueConverter.Deserialize<T>(v);
        }

        /// <summary>
        /// HMGET http://redis.io/commands/hmget
        /// </summary>
        public async Task<T[]> Get(string[] fields, bool queueJump = false)
        {
            var v = await Command.Get(Settings.Db, Key, fields, queueJump).ConfigureAwait(false);
            return v.Select(Settings.ValueConverter.Deserialize<T>).ToArray();
        }

        /// <summary>
        /// HGETALL http://redis.io/commands/hgetall
        /// </summary>
        public async Task<Dictionary<string, T>> GetAll(bool queueJump = false)
        {
            var v = await Command.GetAll(Settings.Db, Key, queueJump).ConfigureAwait(false);
            return v.ToDictionary(x => x.Key, x => Settings.ValueConverter.Deserialize<T>(x.Value));
        }

        /// <summary>
        /// HKEYS http://redis.io/commands/hkeys
        /// </summary>
        public Task<string[]> GetKeys(bool queueJump = false)
        {
            return Command.GetKeys(Settings.Db, Key, queueJump);
        }

        /// <summary>
        /// HLEN http://redis.io/commands/hlen
        /// </summary>
        public Task<long> GetLength(bool queueJump = false)
        {
            return Command.GetLength(Settings.Db, Key, queueJump);
        }

        /// <summary>
        /// HVALS http://redis.io/commands/hvals
        /// </summary>
        public async Task<T[]> GetValues(bool queueJump = false)
        {
            var v = await Command.GetValues(Settings.Db, Key, queueJump).ConfigureAwait(false);
            return v.Select(Settings.ValueConverter.Deserialize<T>).ToArray();
        }

        /// <summary>
        /// HINCRBY http://redis.io/commands/hincrby
        /// </summary>
        public Task<long> Increment(string field, int value = 1, bool queueJump = false)
        {
            return Command.Increment(Settings.Db, Key, field, value, queueJump);
        }

        public Task<long> IncrementLimitByMax(string field, int value, int max, bool queueJump = false)
        {
            var v = Connection.Scripting.Eval(Settings.Db, HashScript.IncrementLimitByMax, new[] { Key, field }, new object[] { value, max }, useCache: true, inferStrings: true, queueJump: queueJump);
            return v.ContinueWith(x => (long)x.Result);
        }

        /// <summary>
        /// HINCRBYFLOAT http://redis.io/commands/hincrbyfloat
        /// </summary>
        public Task<double> Increment(string field, double value, bool queueJump = false)
        {
            return Command.Increment(Settings.Db, Key, field, value, queueJump);
        }

        public Task<double> IncrementLimitByMax(string field, double value, double max, bool queueJump = false)
        {
            var v = Connection.Scripting.Eval(Settings.Db, HashScript.IncrementFloatLimitByMax, new[] { Key, field }, new object[] { value, max }, useCache: true, inferStrings: true, queueJump: queueJump);

            return v.ContinueWith(x => double.Parse((string)x.Result));
        }

        public Task<long> IncrementLimitByMin(string field, int value, int min, bool queueJump = false)
        {
            var v = Connection.Scripting.Eval(Settings.Db, HashScript.IncrementLimitByMin, new[] { Key, field }, new object[] { value, min }, useCache: true, inferStrings: true, queueJump: queueJump);
            return v.ContinueWith(x => (long)x.Result);
        }

        public Task<double> IncrementLimitByMin(string field, double value, double min, bool queueJump = false)
        {
            var v = Connection.Scripting.Eval(Settings.Db, HashScript.IncrementFloatLimitByMin, new[] { Key, field }, new object[] { value, min }, useCache: true, inferStrings: true, queueJump: queueJump);

            return v.ContinueWith(x => double.Parse((string)x.Result));
        }

        /// <summary>
        /// HDEL http://redis.io/commands/hdel
        /// </summary>
        public Task<bool> Remove(string field, bool queueJump = false)
        {
            return Command.Remove(Settings.Db, Key, field, queueJump);
        }
        /// <summary>
        /// HDEL http://redis.io/commands/hdel
        /// </summary>
        public Task<long> Remove(string[] fields, bool queueJump = false)
        {
            return Command.Remove(Settings.Db, Key, fields, queueJump);
        }

        /// <summary>
        /// HMSET http://redis.io/commands/hmset
        /// </summary>
        public Task Set(Dictionary<string, T> values, bool queueJump = false)
        {
            var v = values.ToDictionary(x => x.Key, x => Settings.ValueConverter.Serialize(x.Value));
            return Command.Set(Settings.Db, Key, v, queueJump);
        }

        /// <summary>
        /// HSET http://redis.io/commands/hset
        /// </summary>
        public Task<bool> Set(string field, T value, bool queueJump = false)
        {
            return Command.Set(Settings.Db, Key, field, Settings.ValueConverter.Serialize(value), queueJump);
        }

        /// <summary>
        /// HSETNX http://redis.io/commands/hsetnx
        /// </summary>
        public Task<bool> SetIfNotExists(string field, T value, bool queueJump = false)
        {
            return Command.SetIfNotExists(Settings.Db, Key, field, Settings.ValueConverter.Serialize(value), queueJump);
        }

        public Task<bool> Clear(bool queueJump = false)
        {
            return Connection.Keys.Remove(Settings.Db, Key, queueJump);
        }
    }

    public class RedisHash
    {
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
        public Task<bool> Exists(string field, bool queueJump = false)
        {
            return Command.Exists(Settings.Db, Key, field, queueJump);
        }

        /// <summary>
        /// HGET http://redis.io/commands/hget
        /// </summary>
        public async Task<T> Get<T>(string field, bool queueJump = false)
        {
            var v = await Command.Get(Settings.Db, Key, field, queueJump).ConfigureAwait(false);
            return Settings.ValueConverter.Deserialize<T>(v);
        }

        /// <summary>
        /// HMGET http://redis.io/commands/hmget
        /// </summary>
        public async Task<T[]> Get<T>(string[] fields, bool queueJump = false)
        {
            var v = await Command.Get(Settings.Db, Key, fields, queueJump).ConfigureAwait(false);
            return v.Select(Settings.ValueConverter.Deserialize<T>).ToArray();
        }

        /// <summary>
        /// HGETALL http://redis.io/commands/hgetall
        /// </summary>
        public async Task<Dictionary<string, T>> GetAll<T>(bool queueJump = false)
        {
            var v = await Command.GetAll(Settings.Db, Key, queueJump).ConfigureAwait(false);
            return v.ToDictionary(x => x.Key, x => Settings.ValueConverter.Deserialize<T>(x.Value));
        }

        /// <summary>
        /// HKEYS http://redis.io/commands/hkeys
        /// </summary>
        public Task<string[]> GetKeys(bool queueJump = false)
        {
            return Command.GetKeys(Settings.Db, Key, queueJump);
        }

        /// <summary>
        /// HLEN http://redis.io/commands/hlen
        /// </summary>
        public Task<long> GetLength(bool queueJump = false)
        {
            return Command.GetLength(Settings.Db, Key, queueJump);
        }

        /// <summary>
        /// HVALS http://redis.io/commands/hvals
        /// </summary>
        public async Task<T[]> GetValues<T>(bool queueJump = false)
        {
            var v = await Command.GetValues(Settings.Db, Key, queueJump).ConfigureAwait(false);
            return v.Select(Settings.ValueConverter.Deserialize<T>).ToArray();
        }

        /// <summary>
        /// HINCRBY http://redis.io/commands/hincrby
        /// </summary>
        public Task<long> Increment(string field, int value = 1, bool queueJump = false)
        {
            return Command.Increment(Settings.Db, Key, field, value, queueJump);
        }

        /// <summary>
        /// HINCRBYFLOAT http://redis.io/commands/hincrbyfloat
        /// </summary>
        public Task<double> Increment(string field, double value, bool queueJump = false)
        {
            return Command.Increment(Settings.Db, Key, field, value, queueJump);
        }

        public Task<long> IncrementLimitByMax(string field, int value, int max, bool queueJump = false)
        {
            var v = Connection.Scripting.Eval(Settings.Db, HashScript.IncrementLimitByMax, new[] { Key, field }, new object[] { value, max }, useCache: true, inferStrings: true, queueJump: queueJump);
            return v.ContinueWith(x => (long)x.Result);
        }

        public Task<double> IncrementLimitByMax(string field, double value, double max, bool queueJump = false)
        {
            var v = Connection.Scripting.Eval(Settings.Db, HashScript.IncrementFloatLimitByMax, new[] { Key, field }, new object[] { value, max }, useCache: true, inferStrings: true, queueJump: queueJump);

            return v.ContinueWith(x => double.Parse((string)x.Result));
        }

        public Task<long> IncrementLimitByMin(string field, int value, int min, bool queueJump = false)
        {
            var v = Connection.Scripting.Eval(Settings.Db, HashScript.IncrementLimitByMin, new[] { Key, field }, new object[] { value, min }, useCache: true, inferStrings: true, queueJump: queueJump);
            return v.ContinueWith(x => (long)x.Result);
        }

        public Task<double> IncrementLimitByMin(string field, double value, double min, bool queueJump = false)
        {
            var v = Connection.Scripting.Eval(Settings.Db, HashScript.IncrementFloatLimitByMin, new[] { Key, field }, new object[] { value, min }, useCache: true, inferStrings: true, queueJump: queueJump);

            return v.ContinueWith(x => double.Parse((string)x.Result));
        }

        /// <summary>
        /// HDEL http://redis.io/commands/hdel
        /// </summary>
        public Task<bool> Remove(string field, bool queueJump = false)
        {
            return Command.Remove(Settings.Db, Key, field, queueJump);
        }
        /// <summary>
        /// HDEL http://redis.io/commands/hdel
        /// </summary>
        public Task<long> Remove(string[] fields, bool queueJump = false)
        {
            return Command.Remove(Settings.Db, Key, fields, queueJump);
        }

        /// <summary>
        /// HMSET http://redis.io/commands/hmset
        /// </summary>
        public Task Set(Dictionary<string, object> values, bool queueJump = false)
        {
            var v = values.ToDictionary(x => x.Key, x => Settings.ValueConverter.Serialize(x.Value));
            return Command.Set(Settings.Db, Key, v, queueJump);
        }

        /// <summary>
        /// HSET http://redis.io/commands/hset
        /// </summary>
        public Task<bool> Set(string field, object value, bool queueJump = false)
        {
            return Command.Set(Settings.Db, Key, field, Settings.ValueConverter.Serialize(value), queueJump);
        }

        /// <summary>
        /// HSETNX http://redis.io/commands/hsetnx
        /// </summary>
        public Task<bool> SetIfNotExists(string field, object value, bool queueJump = false)
        {
            return Command.SetIfNotExists(Settings.Db, Key, field, Settings.ValueConverter.Serialize(value), queueJump);
        }

        public Task<bool> Clear(bool queueJump = false)
        {
            return Connection.Keys.Remove(Settings.Db, Key, queueJump);
        }
    }


    /// <summary>
    /// Class mapped RedisHash
    /// </summary>
    public class RedisClass<T> where T : class, new()
    {
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
            var data = await Command.GetAll(Settings.Db, Key, queueJump).ConfigureAwait(false);
            if (data == null)
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

        public Task<T> GetValueOrSet(Func<T> valueFactory, TimeSpan expire, bool queueJump = false)
        {
            return GetValueOrSet(valueFactory, (int)expire.TotalSeconds, queueJump);
        }

        public async Task<T> GetValueOrSet(Func<T> valueFactory, int? expirySeconds = null, bool queueJump = false)
        {
            var value = await GetValue(queueJump).ConfigureAwait(false);
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

        public Task SetValue(T value, bool queueJump = false)
        {
            var accessor = FastMember.TypeAccessor.Create(typeof(T), allowNonPublicAccessors: false);
            var members = accessor.GetMembers();
            var values = new Dictionary<string, byte[]>(members.Count);
            foreach (var member in members)
            {
                values.Add(member.Name, Settings.ValueConverter.Serialize(accessor[value, member.Name]));
            }

            return Command.Set(Settings.Db, Key, values, queueJump);
        }

        public Task<bool> SetField(string field, object value, bool queueJump = false)
        {
            return Command.Set(Settings.Db, Key, field, Settings.ValueConverter.Serialize(value), queueJump);
        }

        public Task SetFields(Tuple<string, object>[] fields, bool queueJump = false)
        {
            var accessor = FastMember.TypeAccessor.Create(typeof(T), allowNonPublicAccessors: false);
            var values = new Dictionary<string, byte[]>(fields.Length);
            foreach (var field in fields)
            {
                values.Add(field.Item1, Settings.ValueConverter.Serialize(accessor[field.Item2, field.Item1]));
            }

            return Command.Set(Settings.Db, Key, values, queueJump);
        }

        public async Task<TField> GetField<TField>(string field, bool queueJump = false)
        {
            var v = await Command.Get(Settings.Db, Key, field, queueJump).ConfigureAwait(false);
            return Settings.ValueConverter.Deserialize<TField>(v);
        }

        public Task<long> Increment(string field, int value = 1, bool queueJump = false)
        {
            return Command.Increment(Settings.Db, Key, field, value, queueJump);
        }

        public Task<double> Increment(string field, double value, bool queueJump = false)
        {
            return Command.Increment(Settings.Db, Key, field, value, queueJump);
        }

        public Task<long> IncrementLimitByMax(string field, int value, int max, bool queueJump = false)
        {
            var v = Connection.Scripting.Eval(Settings.Db, HashScript.IncrementLimitByMax, new[] { Key, field }, new object[] { value, max }, useCache: true, inferStrings: true, queueJump: queueJump);
            return v.ContinueWith(x => (long)x.Result);
        }

        public Task<double> IncrementLimitByMax(string field, double value, double max, bool queueJump = false)
        {
            var v = Connection.Scripting.Eval(Settings.Db, HashScript.IncrementFloatLimitByMax, new[] { Key, field }, new object[] { value, max }, useCache: true, inferStrings: true, queueJump: queueJump);

            return v.ContinueWith(x => double.Parse((string)x.Result));
        }

        public Task<long> IncrementLimitByMin(string field, int value, int min, bool queueJump = false)
        {
            var v = Connection.Scripting.Eval(Settings.Db, HashScript.IncrementLimitByMin, new[] { Key, field }, new object[] { value, min }, useCache: true, inferStrings: true, queueJump: queueJump);
            return v.ContinueWith(x => (long)x.Result);
        }

        public Task<double> IncrementLimitByMin(string field, double value, double min, bool queueJump = false)
        {
            var v = Connection.Scripting.Eval(Settings.Db, HashScript.IncrementFloatLimitByMin, new[] { Key, field }, new object[] { value, min }, useCache: true, inferStrings: true, queueJump: queueJump);

            return v.ContinueWith(x => double.Parse((string)x.Result));
        }

        public Task<bool> SetExpire(TimeSpan expire, bool queueJump = false)
        {
            return SetExpire((int)expire.TotalSeconds, queueJump);
        }

        public Task<bool> SetExpire(int seconds, bool queueJump = false)
        {
            return Connection.Keys.Expire(Settings.Db, Key, seconds, queueJump);
        }

        public Task<bool> Clear(bool queueJump = false)
        {
            return Connection.Keys.Remove(Settings.Db, Key, queueJump);
        }
    }
}