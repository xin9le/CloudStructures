using BookSleeve;
using System;
using System.Threading.Tasks;

namespace CloudStructures.Redis
{
    public class RedisString<T>
    {
        public string Key { get; private set; }
        public int Db { get; private set; }
        readonly RedisSettings settings;
        readonly RedisTransaction transaction;
        readonly IRedisValueConverter valueConverter;

        public RedisString(RedisSettings settings, string stringKey)
        {
            this.settings = settings;
            this.Db = settings.Db;
            this.valueConverter = settings.ValueConverter;
            this.Key = stringKey;
        }

        public RedisString(RedisGroup connectionGroup, string stringKey)
            : this(connectionGroup.GetSettings(stringKey), stringKey)
        {
        }

        public RedisString(RedisTransaction transaction, int db, IRedisValueConverter valueConverter, string stringKey)
        {
            this.transaction = transaction;
            this.Db = db;
            this.valueConverter = valueConverter;
            this.Key = stringKey;
        }

        protected RedisConnection Connection
        {
            get
            {
                return (transaction == null) ? settings.GetConnection() : transaction;
            }
        }

        protected IStringCommands Command
        {
            get
            {
                return Connection.Strings;
            }
        }

        public async Task<Tuple<bool, T>> TryGet(bool queueJump = false)
        {
            var value = await Command.Get(Db, Key, queueJump).ConfigureAwait(false);
            return (value == null)
                ? Tuple.Create(false, default(T))
                : Tuple.Create(true, valueConverter.Deserialize<T>(value));
        }

        public async Task<T> GetOrAdd(Func<T> valueFactory, int? expirySeconds = null, bool queueJump = false)
        {
            var value = await TryGet(queueJump).ConfigureAwait(false);
            if (value.Item1)
            {
                return value.Item2;
            }
            else
            {
                var v = valueFactory();
                await Set(v, expirySeconds, queueJump).ConfigureAwait(false);
                return v;
            }
        }

        public Task Set(T value, long? expirySeconds = null, bool queueJump = false)
        {
            var v = valueConverter.Serialize(value);
            if (expirySeconds == null)
            {
                return Command.Set(Db, Key, v, queueJump: queueJump);
            }
            else
            {
                return Command.Set(Db, Key, v, expirySeconds.Value, queueJump: queueJump);
            }
        }

        public Task<bool> Remove(bool queueJump = false)
        {
            return Connection.Keys.Remove(Db, Key, queueJump);
        }

        public Task<long> Increment(long value = 1, bool queueJump = false)
        {
            return Command.Increment(Db, Key, value, queueJump);
        }

        public Task<double> Increment(double value, bool queueJump = false)
        {
            return Command.Increment(Db, Key, value, queueJump);
        }

        public Task<long> Decrement(long value = 1, bool queueJump = false)
        {
            return Command.Decrement(Db, Key, value, queueJump);
        }

        public Task<long> IncrementLimitByMax(long value, long max, bool queueJump = false)
        {
            var v = Connection.Scripting.Eval(Db, @"
local inc = tonumber(ARGV[1])
local max = tonumber(ARGV[2])
local x = redis.call('incrby', KEYS[1], inc)
if(x > max) then
    redis.call('set', KEYS[1], max)
    x = max
end
return x", new[] { Key }, new object[] { value, max }, useCache: true, inferStrings: true, queueJump: queueJump);
            return v.ContinueWith(x => (long)x.Result);
        }

        public Task<long> IncrementLimitByMin(long value, long min, bool queueJump = false)
        {
            var v = Connection.Scripting.Eval(Db, @"
local inc = tonumber(ARGV[1])
local min = tonumber(ARGV[2])
local x = redis.call('incrby', KEYS[1], inc)
if(x < min) then
    redis.call('set', KEYS[1], min)
    x = min
end
return x", new[] { Key }, new object[] { value, min }, useCache: true, inferStrings: true, queueJump: queueJump);
            return v.ContinueWith(x => (long)x.Result);
        }

        public Task<double> IncrementLimitByMax(double value, double max, bool queueJump = false)
        {
            var v = Connection.Scripting.Eval(Db, @"
local inc = tonumber(ARGV[1])
local max = tonumber(ARGV[2])
local x = tonumber(redis.call('incrbyfloat', KEYS[1], inc))
if(x > max) then
    redis.call('set', KEYS[1], max)
    x = max
end
return tostring(x)", new[] { Key }, new object[] { value, max }, useCache: true, inferStrings: true, queueJump: queueJump);
            return v.ContinueWith(x => double.Parse((string)x.Result));
        }

        public Task<double> IncrementLimitByMin(double value, double min, bool queueJump = false)
        {
            var v = Connection.Scripting.Eval(Db, @"
local inc = tonumber(ARGV[1])
local min = tonumber(ARGV[2])
local x = tonumber(redis.call('incrbyfloat', KEYS[1], inc))
if(x < min) then
    redis.call('set', KEYS[1], min)
    x = min
end
return tostring(x)", new[] { Key }, new object[] { value, min }, useCache: true, inferStrings: true, queueJump: queueJump);
            return v.ContinueWith(x => double.Parse((string)x.Result));
        }
    }

    public static class RedisStringExtensions
    {
        public static async Task<T> GetValueOrDefault<T>(this RedisString<T> redis, T defaultValue = default(T), bool queueJump = false)
        {
            var result = await redis.TryGet(queueJump).ConfigureAwait(false);
            return result.Item1 ? result.Item2 : defaultValue;
        }
    }
}