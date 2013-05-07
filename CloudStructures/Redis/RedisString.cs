using BookSleeve;
using System;
using System.Threading.Tasks;

namespace CloudStructures.Redis
{
    public class RedisString<T>
    {
        const string CallType = "RedisString";

        public string Key { get; private set; }
        public RedisSettings Settings { get; private set; }

        public RedisString(RedisSettings settings, string stringKey)
        {
            this.Settings = settings;
            this.Key = stringKey;
        }

        public RedisString(RedisGroup connectionGroup, string stringKey)
            : this(connectionGroup.GetSettings(stringKey), stringKey)
        {
        }

        protected RedisConnection Connection
        {
            get
            {
                return Settings.GetConnection();
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
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                var value = await Command.Get(Settings.Db, Key, queueJump).ConfigureAwait(false);
                return (value == null)
                    ? Tuple.Create(false, default(T))
                    : Tuple.Create(true, Settings.ValueConverter.Deserialize<T>(value));
            }
        }

        public Task<T> GetOrSet(Func<T> valueFactory, TimeSpan expire, bool configureAwait = true, bool queueJump = false)
        {
            return GetOrSet(valueFactory, (int)expire.TotalSeconds, configureAwait, queueJump);
        }

        public async Task<T> GetOrSet(Func<T> valueFactory, int? expirySeconds = null, bool configureAwait = true, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                var value = await TryGet(queueJump).ConfigureAwait(configureAwait); // keep valueFactory synchronization context
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
        }

        public Task<T> GetOrSet(Func<Task<T>> valueFactory, TimeSpan expire, bool configureAwait = true, bool queueJump = false)
        {
            return GetOrSet(valueFactory, (int)expire.TotalSeconds, queueJump);
        }

        public async Task<T> GetOrSet(Func<Task<T>> valueFactory, int? expirySeconds = null, bool configureAwait = true, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                var value = await TryGet(queueJump).ConfigureAwait(configureAwait); // keep valueFactory synchronization context
                if (value.Item1)
                {
                    return value.Item2;
                }
                else
                {
                    var v = await valueFactory().ConfigureAwait(false);
                    await Set(v, expirySeconds, queueJump).ConfigureAwait(false);
                    return v;
                }
            }
        }

        public Task Set(T value, TimeSpan expire, bool queueJump = false)
        {
            return Set(value, (int)expire.TotalSeconds, queueJump);
        }

        public Task Set(T value, long? expirySeconds = null, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                var v = Settings.ValueConverter.Serialize(value);
                if (expirySeconds == null)
                {
                    return Command.Set(Settings.Db, Key, v, queueJump: queueJump);
                }
                else
                {
                    return Command.Set(Settings.Db, Key, v, expirySeconds.Value, queueJump: queueJump);
                }
            }
        }

        public async Task<bool> Remove(bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                return await Connection.Keys.Remove(Settings.Db, Key, queueJump).ConfigureAwait(false);
            }
        }

        public async Task<long> Increment(long value = 1, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                return await Command.Increment(Settings.Db, Key, value, queueJump).ConfigureAwait(false);
            }
        }

        public async Task<double> Increment(double value, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                return await Command.Increment(Settings.Db, Key, value, queueJump).ConfigureAwait(false);
            }
        }

        public async Task<long> Decrement(long value = 1, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                return await Command.Decrement(Settings.Db, Key, value, queueJump).ConfigureAwait(false);
            }
        }

        public async Task<bool> SetExpire(TimeSpan expire, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                return await SetExpire((int)expire.TotalSeconds, queueJump).ConfigureAwait(false);
            }
        }

        public async Task<bool> SetExpire(int seconds, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                return await Connection.Keys.Expire(Settings.Db, Key, seconds, queueJump).ConfigureAwait(false);
            }
        }

        public async Task<long> IncrementLimitByMax(long value, long max, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                var v = Connection.Scripting.Eval(Settings.Db, @"
local inc = tonumber(ARGV[1])
local max = tonumber(ARGV[2])
local x = redis.call('incrby', KEYS[1], inc)
if(x > max) then
    redis.call('set', KEYS[1], max)
    x = max
end
return x", new[] { Key }, new object[] { value, max }, useCache: true, inferStrings: true, queueJump: queueJump);
                return (long)(await v.ConfigureAwait(false));
            }
        }

        public async Task<long> IncrementLimitByMin(long value, long min, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                var v = Connection.Scripting.Eval(Settings.Db, @"
local inc = tonumber(ARGV[1])
local min = tonumber(ARGV[2])
local x = redis.call('incrby', KEYS[1], inc)
if(x < min) then
    redis.call('set', KEYS[1], min)
    x = min
end
return x", new[] { Key }, new object[] { value, min }, useCache: true, inferStrings: true, queueJump: queueJump);
                return (long)(await v.ConfigureAwait(false));
            }
        }

        public async Task<double> IncrementLimitByMax(double value, double max, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                var v = Connection.Scripting.Eval(Settings.Db, @"
local inc = tonumber(ARGV[1])
local max = tonumber(ARGV[2])
local x = tonumber(redis.call('incrbyfloat', KEYS[1], inc))
if(x > max) then
    redis.call('set', KEYS[1], max)
    x = max
end
return tostring(x)", new[] { Key }, new object[] { value, max }, useCache: true, inferStrings: true, queueJump: queueJump);
                return double.Parse((string)(await v.ConfigureAwait(false)));
            }
        }

        public async Task<double> IncrementLimitByMin(double value, double min, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                var v = Connection.Scripting.Eval(Settings.Db, @"
local inc = tonumber(ARGV[1])
local min = tonumber(ARGV[2])
local x = tonumber(redis.call('incrbyfloat', KEYS[1], inc))
if(x < min) then
    redis.call('set', KEYS[1], min)
    x = min
end
return tostring(x)", new[] { Key }, new object[] { value, min }, useCache: true, inferStrings: true, queueJump: queueJump);
                return double.Parse((string)(await v.ConfigureAwait(false)));
            }
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