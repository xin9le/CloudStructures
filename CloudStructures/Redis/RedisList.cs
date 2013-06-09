using BookSleeve;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CloudStructures.Redis
{
    public class RedisList<T>
    {
        const string CallType = "RedisList";

        public string Key { get; private set; }
        public RedisSettings Settings { get; private set; }

        public RedisList(RedisSettings settings, string listKey)
        {
            this.Settings = settings;
            this.Key = listKey;
        }

        public RedisList(RedisGroup connectionGroup, string listKey)
            : this(connectionGroup.GetSettings(listKey), listKey)
        {
        }

        protected RedisConnection Connection
        {
            get
            {
                return Settings.GetConnection();
            }
        }

        protected IListCommands Command
        {
            get
            {
                return Connection.Lists;
            }
        }

        /// <summary>
        /// LPUSH http://redis.io/commands/lpush
        /// </summary>
        public async Task<long> AddFirst(T value, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                var v = Settings.ValueConverter.Serialize(value);
                return await Command.AddFirst(Settings.Db, Key, v, createIfMissing: true, queueJump: queueJump).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// RPUSH http://redis.io/commands/rpush
        /// </summary>
        public async Task<long> AddLast(T value, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                var v = Settings.ValueConverter.Serialize(value);
                return await Command.AddLast(Settings.Db, Key, v, createIfMissing: true, queueJump: queueJump).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// LINDEX http://redis.io/commands/lindex
        /// </summary>
        public async Task<Tuple<bool, T>> TryGet(int index, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                var value = await Command.Get(Settings.Db, Key, index, queueJump).ConfigureAwait(false);
                return (value == null)
                    ? Tuple.Create(false, default(T))
                    : Tuple.Create(true, Settings.ValueConverter.Deserialize<T>(value));
            }
        }

        /// <summary>
        /// LLEN http://redis.io/commands/llen
        /// </summary>
        public async Task<long> GetLength(bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                return await Command.GetLength(Settings.Db, Key, queueJump).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// LRANGE http://redis.io/commands/lrange
        /// </summary>
        public async Task<T[]> Range(int start, int stop, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                var results = await Command.Range(Settings.Db, Key, start, stop, queueJump).ConfigureAwait(false);
                return results.Select(Settings.ValueConverter.Deserialize<T>).ToArray();
            }
        }

        /// <summary>
        /// LREM http://redis.io/commands/lrem
        /// </summary>
        public async Task<long> Remove(T value, int count = 1, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                var v = Settings.ValueConverter.Serialize(value);
                return await Command.Remove(Settings.Db, Key, v, count, queueJump).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// LPOP http://redis.io/commands/lpop
        /// </summary>
        public async Task<T> RemoveFirst(bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                var result = await Command.RemoveFirst(Settings.Db, Key, queueJump).ConfigureAwait(false);
                return Settings.ValueConverter.Deserialize<T>(result);
            }
        }

        /// <summary>
        /// RPOP http://redis.io/commands/rpop
        /// </summary>
        public async Task<T> RemoveLast(bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                var result = await Command.RemoveLast(Settings.Db, Key, queueJump).ConfigureAwait(false);
                return Settings.ValueConverter.Deserialize<T>(result);
            }
        }

        /// <summary>
        /// LSET http://redis.io/commands/lset
        /// </summary>
        public async Task Set(int index, T value, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                var v = Settings.ValueConverter.Serialize(value);
                await Command.Set(Settings.Db, Key, index, v, queueJump).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// LTRIM http://redis.io/commands/ltrim
        /// </summary>
        public async Task Trim(int count, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                await Command.Trim(Settings.Db, Key, count, queueJump).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// LTRIM http://redis.io/commands/ltrim
        /// </summary>
        public async Task Trim(int start, int stop, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                await Command.Trim(Settings.Db, Key, start, stop, queueJump).ConfigureAwait(false);
            }
        }

        // additional commands

        public async Task<long> AddFirstAndFixLength(T value, int fixLength, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                var v = Settings.ValueConverter.Serialize(value);
                using (var tx = Settings.GetConnection().CreateTransaction())
                {
                    var addResult = tx.Lists.AddFirst(Settings.Db, Key, v, createIfMissing: true, queueJump: queueJump);
                    var trimResult = tx.Lists.Trim(Settings.Db, Key, fixLength - 1, queueJump);

                    await tx.Execute(queueJump).ConfigureAwait(false);
                    return await addResult.ConfigureAwait(false);
                }
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

        public async Task<T[]> ToArray(bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                var length = await GetLength().ConfigureAwait(false);
                return await Range(0, (int)length, queueJump).ConfigureAwait(false);
            }
        }
    }

    public static class RedisListExtensions
    {
        /// <summary>
        /// LINDEX http://redis.io/commands/lindex
        /// </summary>
        public static async Task<T> GetOrDefault<T>(this RedisList<T> redis, int index, T defaultValue = default(T), bool queueJump = false)
        {
            var result = await redis.TryGet(index, queueJump).ConfigureAwait(false);
            return result.Item1 ? result.Item2 : defaultValue;
        }
    }
}