using BookSleeve;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace CloudStructures.Redis
{
    public class RedisSortedSet<T>
    {
        const string CallType = "RedisSortedSet";

        public string Key { get; private set; }
        public RedisSettings Settings { get; private set; }

        public RedisSortedSet(RedisSettings settings, string stringKey)
        {
            this.Settings = settings;
            this.Key = stringKey;
        }

        public RedisSortedSet(RedisGroup connectionGroup, string stringKey)
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

        protected ISortedSetCommands Command
        {
            get
            {
                return Connection.SortedSets;
            }
        }

        /// <summary>
        /// ZADD http://redis.io/commands/zadd
        /// </summary>
        public async Task<bool> Add(T value, double score, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                return await Command.Add(Settings.Db, Key, Settings.ValueConverter.Serialize(value), score, queueJump).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// ZCARD http://redis.io/commands/zcard
        /// </summary>
        public async Task<long> GetLength(bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                return await Command.GetLength(Settings.Db, Key, queueJump).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// ZCOUNT http://redis.io/commands/zcount
        /// </summary>
        public async Task<long> GetLength(double min, double max, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                return await Command.GetLength(Settings.Db, Key, min, max, queueJump).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// ZINCRBY http://redis.io/commands/zincrby
        /// </summary>
        public async Task<double> Increment(T member, double delta, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                return await Command.Increment(Settings.Db, Key, Settings.ValueConverter.Serialize(member), delta, queueJump).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// ZINCRBY http://redis.io/commands/zincrby
        /// </summary>
        public async Task<double[]> Increment(T[] members, double delta, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                var v = members.Select(x => Settings.ValueConverter.Serialize(x)).ToArray();
                return await Task.WhenAll(Command.Increment(Settings.Db, Key, v, delta, queueJump)).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// ZRANGE http://redis.io/commands/zrange
        /// </summary>
        public async Task<KeyValuePair<T, double>[]> Range(long start, long stop, bool ascending = true, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                var v = await Command.Range(Settings.Db, Key, start, stop, ascending, queueJump).ConfigureAwait(false);
                return v.Select(x => new KeyValuePair<T, double>(Settings.ValueConverter.Deserialize<T>(x.Key), x.Value)).ToArray();
            }
        }

        /// <summary>
        /// ZRANGEBYSCORE http://redis.io/commands/zrangebyscore
        /// </summary>
        public async Task<KeyValuePair<T, double>[]> Range(double min = -1.0 / 0.0, double max = 1.0 / 0.0, bool ascending = true, bool minInclusive = true, bool maxInclusive = true, long offset = 0, long count = 9223372036854775807, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                var v = await Command.Range(Settings.Db, Key, min, max, ascending, minInclusive, maxInclusive, offset, count, queueJump).ConfigureAwait(false);
                return v.Select(x => new KeyValuePair<T, double>(Settings.ValueConverter.Deserialize<T>(x.Key), x.Value)).ToArray();
            }
        }

        /// <summary>
        /// ZRANK http://redis.io/commands/zrank
        /// </summary>
        public async Task<long?> Rank(T member, bool ascending = true, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                return await Command.Rank(Settings.Db, Key, Settings.ValueConverter.Serialize(member), ascending, queueJump).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// ZREM http://redis.io/commands/zrem
        /// </summary>
        public async Task<bool> Remove(T member, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                return await Command.Remove(Settings.Db, Key, Settings.ValueConverter.Serialize(member), queueJump).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// ZREM http://redis.io/commands/zrem
        /// </summary>
        public async Task<long> Remove(T[] members, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                var v = members.Select(x => Settings.ValueConverter.Serialize(x)).ToArray();
                return await Command.Remove(Settings.Db, Key, v, queueJump).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// ZREMRANGEBYRANK http://redis.io/commands/zremrangebyrank
        /// </summary>
        public async Task<long> RemoveRange(long start, long stop, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                return await Command.RemoveRange(Settings.Db, Key, start, stop, queueJump).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// ZREMRANGEBYSCORE http://redis.io/commands/zremrangebyscore
        /// </summary>
        public async Task<long> RemoveRange(double min, double max, bool minInclusive = true, bool maxInclusive = true, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                return await Command.RemoveRange(Settings.Db, Key, min, max, minInclusive, maxInclusive, queueJump).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// ZSCORE http://redis.io/commands/zscore
        /// </summary>
        public async Task<double?> Score(T member, bool queueJump = false)
        {
            using (Monitor.Start(Settings.PerformanceMonitor, Key, CallType))
            {
                return await Command.Score(Settings.Db, Key, Settings.ValueConverter.Serialize(member), queueJump).ConfigureAwait(false);
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