using BookSleeve;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace CloudStructures.Redis
{
    public class RedisSortedSet<T>
    {
        public string Key { get; private set; }
        public int Db { get; private set; }
        readonly RedisSettings settings;
        readonly RedisTransaction transaction;
        readonly IRedisValueConverter valueConverter;

        public RedisSortedSet(RedisSettings settings, string stringKey)
        {
            this.settings = settings;
            this.Db = settings.Db;
            this.valueConverter = settings.ValueConverter;
            this.Key = stringKey;
        }

        public RedisSortedSet(RedisGroup connectionGroup, string stringKey)
            : this(connectionGroup.GetSettings(stringKey), stringKey)
        {
        }

        public RedisSortedSet(RedisTransaction transaction, int db, IRedisValueConverter valueConverter, string stringKey)
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
        public Task<bool> Add(T value, double score, bool queueJump = false)
        {
            return Command.Add(Db, Key, valueConverter.Serialize(value), score, queueJump);
        }

        /// <summary>
        /// ZCARD http://redis.io/commands/zcard
        /// </summary>
        public Task<long> GetLength(bool queueJump = false)
        {
            return Command.GetLength(Db, Key, queueJump);
        }

        /// <summary>
        /// ZCOUNT http://redis.io/commands/zcount
        /// </summary>
        public Task<long> GetLength(double min, double max, bool queueJump = false)
        {
            return Command.GetLength(Db, Key, min, max, queueJump);
        }

        /// <summary>
        /// ZINCRBY http://redis.io/commands/zincrby
        /// </summary>
        public Task<double> Increment(T member, double delta, bool queueJump = false)
        {
            return Command.Increment(Db, Key, valueConverter.Serialize(member), delta, queueJump);
        }

        /// <summary>
        /// ZINCRBY http://redis.io/commands/zincrby
        /// </summary>
        public Task<double>[] Increment(T[] members, double delta, bool queueJump = false)
        {
            var v = members.Select(x => valueConverter.Serialize(x)).ToArray();
            return Command.Increment(Db, Key, v, delta, queueJump);
        }

        /// <summary>
        /// ZRANGE http://redis.io/commands/zrange
        /// </summary>
        public async Task<KeyValuePair<T, double>[]> Range(long start, long stop, bool ascending = true, bool queueJump = false)
        {
            var v = await Command.Range(Db, Key, start, stop, ascending, queueJump).ConfigureAwait(false);
            return v.Select(x => new KeyValuePair<T, double>(valueConverter.Deserialize<T>(x.Key), x.Value)).ToArray();
        }

        /// <summary>
        /// ZRANGEBYSCORE http://redis.io/commands/zrangebyscore
        /// </summary>
        public async Task<KeyValuePair<T, double>[]> Range(double min = -1.0 / 0.0, double max = 1.0 / 0.0, bool ascending = true, bool minInclusive = true, bool maxInclusive = true, long offset = 0, long count = 9223372036854775807, bool queueJump = false)
        {
            var v = await Command.Range(Db, Key, min, max, ascending, minInclusive, maxInclusive, offset, count, queueJump).ConfigureAwait(false);
            return v.Select(x => new KeyValuePair<T, double>(valueConverter.Deserialize<T>(x.Key), x.Value)).ToArray();
        }

        /// <summary>
        /// ZRANK http://redis.io/commands/zrank
        /// </summary>
        public Task<long?> Rank(T member, bool ascending = true, bool queueJump = false)
        {
            return Command.Rank(Db, Key, valueConverter.Serialize(member), ascending, queueJump);
        }

        /// <summary>
        /// ZREM http://redis.io/commands/zrem
        /// </summary>
        public Task<bool> Remove(T member, bool queueJump = false)
        {
            return Command.Remove(Db, Key, valueConverter.Serialize(member), queueJump);
        }

        /// <summary>
        /// ZREM http://redis.io/commands/zrem
        /// </summary>
        public Task<long> Remove(T[] members, bool queueJump = false)
        {
            var v = members.Select(x => valueConverter.Serialize(x)).ToArray();
            return Command.Remove(Db, Key, v, queueJump);
        }

        /// <summary>
        /// ZREMRANGEBYRANK http://redis.io/commands/zremrangebyrank
        /// </summary>
        public Task<long> RemoveRange(long start, long stop, bool queueJump = false)
        {
            return Command.RemoveRange(Db, Key, start, stop, queueJump);
        }

        /// <summary>
        /// ZREMRANGEBYSCORE http://redis.io/commands/zremrangebyscore
        /// </summary>
        public Task<long> RemoveRange(double min, double max, bool minInclusive = true, bool maxInclusive = true, bool queueJump = false)
        {
            return Command.RemoveRange(Db, Key, min, max, minInclusive, maxInclusive, queueJump);
        }

        /// <summary>
        /// ZSCORE http://redis.io/commands/zscore
        /// </summary>
        public Task<double?> Score(T member, bool queueJump = false)
        {
            return Command.Score(Db, Key, valueConverter.Serialize(member), queueJump);
        }

        public Task<bool> SetExpire(TimeSpan expire, bool queueJump = false)
        {
            return SetExpire((int)expire.TotalSeconds, queueJump);
        }

        public Task<bool> SetExpire(int seconds, bool queueJump = false)
        {
            return Connection.Keys.Expire(Db, Key, seconds, queueJump);
        }

        public Task<bool> Clear(bool queueJump = false)
        {
            return Connection.Keys.Remove(Db, Key, queueJump);
        }
    }
}