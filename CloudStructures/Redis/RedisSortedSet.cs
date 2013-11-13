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
        public Task<bool> Add(T value, double score, bool queueJump = false)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var r = await Command.Add(Settings.Db, Key, Settings.ValueConverter.Serialize(value), score, queueJump).ConfigureAwait(false);
                return Pair.Create(new { value, score }, r);
            });
        }

        /// <summary>
        /// ZCARD http://redis.io/commands/zcard
        /// </summary>
        public Task<long> GetLength(bool queueJump = false)
        {
            return TraceHelper.RecordReceive(Settings, Key, CallType, () =>
            {
                return Command.GetLength(Settings.Db, Key, queueJump);
            });
        }

        /// <summary>
        /// ZCOUNT http://redis.io/commands/zcount
        /// </summary>
        public Task<long> GetLength(double min, double max, bool queueJump = false)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var r = await Command.GetLength(Settings.Db, Key, min, max, queueJump).ConfigureAwait(false);
                return Pair.Create(new { min, max }, r);
            });
        }

        /// <summary>
        /// ZINCRBY http://redis.io/commands/zincrby
        /// </summary>
        public Task<double> Increment(T member, double delta, bool queueJump = false)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var r = await Command.Increment(Settings.Db, Key, Settings.ValueConverter.Serialize(member), delta, queueJump).ConfigureAwait(false);
                return Pair.Create(new { member, delta }, r);
            });
        }

        /// <summary>
        /// ZINCRBY http://redis.io/commands/zincrby
        /// </summary>
        public Task<double[]> Increment(T[] members, double delta, bool queueJump = false)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var v = members.Select(x => Settings.ValueConverter.Serialize(x)).ToArray();
                var r = await Task.WhenAll(Command.Increment(Settings.Db, Key, v, delta, queueJump)).ConfigureAwait(false);

                return Pair.Create(new { members, delta }, r);
            });
        }

        /// <summary>
        /// ZRANGE http://redis.io/commands/zrange
        /// </summary>
        public Task<KeyValuePair<T, double>[]> Range(long start, long stop, bool ascending = true, bool queueJump = false)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var v = await Command.Range(Settings.Db, Key, start, stop, ascending, queueJump).ConfigureAwait(false);
                var r = v.Select(x => new KeyValuePair<T, double>(Settings.ValueConverter.Deserialize<T>(x.Key), x.Value)).ToArray();
                return Pair.Create(new { start, stop, ascending }, r);
            });
        }

        /// <summary>
        /// ZRANGEBYSCORE http://redis.io/commands/zrangebyscore
        /// </summary>
        public Task<KeyValuePair<T, double>[]> Range(double min = -1.0 / 0.0, double max = 1.0 / 0.0, bool ascending = true, bool minInclusive = true, bool maxInclusive = true, long offset = 0, long count = 9223372036854775807, bool queueJump = false)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var v = await Command.Range(Settings.Db, Key, min, max, ascending, minInclusive, maxInclusive, offset, count, queueJump).ConfigureAwait(false);
                var r = v.Select(x => new KeyValuePair<T, double>(Settings.ValueConverter.Deserialize<T>(x.Key), x.Value)).ToArray();
                return Pair.Create(new { min, max, ascending, minInclusive, maxInclusive, offset, count }, r);
            });
        }

        /// <summary>
        /// ZRANK http://redis.io/commands/zrank
        /// </summary>
        public Task<long?> Rank(T member, bool ascending = true, bool queueJump = false)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var r = await Command.Rank(Settings.Db, Key, Settings.ValueConverter.Serialize(member), ascending, queueJump).ConfigureAwait(false);
                return Pair.Create(new { member, ascending }, r);
            });
        }

        /// <summary>
        /// ZREM http://redis.io/commands/zrem
        /// </summary>
        public Task<bool> Remove(T member, bool queueJump = false)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var r = await Command.Remove(Settings.Db, Key, Settings.ValueConverter.Serialize(member), queueJump).ConfigureAwait(false);
                return Pair.Create(new { member }, r);
            });
        }

        /// <summary>
        /// ZREM http://redis.io/commands/zrem
        /// </summary>
        public Task<long> Remove(T[] members, bool queueJump = false)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var v = members.Select(x => Settings.ValueConverter.Serialize(x)).ToArray();
                var r = await Command.Remove(Settings.Db, Key, v, queueJump).ConfigureAwait(false);
                return Pair.Create(new { members }, r);
            });
        }

        /// <summary>
        /// ZREMRANGEBYRANK http://redis.io/commands/zremrangebyrank
        /// </summary>
        public Task<long> RemoveRange(long start, long stop, bool queueJump = false)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var r = await Command.RemoveRange(Settings.Db, Key, start, stop, queueJump).ConfigureAwait(false);
                return Pair.Create(new { start, stop }, r);
            });
        }

        /// <summary>
        /// ZREMRANGEBYSCORE http://redis.io/commands/zremrangebyscore
        /// </summary>
        public Task<long> RemoveRange(double min, double max, bool minInclusive = true, bool maxInclusive = true, bool queueJump = false)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var r = await Command.RemoveRange(Settings.Db, Key, min, max, minInclusive, maxInclusive, queueJump).ConfigureAwait(false);
                return Pair.Create(new { min, max, minInclusive, maxInclusive }, r);
            });
        }

        /// <summary>
        /// ZSCORE http://redis.io/commands/zscore
        /// </summary>
        public Task<double?> Score(T member, bool queueJump = false)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var r = await Command.Score(Settings.Db, Key, Settings.ValueConverter.Serialize(member), queueJump).ConfigureAwait(false);
                return Pair.Create(new { member }, r);
            });
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

        public Task<bool> SetExpire(int seconds, bool queueJump = false)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var r = await Connection.Keys.Expire(Settings.Db, Key, seconds, queueJump).ConfigureAwait(false);
                return Pair.Create(new { seconds }, r);
            });
        }

        public Task<bool> KeyExists(bool queueJump = false)
        {
            return TraceHelper.RecordReceive(Settings, Key, CallType, () =>
            {
                return Connection.Keys.Exists(Settings.Db, Key, queueJump);
            });
        }

        public Task<bool> Clear(bool queueJump = false)
        {
            return TraceHelper.RecordReceive(Settings, Key, CallType, () =>
            {
                return Connection.Keys.Remove(Settings.Db, Key, queueJump);
            });
        }
    }
}