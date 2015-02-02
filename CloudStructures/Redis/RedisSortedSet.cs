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
        public Task<bool> Add(T value, double score, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var r = await Command.Add(Settings.Db, Key, Settings.ValueConverter.Serialize(value), score, commandFlags).ConfigureAwait(false);
                return Pair.Create(new { value, score }, r);
            });
        }

        /// <summary>
        /// ZCARD http://redis.io/commands/zcard
        /// </summary>
        public Task<long> GetLength(CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordReceive(Settings, Key, CallType, () =>
            {
                return Command.GetLength(Settings.Db, Key, commandFlags);
            });
        }

        /// <summary>
        /// ZCOUNT http://redis.io/commands/zcount
        /// </summary>
        public Task<long> GetLength(double min, double max, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var r = await Command.GetLength(Settings.Db, Key, min, max, commandFlags).ConfigureAwait(false);
                return Pair.Create(new { min, max }, r);
            });
        }

        /// <summary>
        /// ZINCRBY http://redis.io/commands/zincrby
        /// </summary>
        public Task<double> Increment(T member, double delta, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var r = await Command.Increment(Settings.Db, Key, Settings.ValueConverter.Serialize(member), delta, commandFlags).ConfigureAwait(false);
                return Pair.Create(new { member, delta }, r);
            });
        }

        /// <summary>
        /// ZINCRBY http://redis.io/commands/zincrby
        /// </summary>
        public Task<double[]> Increment(T[] members, double delta, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var v = members.Select(x => Settings.ValueConverter.Serialize(x)).ToArray();
                var r = await Task.WhenAll(Command.Increment(Settings.Db, Key, v, delta, commandFlags)).ConfigureAwait(false);

                return Pair.Create(new { members, delta }, r);
            });
        }

        /// <summary>
        /// ZRANGE http://redis.io/commands/zrange
        /// </summary>
        public Task<KeyValuePair<T, double>[]> Range(long start, long stop, bool ascending = true, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var v = await Command.Range(Settings.Db, Key, start, stop, ascending, commandFlags).ConfigureAwait(false);
                var r = v.Select(x => new KeyValuePair<T, double>(Settings.ValueConverter.Deserialize<T>(x.Key), x.Value)).ToArray();
                return Pair.Create(new { start, stop, ascending }, r);
            });
        }

        /// <summary>
        /// ZRANGEBYSCORE http://redis.io/commands/zrangebyscore
        /// </summary>
        public Task<KeyValuePair<T, double>[]> Range(double min = -1.0 / 0.0, double max = 1.0 / 0.0, bool ascending = true, bool minInclusive = true, bool maxInclusive = true, long offset = 0, long count = 9223372036854775807, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var v = await Command.Range(Settings.Db, Key, min, max, ascending, minInclusive, maxInclusive, offset, count, commandFlags).ConfigureAwait(false);
                var r = v.Select(x => new KeyValuePair<T, double>(Settings.ValueConverter.Deserialize<T>(x.Key), x.Value)).ToArray();
                return Pair.Create(new { min, max, ascending, minInclusive, maxInclusive, offset, count }, r);
            });
        }

        /// <summary>
        /// ZRANK http://redis.io/commands/zrank
        /// </summary>
        public Task<long?> Rank(T member, bool ascending = true, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var r = await Command.Rank(Settings.Db, Key, Settings.ValueConverter.Serialize(member), ascending, commandFlags).ConfigureAwait(false);
                return Pair.Create(new { member, ascending }, r);
            });
        }

        /// <summary>
        /// ZREM http://redis.io/commands/zrem
        /// </summary>
        public Task<bool> Remove(T member, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var r = await Command.Remove(Settings.Db, Key, Settings.ValueConverter.Serialize(member), commandFlags).ConfigureAwait(false);
                return Pair.Create(new { member }, r);
            });
        }

        /// <summary>
        /// ZREM http://redis.io/commands/zrem
        /// </summary>
        public Task<long> Remove(T[] members, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var v = members.Select(x => Settings.ValueConverter.Serialize(x)).ToArray();
                var r = await Command.Remove(Settings.Db, Key, v, commandFlags).ConfigureAwait(false);
                return Pair.Create(new { members }, r);
            });
        }

        /// <summary>
        /// ZREMRANGEBYRANK http://redis.io/commands/zremrangebyrank
        /// </summary>
        public Task<long> RemoveRange(long start, long stop, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var r = await Command.RemoveRange(Settings.Db, Key, start, stop, commandFlags).ConfigureAwait(false);
                return Pair.Create(new { start, stop }, r);
            });
        }

        /// <summary>
        /// ZREMRANGEBYSCORE http://redis.io/commands/zremrangebyscore
        /// </summary>
        public Task<long> RemoveRange(double min, double max, bool minInclusive = true, bool maxInclusive = true, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var r = await Command.RemoveRange(Settings.Db, Key, min, max, minInclusive, maxInclusive, commandFlags).ConfigureAwait(false);
                return Pair.Create(new { min, max, minInclusive, maxInclusive }, r);
            });
        }

        /// <summary>
        /// ZSCORE http://redis.io/commands/zscore
        /// </summary>
        public Task<double?> Score(T member, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var r = await Command.Score(Settings.Db, Key, Settings.ValueConverter.Serialize(member), commandFlags).ConfigureAwait(false);
                return Pair.Create(new { member }, r);
            });
        }

        /// <summary>
        /// expire subtract Datetime.Now
        /// </summary>
        public Task<bool> SetExpire(DateTime expire, CommandFlags commandFlags = CommandFlags.None)
        {
            return SetExpire(expire - DateTime.Now, commandFlags);
        }

        public Task<bool> SetExpire(TimeSpan expire, CommandFlags commandFlags = CommandFlags.None)
        {
            return SetExpire((int)expire.TotalSeconds, commandFlags);
        }

        public Task<bool> SetExpire(int seconds, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var r = await Connection.Keys.Expire(Settings.Db, Key, seconds, commandFlags).ConfigureAwait(false);
                return Pair.Create(new { seconds }, r);
            });
        }

        public Task<bool> KeyExists(CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordReceive(Settings, Key, CallType, () =>
            {
                return Connection.Keys.Exists(Settings.Db, Key, commandFlags);
            });
        }

        public Task<bool> Clear(CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordReceive(Settings, Key, CallType, () =>
            {
                return Connection.Keys.Remove(Settings.Db, Key, commandFlags);
            });
        }
    }
}