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
        public Task<long> AddFirst(T value, bool queueJump = false)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var v = Settings.ValueConverter.Serialize(value);
                var vr = await Command.AddFirst(Settings.Db, Key, v, createIfMissing: true, queueJump: queueJump).ConfigureAwait(false);
                return Pair.Create(new { value }, vr);
            });
        }

        /// <summary>
        /// RPUSH http://redis.io/commands/rpush
        /// </summary>
        public Task<long> AddLast(T value, bool queueJump = false)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var v = Settings.ValueConverter.Serialize(value);
                var vr = await Command.AddLast(Settings.Db, Key, v, createIfMissing: true, queueJump: queueJump).ConfigureAwait(false);
                return Pair.Create(new { value }, vr);
            });
        }

        /// <summary>
        /// LINDEX http://redis.io/commands/lindex
        /// </summary>
        public Task<Tuple<bool, T>> TryGet(int index, bool queueJump = false)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var value = await Command.Get(Settings.Db, Key, index, queueJump).ConfigureAwait(false);
                var result = (value == null)
                    ? Tuple.Create(false, default(T))
                    : Tuple.Create(true, Settings.ValueConverter.Deserialize<T>(value));
                return Pair.Create(new { index }, result);
            });
        }

        public async Task<T> GetOrDefault(int index, T defaultValue = default(T), bool queueJump = false)
        {
            var result = await TryGet(index, queueJump).ConfigureAwait(false);
            return result.Item1 ? result.Item2 : defaultValue;
        }

        /// <summary>
        /// LLEN http://redis.io/commands/llen
        /// </summary>
        public Task<long> GetLength(bool queueJump = false)
        {
            return TraceHelper.RecordReceive(Settings, Key, CallType, () =>
            {
                return Command.GetLength(Settings.Db, Key, queueJump);
            });
        }

        /// <summary>
        /// LRANGE http://redis.io/commands/lrange
        /// </summary>
        public Task<T[]> Range(int start, int stop, bool queueJump = false)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var results = await Command.Range(Settings.Db, Key, start, stop, queueJump).ConfigureAwait(false);
                var resultArray = results.Select(Settings.ValueConverter.Deserialize<T>).ToArray();
                return Pair.Create(new { start, stop }, resultArray);
            });
        }

        /// <summary>
        /// LREM http://redis.io/commands/lrem
        /// </summary>
        public Task<long> Remove(T value, int count = 1, bool queueJump = false)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var v = Settings.ValueConverter.Serialize(value);
                var r = await Command.Remove(Settings.Db, Key, v, count, queueJump).ConfigureAwait(false);

                return Pair.Create(new { value, count }, r);
            });
        }

        /// <summary>
        /// LPOP http://redis.io/commands/lpop
        /// </summary>
        public Task<T> RemoveFirst(bool queueJump = false)
        {
            return TraceHelper.RecordReceive(Settings, Key, CallType, async () =>
            {
                var result = await Command.RemoveFirst(Settings.Db, Key, queueJump).ConfigureAwait(false);
                return Settings.ValueConverter.Deserialize<T>(result);
            });
        }

        /// <summary>
        /// RPOP http://redis.io/commands/rpop
        /// </summary>
        public Task<T> RemoveLast(bool queueJump = false)
        {
            return TraceHelper.RecordReceive(Settings, Key, CallType, async () =>
            {
                var result = await Command.RemoveLast(Settings.Db, Key, queueJump).ConfigureAwait(false);
                return Settings.ValueConverter.Deserialize<T>(result);
            });
        }

        /// <summary>
        /// LSET http://redis.io/commands/lset
        /// </summary>
        public Task Set(int index, T value, bool queueJump = false)
        {
            return TraceHelper.RecordSend(Settings, Key, CallType, async () =>
            {
                var v = Settings.ValueConverter.Serialize(value);
                await Command.Set(Settings.Db, Key, index, v, queueJump).ConfigureAwait(false);
                return new { index, value };
            });
        }

        /// <summary>
        /// LTRIM http://redis.io/commands/ltrim
        /// </summary>
        public Task Trim(int count, bool queueJump = false)
        {
            return TraceHelper.RecordSend(Settings, Key, CallType, async () =>
            {
                await Command.Trim(Settings.Db, Key, count, queueJump);
                return new { count };
            });
        }

        /// <summary>
        /// LTRIM http://redis.io/commands/ltrim
        /// </summary>
        public Task Trim(int start, int stop, bool queueJump = false)
        {
            return TraceHelper.RecordSend(Settings, Key, CallType, async () =>
            {
                await Command.Trim(Settings.Db, Key, start, stop, queueJump).ConfigureAwait(false);
                return new { start, stop };
            });
        }

        // additional commands

        public Task<long> AddFirstAndFixLength(T value, int fixLength, bool queueJump = false)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var v = Settings.ValueConverter.Serialize(value);
                using (var tx = Settings.GetConnection().CreateTransaction())
                {
                    var addResult = tx.Lists.AddFirst(Settings.Db, Key, v, createIfMissing: true, queueJump: queueJump);
                    var trimResult = tx.Lists.Trim(Settings.Db, Key, count: fixLength, queueJump: queueJump);

                    await tx.Execute(queueJump).ConfigureAwait(false);
                    var result = await addResult.ConfigureAwait(false);
                    return Pair.Create(new { value, fixLength }, result);
                }
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
                var r = await Connection.Keys.Expire(Settings.Db, Key, seconds, queueJump);
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

        public Task<T[]> ToArray(bool queueJump = false)
        {
            return TraceHelper.RecordReceive(Settings, Key, CallType, () =>
            {
                return Range(0, -1, queueJump);
            });
        }
    }
}