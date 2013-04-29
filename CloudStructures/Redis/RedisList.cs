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
            var v = Settings.ValueConverter.Serialize(value);
            return Command.AddFirst(Settings.Db, Key, v, createIfMissing: true, queueJump: queueJump);
        }

        /// <summary>
        /// RPUSH http://redis.io/commands/rpush
        /// </summary>
        public Task<long> AddLast(T value, bool queueJump = false)
        {
            var v = Settings.ValueConverter.Serialize(value);
            return Command.AddLast(Settings.Db, Key, v, createIfMissing: true, queueJump: queueJump);
        }

        /// <summary>
        /// LINDEX http://redis.io/commands/lindex
        /// </summary>
        public async Task<Tuple<bool, T>> TryGet(int index, bool queueJump = false)
        {
            var value = await Command.Get(Settings.Db, Key, index, queueJump).ConfigureAwait(false);
            return (value == null)
                ? Tuple.Create(false, default(T))
                : Tuple.Create(true, Settings.ValueConverter.Deserialize<T>(value));
        }

        /// <summary>
        /// LLEN http://redis.io/commands/llen
        /// </summary>
        public Task<long> GetLength(bool queueJump = false)
        {
            return Command.GetLength(Settings.Db, Key, queueJump);
        }

        /// <summary>
        /// LRANGE http://redis.io/commands/lrange
        /// </summary>
        public async Task<T[]> Range(int start, int stop, bool queueJump = false)
        {
            var results = await Command.Range(Settings.Db, Key, start, stop, queueJump).ConfigureAwait(false);
            return results.Select(Settings.ValueConverter.Deserialize<T>).ToArray();
        }

        /// <summary>
        /// LREM http://redis.io/commands/lrem
        /// </summary>
        public Task<long> Remove(T value, int count = 1, bool queueJump = false)
        {
            var v = Settings.ValueConverter.Serialize(value);
            return Command.Remove(Settings.Db, Key, v, count, queueJump);
        }

        /// <summary>
        /// LPOP http://redis.io/commands/lpop
        /// </summary>
        public async Task<T> RemoveFirst(bool queueJump = false)
        {
            var result = await Command.RemoveFirst(Settings.Db, Key, queueJump).ConfigureAwait(false);
            return Settings.ValueConverter.Deserialize<T>(result);
        }

        /// <summary>
        /// RPOP http://redis.io/commands/rpop
        /// </summary>
        public async Task<T> RemoveLast(bool queueJump = false)
        {
            var result = await Command.RemoveLast(Settings.Db, Key, queueJump).ConfigureAwait(false);
            return Settings.ValueConverter.Deserialize<T>(result);
        }

        /// <summary>
        /// LSET http://redis.io/commands/lset
        /// </summary>
        public Task Set(int index, T value, bool queueJump = false)
        {
            var v = Settings.ValueConverter.Serialize(value);
            return Command.Set(Settings.Db, Key, index, v, queueJump);
        }

        /// <summary>
        /// LTRIM http://redis.io/commands/ltrim
        /// </summary>
        public Task Trim(int count, bool queueJump = false)
        {
            return Command.Trim(Settings.Db, Key, count, queueJump);
        }

        /// <summary>
        /// LTRIM http://redis.io/commands/ltrim
        /// </summary>
        public Task Trim(int start, int stop, bool queueJump = false)
        {
            return Command.Trim(Settings.Db, Key, start, stop, queueJump);
        }

        // additional commands

        public async Task<long> AddFirstAndFixLength(T value, int fixLength, bool queueJump = false)
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

        public async Task<T[]> ToArray(bool queueJump = false)
        {
            var length = await GetLength().ConfigureAwait(false);
            return await Range(0, (int)length, queueJump).ConfigureAwait(false);
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