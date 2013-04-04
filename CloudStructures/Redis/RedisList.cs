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
        public int Db { get; private set; }
        readonly RedisSettings settings;
        readonly RedisTransaction transaction;
        readonly IRedisValueConverter valueConverter;

        public RedisList(RedisSettings settings, string listKey)
        {
            this.settings = settings;
            this.Db = settings.Db;
            this.valueConverter = settings.ValueConverter;
            this.Key = listKey;
        }

        public RedisList(RedisGroup connectionGroup, string listKey)
            : this(connectionGroup.GetSettings(listKey), listKey)
        {
        }

        public RedisList(RedisTransaction transaction, int db, IRedisValueConverter valueConverter, string listKey)
        {
            this.transaction = transaction;
            this.Db = db;
            this.valueConverter = valueConverter;
            this.Key = listKey;
        }

        protected RedisConnection Connection
        {
            get
            {
                return (transaction == null) ? settings.GetConnection() : transaction;
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
        public virtual Task<long> AddFirst(T value, bool queueJump = false)
        {
            var v = valueConverter.Serialize(value);
            return Command.AddFirst(Db, Key, v, createIfMissing: true, queueJump: queueJump);
        }

        /// <summary>
        /// RPUSH http://redis.io/commands/rpush
        /// </summary>
        public virtual Task<long> AddLast(T value, bool queueJump = false)
        {
            var v = valueConverter.Serialize(value);
            return Command.AddLast(Db, Key, v, createIfMissing: true, queueJump: queueJump);
        }

        /// <summary>
        /// LINDEX http://redis.io/commands/lindex
        /// </summary>
        public virtual async Task<Tuple<bool, T>> TryGet(int index, bool queueJump = false)
        {
            var value = await Command.Get(Db, Key, index, queueJump).ConfigureAwait(false);
            return (value == null)
                ? Tuple.Create(false, default(T))
                : Tuple.Create(true, valueConverter.Deserialize<T>(value));
        }

        /// <summary>
        /// LLEN http://redis.io/commands/llen
        /// </summary>
        public virtual Task<long> GetLength(bool queueJump = false)
        {
            return Command.GetLength(Db, Key, queueJump);
        }

        /// <summary>
        /// LRANGE http://redis.io/commands/lrange
        /// </summary>
        public virtual async Task<T[]> Range(int start, int stop, bool queueJump = false)
        {
            var results = await Command.Range(Db, Key, start, stop, queueJump).ConfigureAwait(false);
            return results.Select(valueConverter.Deserialize<T>).ToArray();
        }

        /// <summary>
        /// LREM http://redis.io/commands/lrem
        /// </summary>
        public virtual Task<long> Remove(T value, int count = 1, bool queueJump = false)
        {
            var v = valueConverter.Serialize(value);
            return Command.Remove(Db, Key, v, count, queueJump);
        }

        /// <summary>
        /// LPOP http://redis.io/commands/lpop
        /// </summary>
        public virtual async Task<T> RemoveFirst(bool queueJump = false)
        {
            var result = await Command.RemoveFirst(Db, Key, queueJump).ConfigureAwait(false);
            return valueConverter.Deserialize<T>(result);
        }

        /// <summary>
        /// RPOP http://redis.io/commands/rpop
        /// </summary>
        public virtual async Task<T> RemoveLast(bool queueJump = false)
        {
            var result = await Command.RemoveLast(Db, Key, queueJump).ConfigureAwait(false);
            return valueConverter.Deserialize<T>(result);
        }

        /// <summary>
        /// LSET http://redis.io/commands/lset
        /// </summary>
        public virtual Task Set(int index, T value, bool queueJump = false)
        {
            var v = valueConverter.Serialize(value);
            return Command.Set(Db, Key, index, v, queueJump);
        }

        /// <summary>
        /// LTRIM http://redis.io/commands/ltrim
        /// </summary>
        public virtual Task Trim(int count, bool queueJump = false)
        {
            return Command.Trim(Db, Key, count, queueJump);
        }

        /// <summary>
        /// LTRIM http://redis.io/commands/ltrim
        /// </summary>
        public virtual Task Trim(int start, int stop, bool queueJump = false)
        {
            return Command.Trim(Db, Key, start, stop, queueJump);
        }

        // additional commands

        public virtual async Task<long> AddFirstAndFixLength(T value, int fixLength, bool queueJump = false)
        {
            if (settings == null) throw new InvalidOperationException("AddFirstAndFixeLength does not supports passed by IListCommands");

            var v = valueConverter.Serialize(value);
            using (var tx = settings.GetConnection().CreateTransaction())
            {
                var addResult = tx.Lists.AddFirst(Db, Key, v, createIfMissing: true, queueJump: queueJump);
                var trimResult = tx.Lists.Trim(Db, Key, fixLength - 1, queueJump);

                await tx.Execute(queueJump).ConfigureAwait(false);
                return await addResult.ConfigureAwait(false);
            }
        }

        public virtual Task<bool> SetExpire(int seconds, bool queueJump = false)
        {
            return Connection.Keys.Expire(Db, Key, seconds, queueJump);
        }

        public virtual Task<bool> Clear(bool queueJump = false)
        {
            return Connection.Keys.Remove(Db, Key, queueJump);
        }

        public async virtual Task<T[]> ToArray(bool queueJump = false)
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