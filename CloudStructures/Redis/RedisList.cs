using BookSleeve;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CloudStructures.Redis
{
    public class RedisList<T> : IObservable<T>
    {
        public string Key { get; private set; }
        readonly RedisSettings settings;
        readonly int enumerateThreshold;

        public RedisList(RedisSettings settings, string listKey, int enumerateThreshold = 100)
        {
            this.settings = settings;
            this.Key = listKey;
            this.enumerateThreshold = enumerateThreshold;
        }

        public RedisList(RedisGroup connectionGroup, string listKey, int enumerateThreshold = 100)
            : this(connectionGroup.GetSettings(listKey), listKey, enumerateThreshold)
        {
        }

        protected RedisConnection Connection
        {
            get
            {
                return settings.GetConnection();
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
            var v = settings.ValueConverter.Serialize(value);
            return Command.AddFirst(settings.Db, Key, v, createIfMissing: true, queueJump: queueJump);
        }

        /// <summary>
        /// RPUSH http://redis.io/commands/rpush
        /// </summary>
        public virtual Task<long> AddLast(T value, bool queueJump = false)
        {
            var v = settings.ValueConverter.Serialize(value);
            return Command.AddLast(settings.Db, Key, v, createIfMissing: true, queueJump: queueJump);
        }

        /// <summary>
        /// LINDEX http://redis.io/commands/lindex
        /// </summary>
        public virtual async Task<Tuple<bool, T>> TryGet(int index, bool queueJump = false)
        {
            var value = await Command.Get(settings.Db, Key, index, queueJump);
            return (value == null)
                ? Tuple.Create(false, default(T))
                : Tuple.Create(true, settings.ValueConverter.Deserialize<T>(value));
        }

        /// <summary>
        /// LLEN http://redis.io/commands/llen
        /// </summary>
        public virtual Task<long> GetLength(bool queueJump = false)
        {
            return Command.GetLength(settings.Db, Key, queueJump);
        }

        /// <summary>
        /// LRANGE http://redis.io/commands/lrange
        /// </summary>
        public virtual async Task<T[]> Range(int start, int stop, bool queueJump = false)
        {
            var results = await Command.Range(settings.Db, Key, start, stop, queueJump);
            return results.Select(settings.ValueConverter.Deserialize<T>).ToArray();
        }

        /// <summary>
        /// LREM http://redis.io/commands/lrem
        /// </summary>
        public virtual Task<long> Remove(T value, int count = 1, bool queueJump = false)
        {
            var v = settings.ValueConverter.Serialize(value);
            return Command.Remove(settings.Db, Key, v, count, queueJump);
        }

        /// <summary>
        /// LPOP http://redis.io/commands/lpop
        /// </summary>
        public virtual async Task<T> RemoveFirst(bool queueJump = false)
        {
            var result = await Command.RemoveFirst(settings.Db, Key, queueJump);
            return settings.ValueConverter.Deserialize<T>(result);
        }

        /// <summary>
        /// RPOP http://redis.io/commands/rpop
        /// </summary>
        public virtual async Task<T> RemoveLast(bool queueJump = false)
        {
            var result = await Command.RemoveLast(settings.Db, Key, queueJump);
            return settings.ValueConverter.Deserialize<T>(result);
        }

        /// <summary>
        /// LSET http://redis.io/commands/lset
        /// </summary>
        public virtual Task Set(int index, T value, bool queueJump = false)
        {
            var v = settings.ValueConverter.Serialize(value);
            return Command.Set(settings.Db, Key, index, v, queueJump);
        }

        /// <summary>
        /// LTRIM http://redis.io/commands/ltrim
        /// </summary>
        public virtual Task Trim(int count, bool queueJump = false)
        {
            return Command.Trim(settings.Db, Key, count, queueJump);
        }

        /// <summary>
        /// LTRIM http://redis.io/commands/ltrim
        /// </summary>
        public virtual Task Trim(int start, int stop, bool queueJump = false)
        {
            return Command.Trim(settings.Db, Key, start, stop, queueJump);
        }

        // additional commands

        public virtual async Task<long> AddFirstAndFixLength(T value, int fixLength, bool queueJump = false)
        {
            var v = settings.ValueConverter.Serialize(value);
            using (var tx = Connection.CreateTransaction())
            {
                var addResult = tx.Lists.AddFirst(settings.Db, Key, v, createIfMissing: true, queueJump: queueJump);
                var trimResult = tx.Lists.Trim(settings.Db, Key, fixLength - 1, queueJump);

                await tx.Execute(queueJump);
                return await addResult;
            }
        }

        public virtual Task<bool> SetExpire(int seconds, bool queueJump = false)
        {
            return Connection.Keys.Expire(settings.Db, Key, seconds, queueJump);
        }

        public virtual Task<bool> Clear(bool queueJump = false)
        {
            return Connection.Keys.Remove(settings.Db, Key, queueJump);
        }

        public virtual IDisposable Subscribe(IObserver<T> observer)
        {
            var observable = Observable.Create<T>(async (o, ct) =>
            {
                try
                {
                    var length = await GetLength();
                    var start = 0;
                    var stop = enumerateThreshold;

                    while (true)
                    {
                        if (ct.IsCancellationRequested) break;

                        var values = await Range(start, stop);
                        if (values.Length == 0) break;
                        foreach (var item in values)
                        {
                            o.OnNext(item);
                        }
                        if (start + values.Length == length) break;
                        start = stop + 1;
                        stop += stop;
                    }
                }
                catch (Exception ex)
                {
                    o.OnError(ex);
                    return;
                }
                if (!ct.IsCancellationRequested)
                {
                    o.OnCompleted();
                }
            });

            return observable.Subscribe(observer);
        }
    }

    public static class RedisListExtensions
    {
        /// <summary>
        /// LINDEX http://redis.io/commands/lindex
        /// </summary>
        public static async Task<T> GetOrDefault<T>(this RedisList<T> redis, int index, T defaultValue = default(T), bool queueJump = false)
        {
            var result = await redis.TryGet(index, queueJump);
            return result.Item1 ? result.Item2 : defaultValue;
        }
    }
}