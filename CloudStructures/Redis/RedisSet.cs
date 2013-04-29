using BookSleeve;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace CloudStructures.Redis
{
    public class RedisSet<T>
    {
        public string Key { get; private set; }
        public RedisSettings Settings { get; private set; }

        public RedisSet(RedisSettings settings, string stringKey)
        {
            this.Settings = settings;
            this.Key = stringKey;
        }

        public RedisSet(RedisGroup connectionGroup, string stringKey)
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

        protected ISetCommands Command
        {
            get
            {
                return Connection.Sets;
            }
        }

        /// <summary>
        /// SADD http://redis.io/commands/sadd
        /// </summary>
        public Task<bool> Add(T value, bool queueJump = false)
        {
            return Command.Add(Settings.Db, Key, Settings.ValueConverter.Serialize(value), queueJump);
        }

        /// <summary>
        /// SADD http://redis.io/commands/sadd
        /// </summary>
        public Task<long> Add(T[] values, bool queueJump = false)
        {
            var v = values.Select(x => Settings.ValueConverter.Serialize(x)).ToArray();
            return Command.Add(Settings.Db, Key, v, queueJump);
        }

        /// <summary>
        /// SISMEMBER http://redis.io/commands/sismember
        /// </summary>
        public Task<bool> Contains(T value, bool queueJump = false)
        {
            return Command.Contains(Settings.Db, Key, Settings.ValueConverter.Serialize(value), queueJump);
        }


        /// <summary>
        /// SMEMBERS http://redis.io/commands/smembers
        /// </summary>
        public async Task<T[]> GetAll(bool queueJump = false)
        {
            var v = await Command.GetAll(Settings.Db, Key, queueJump).ConfigureAwait(false);
            return v.Select(Settings.ValueConverter.Deserialize<T>).ToArray();
        }

        /// <summary>
        /// SCARD http://redis.io/commands/scard
        /// </summary>
        public Task<long> GetLength(bool queueJump = false)
        {
            return Command.GetLength(Settings.Db, Key, queueJump);
        }

        /// <summary>
        /// SRANDMEMBER http://redis.io/commands/srandmember
        /// </summary>
        public async Task<T> GetRandom(bool queueJump = false)
        {
            var v = await Command.GetRandom(Settings.Db, Key, queueJump).ConfigureAwait(false);
            return Settings.ValueConverter.Deserialize<T>(v);
        }

        /// <summary>
        /// SRANDMEMBER http://redis.io/commands/srandmember
        /// </summary>
        public async Task<T[]> GetRandom(int count, bool queueJump = false)
        {
            var v = await Command.GetRandom(Settings.Db, Key, count, queueJump).ConfigureAwait(false);
            return v.Select(Settings.ValueConverter.Deserialize<T>).ToArray();
        }

        /// <summary>
        /// SREM http://redis.io/commands/srem
        /// </summary>
        public Task<bool> Remove(T member, bool queueJump = false)
        {
            return Command.Remove(Settings.Db, Key, Settings.ValueConverter.Serialize(member), queueJump);
        }

        /// <summary>
        /// SREM http://redis.io/commands/srem
        /// </summary>
        public Task<long> Remove(T[] members, bool queueJump = false)
        {
            var v = members.Select(x => Settings.ValueConverter.Serialize(x)).ToArray();
            return Command.Remove(Settings.Db, Key, v, queueJump);
        }

        /// <summary>
        /// SPOP http://redis.io/commands/spop
        /// </summary>
        public async Task<T> RemoveRandom(bool queueJump = false)
        {
            var v = await Command.RemoveRandom(Settings.Db, Key, queueJump).ConfigureAwait(false);
            return Settings.ValueConverter.Deserialize<T>(v);
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
    }
}