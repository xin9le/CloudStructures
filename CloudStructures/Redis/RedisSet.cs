using BookSleeve;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace CloudStructures.Redis
{
    public class RedisSet<T>
    {
        const string CallType = "RedisSet";

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
        public Task<bool> Add(T value, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var r = await Command.Add(Settings.Db, Key, Settings.ValueConverter.Serialize(value), commandFlags).ConfigureAwait(false);
                return Pair.Create(new { value }, r);
            });
        }

        /// <summary>
        /// SADD http://redis.io/commands/sadd
        /// </summary>
        public Task<long> Add(T[] values, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var v = values.Select(x => Settings.ValueConverter.Serialize(x)).ToArray();
                var r = await Command.Add(Settings.Db, Key, v, commandFlags).ConfigureAwait(false);
                return Pair.Create(new { values }, r);
            });
        }

        /// <summary>
        /// SISMEMBER http://redis.io/commands/sismember
        /// </summary>
        public Task<bool> Contains(T value, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var r = await Command.Contains(Settings.Db, Key, Settings.ValueConverter.Serialize(value), commandFlags).ConfigureAwait(false);
                return Pair.Create(new { value }, r);
            });
        }


        /// <summary>
        /// SMEMBERS http://redis.io/commands/smembers
        /// </summary>
        public Task<T[]> GetAll(CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordReceive(Settings, Key, CallType, async () =>
            {
                var v = await Command.GetAll(Settings.Db, Key, commandFlags).ConfigureAwait(false);
                var r = v.Select(Settings.ValueConverter.Deserialize<T>).ToArray();
                return r;
            });
        }

        /// <summary>
        /// SCARD http://redis.io/commands/scard
        /// </summary>
        public Task<long> GetLength(CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordReceive(Settings, Key, CallType,  () =>
            {
                return Command.GetLength(Settings.Db, Key, commandFlags);
            });
        }

        /// <summary>
        /// SRANDMEMBER http://redis.io/commands/srandmember
        /// </summary>
        public Task<T> GetRandom(CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordReceive(Settings, Key, CallType, async () =>
            {
                var v = await Command.GetRandom(Settings.Db, Key, commandFlags).ConfigureAwait(false);
                return Settings.ValueConverter.Deserialize<T>(v);
            });
        }

        /// <summary>
        /// SRANDMEMBER http://redis.io/commands/srandmember
        /// </summary>
        public Task<T[]> GetRandom(int count, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var v = await Command.GetRandom(Settings.Db, Key, count, commandFlags).ConfigureAwait(false);
                var r = v.Select(Settings.ValueConverter.Deserialize<T>).ToArray();
                return Pair.Create(new { count }, r);
            });
        }

        /// <summary>
        /// SREM http://redis.io/commands/srem
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
        /// SREM http://redis.io/commands/srem
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
        /// SPOP http://redis.io/commands/spop
        /// </summary>
        public Task<T> RemoveRandom(CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordReceive(Settings, Key, CallType, async () =>
            {
                var v = await Command.RemoveRandom(Settings.Db, Key, commandFlags).ConfigureAwait(false);
                return Settings.ValueConverter.Deserialize<T>(v);
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
            return TraceHelper.RecordReceive(Settings, Key, CallType,  () =>
            {
                return  Connection.Keys.Exists(Settings.Db, Key, commandFlags);
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