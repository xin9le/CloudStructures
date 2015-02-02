using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CloudStructures
{
    public abstract class RedisStructure
    {
        public RedisKey Key { get; private set; }
        public RedisSettings Settings { get; private set; }
        protected abstract string CallType { get; }

        internal RedisStructure(RedisSettings settings, RedisKey key)
        {
            this.Settings = settings;
            this.Key = key;
        }

        internal RedisStructure(RedisGroup connectionGroup, RedisKey key)
            : this(connectionGroup.GetSettings(key), key)
        {
        }

        internal ConnectionMultiplexer Connection
        {
            get
            {
                return Settings.GetConnection();
            }
        }

        internal IDatabaseAsync Command
        {
            get
            {
                return Connection.GetDatabase(Settings.Db);
            }
        }

        internal ITransaction CreateTransaction()
        {
            var command = Command;
            return ((IDatabase)command).CreateTransaction();
        }

        /// <summary>
        /// SETEX, PSETEX http://redis.io/commands/setex http://redis.io/commands/psetex
        /// </summary>
        public Task<bool> SetExpire(DateTime expiry, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var r = await Command.KeyExpireAsync(Key, expiry, commandFlags).ForAwait();
                return Tracing.CreateSentAndReceived(new { expiry }, 8, r, sizeof(bool));
            });
        }

        /// <summary>
        /// SETEX, PSETEX http://redis.io/commands/setex http://redis.io/commands/psetex
        /// </summary>
        public Task<bool> SetExpire(TimeSpan expiry, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var r = await Command.KeyExpireAsync(Key, expiry, commandFlags).ForAwait();
                return Tracing.CreateSentAndReceived(new { expiry }, 8, r, sizeof(bool));
            });
        }

        /// <summary>
        /// EXISTS http://redis.io/commands/exists
        /// </summary>
        public Task<bool> KeyExists(CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordReceive(Settings, Key, CallType, async () =>
            {
                var r = await Command.KeyExistsAsync(Key, commandFlags).ForAwait();
                return Tracing.CreateReceived(r, sizeof(bool));
            });
        }

        /// <summary>
        /// DEL http://redis.io/commands/del
        /// </summary>
        public Task<bool> Delete(CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordReceive(Settings, Key, CallType, async () =>
            {
                var v = await Command.KeyDeleteAsync(Key, commandFlags).ForAwait();
                return Tracing.CreateReceived(v, sizeof(bool));
            });
        }

        /// <summary>
        /// TTL http://redis.io/commands/ttl
        /// </summary>
        public Task<TimeSpan?> TimeToLive(CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordReceive(Settings, Key, CallType, async () =>
            {
                var v = await Command.KeyTimeToLiveAsync(Key, commandFlags).ForAwait();
                return Tracing.CreateReceived(v, 8);
            });
        }
    }
}