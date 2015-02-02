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

        public Task<bool> SetExpire(DateTime expiry, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var r = await Command.KeyExpireAsync(Key, expiry, commandFlags).ConfigureAwait(false);
                return Pair.CreatePair(new { expiry }, 8, r, sizeof(bool));
            });
        }

        public Task<bool> SetExpire(TimeSpan expiry, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var r = await Command.KeyExpireAsync(Key, expiry, commandFlags).ConfigureAwait(false);
                return Pair.CreatePair(new { expiry }, 8, r, sizeof(bool));
            });
        }

        public Task<bool> KeyExists(CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordReceive(Settings, Key, CallType, async () =>
            {
                var r = await Command.KeyExistsAsync(Key, commandFlags).ConfigureAwait(false);
                return Pair.CreateReceived(r, sizeof(bool));
            });
        }

        public Task<bool> Delete(CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordReceive(Settings, Key, CallType, async () =>
            {
                var v = await Command.KeyDeleteAsync(Key, commandFlags).ConfigureAwait(false);
                return Pair.CreateReceived(v, sizeof(bool));
            });
        }

        public Task<TimeSpan?> TimeToLive(CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordReceive(Settings, Key, CallType, async () =>
            {
                var v = await Command.KeyTimeToLiveAsync(Key, commandFlags).ConfigureAwait(false);
                return Pair.CreateReceived(v, 8);
            });
        }
    }
}