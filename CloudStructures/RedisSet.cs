using StackExchange.Redis;
using System;
using System.Linq;
using System.Threading.Tasks;

// TODO:not yet complete

namespace CloudStructures
{
    public class RedisSet<T> : RedisStructure
    {
        protected override string CallType
        {
            get { return "RedisList"; }
        }

        public RedisSet(RedisSettings settings, string setKey)
            : base(settings, setKey)
        {
        }

        public RedisSet(RedisGroup connectionGroup, string setKey)
            : base(connectionGroup, setKey)
        {
        }

        /// <summary>
        /// SADD http://redis.io/commands/sadd
        /// </summary>
        public Task<bool> Add(T value, RedisExpiry expiry = null, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                long sentSize;
                var v = Settings.ValueConverter.Serialize(value, out sentSize);
                var r = await this.ExecuteWithKeyExpire(x => x.SetAddAsync(Key, v, commandFlags), Key, expiry, commandFlags).ConfigureAwait(false);
                return Tracing.CreateSentAndReceived(new { value, expiry = expiry?.Value }, sentSize, r, sizeof(bool));
            });
        }

        /// <summary>
        /// SADD http://redis.io/commands/sadd
        /// </summary>
        public Task<long> Add(T[] values, RedisExpiry expiry = null, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var sentSize = 0L;
                var redisValues = values.Select(x =>
                {
                    long _size;
                    var rv = Settings.ValueConverter.Serialize(x, out _size);
                    sentSize += _size;
                    return rv;
                }).ToArray();

                var vr = await this.ExecuteWithKeyExpire(x => x.SetAddAsync(Key, redisValues, commandFlags), Key, expiry, commandFlags).ForAwait();
                return Tracing.CreateSentAndReceived(new { values, expiry = expiry?.Value }, sentSize, vr, sizeof(long));
            });
        }

        ///// <summary>
        ///// SISMEMBER http://redis.io/commands/sismember
        ///// </summary>
        //public Task<bool> Contains(T value, CommandFlags commandFlags = CommandFlags.None)
        //{
        //    return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
        //    {
        //        var r = await Command.Contains(Settings.Db, Key, Settings.ValueConverter.Serialize(value), commandFlags).ConfigureAwait(false);
        //        return Pair.Create(new { value }, r);
        //    });
        //}

        /// <summary>
        /// SMEMBERS http://redis.io/commands/smembers
        /// </summary>
        public Task<T[]> Members(CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordReceive(Settings, Key, CallType, async () =>
            {
                var v = await Command.SetMembersAsync(Key, commandFlags).ConfigureAwait(false);
                var size = 0L;
                var r = v.Select(x =>
                {
                    long s;
                    var dr = Settings.ValueConverter.Deserialize<T>(x, out s);
                    size += s;
                    return dr;
                }).ToArray();
                return Tracing.CreateReceived(r, size);
            });
        }

        ///// <summary>
        ///// SCARD http://redis.io/commands/scard
        ///// </summary>
        //public Task<long> GetLength(CommandFlags commandFlags = CommandFlags.None)
        //{
        //    return TraceHelper.RecordReceive(Settings, Key, CallType, () =>
        //   {
        //       return Command.GetLength(Settings.Db, Key, commandFlags);
        //   });
        //}

        ///// <summary>
        ///// SRANDMEMBER http://redis.io/commands/srandmember
        ///// </summary>
        //public Task<T> GetRandom(CommandFlags commandFlags = CommandFlags.None)
        //{
        //    return TraceHelper.RecordReceive(Settings, Key, CallType, async () =>
        //    {
        //        var v = await Command.GetRandom(Settings.Db, Key, commandFlags).ConfigureAwait(false);
        //        return Settings.ValueConverter.Deserialize<T>(v);
        //    });
        //}

        ///// <summary>
        ///// SRANDMEMBER http://redis.io/commands/srandmember
        ///// </summary>
        //public Task<T[]> GetRandom(int count, CommandFlags commandFlags = CommandFlags.None)
        //{
        //    return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
        //    {
        //        var v = await Command.GetRandom(Settings.Db, Key, count, commandFlags).ConfigureAwait(false);
        //        var r = v.Select(Settings.ValueConverter.Deserialize<T>).ToArray();
        //        return Pair.Create(new { count }, r);
        //    });
        //}

        ///// <summary>
        ///// SREM http://redis.io/commands/srem
        ///// </summary>
        //public Task<bool> Remove(T member, CommandFlags commandFlags = CommandFlags.None)
        //{
        //    return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
        //    {
        //        var r = await Command.Remove(Settings.Db, Key, Settings.ValueConverter.Serialize(member), commandFlags).ConfigureAwait(false);
        //        return Pair.Create(new { member }, r);
        //    });
        //}

        ///// <summary>
        ///// SREM http://redis.io/commands/srem
        ///// </summary>
        //public Task<long> Remove(T[] members, CommandFlags commandFlags = CommandFlags.None)
        //{
        //    return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
        //    {
        //        var v = members.Select(x => Settings.ValueConverter.Serialize(x)).ToArray();
        //        var r = await Command.Remove(Settings.Db, Key, v, commandFlags).ConfigureAwait(false);

        //        return Pair.Create(new { members }, r);
        //    });
        //}

        ///// <summary>
        ///// SPOP http://redis.io/commands/spop
        ///// </summary>
        //public Task<T> RemoveRandom(CommandFlags commandFlags = CommandFlags.None)
        //{
        //    return TraceHelper.RecordReceive(Settings, Key, CallType, async () =>
        //    {
        //        var v = await Command.RemoveRandom(Settings.Db, Key, commandFlags).ConfigureAwait(false);
        //        return Settings.ValueConverter.Deserialize<T>(v);
        //    });
        //}
    }
}