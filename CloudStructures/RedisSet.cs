using StackExchange.Redis;
using System;
using System.Linq;
using System.Threading.Tasks;

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
                var r = await this.ExecuteWithKeyExpire(x => x.SetAddAsync(Key, v, commandFlags), Key, expiry, commandFlags).ForAwait();
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

        /// <summary>
        /// SISMEMBER http://redis.io/commands/sismember
        /// </summary>
        public Task<bool> Contains(T value, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                long s;
                var sr = Settings.ValueConverter.Serialize(value, out s);
                var r = await Command.SetContainsAsync(Key, sr, commandFlags).ForAwait();
                return Tracing.CreateSentAndReceived(new { value }, s, r, sizeof(bool));
            });
        }

        /// <summary>
        /// SMEMBERS http://redis.io/commands/smembers
        /// </summary>
        public Task<T[]> Members(CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordReceive(Settings, Key, CallType, async () =>
            {
                var v = await Command.SetMembersAsync(Key, commandFlags).ForAwait();
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

        /// <summary>
        /// SCARD http://redis.io/commands/scard
        /// </summary>
        public Task<long> Length(CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordReceive(Settings, Key, CallType, async () =>
           {
               var r = await Command.SetLengthAsync(Key, commandFlags).ForAwait();
               return Tracing.CreateReceived(r, sizeof(long));
           });
        }

        /// <summary>
        /// SRANDMEMBER http://redis.io/commands/srandmember
        /// </summary>
        public Task<RedisResult<T>> RandomMember(CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordReceive(Settings, Key, CallType, async () =>
            {
                var v = await Command.SetRandomMemberAsync(Key, commandFlags).ForAwait();
                long size;
                var result = RedisResult.FromRedisValue<T>(v, Settings, out size);
                return Tracing.CreateReceived(result, size);
            });
        }

        /// <summary>
        /// SRANDMEMBER http://redis.io/commands/srandmember
        /// </summary>
        public Task<T[]> RandomMembers(long count, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var v = await Command.SetRandomMembersAsync(Key, count, commandFlags).ForAwait();
                var size = 0L;
                var result = v.Select(x =>
                {
                    long s;
                    var r = Settings.ValueConverter.Deserialize<T>(x, out s);
                    size += s;
                    return r;
                }).ToArray();

                return Tracing.CreateSentAndReceived(new { count }, sizeof(long), result, size);
            });
        }

        /// <summary>
        /// SREM http://redis.io/commands/srem
        /// </summary>
        public Task<bool> Remove(T member, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                long sentSize;
                var v = Settings.ValueConverter.Serialize(member, out sentSize);
                var r = await Command.SetRemoveAsync(Key, v, commandFlags).ForAwait();

                return Tracing.CreateSentAndReceived(new { member }, sentSize, r, sizeof(bool));
            });
        }

        /// <summary>
        /// SREM http://redis.io/commands/srem
        /// </summary>
        public Task<long> Remove(T[] members, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var size = 0L;
                var values = members.Select(x =>
                {
                    long s;
                    var v = Settings.ValueConverter.Serialize(x, out s);
                    size += s;
                    return v;
                }).ToArray();

                var r = await Command.SetRemoveAsync(Key, values, commandFlags).ForAwait();

                return Tracing.CreateSentAndReceived(new { members }, size, r, sizeof(long));
            });
        }

        /// <summary>
        /// SPOP http://redis.io/commands/spop
        /// </summary>
        public Task<RedisResult<T>> Pop(CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordReceive(Settings, Key, CallType, async () =>
            {
                var v = await Command.SetPopAsync(Key, commandFlags).ForAwait();
                long size;
                var result = RedisResult.FromRedisValue<T>(v, Settings, out size);
                return Tracing.CreateReceived(result, size);
            });
        }
    }
}