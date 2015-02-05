using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CloudStructures
{
    public class RedisList<T> : RedisStructure
    {
        protected override string CallType
        {
            get { return "RedisList"; }
        }

        public RedisList(RedisSettings settings, RedisKey listKey)
            : base(settings, listKey)
        {
        }

        public RedisList(RedisGroup connectionGroup, RedisKey listKey)
            : base(connectionGroup, listKey)
        {
        }

        /// <summary>
        /// LPUSH http://redis.io/commands/lpush
        /// </summary>
        public Task<long> LeftPush(T value, RedisExpiry expiry = null, When when = When.Always, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                long sentSize;
                var v = Settings.ValueConverter.Serialize(value, out sentSize);
                var vr = await this.ExecuteWithKeyExpire(x => x.ListLeftPushAsync(Key, v, when, commandFlags), Key, expiry, commandFlags).ForAwait();
                return Tracing.CreateSentAndReceived(new { value, expiry = expiry?.Value, when }, sentSize, vr, sizeof(long));
            });
        }

        /// <summary>
        /// LPUSH http://redis.io/commands/lpush
        /// </summary>
        public Task<long> LeftPush(T[] values, RedisExpiry expiry = null, CommandFlags commandFlags = CommandFlags.None)
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

                var vr = await this.ExecuteWithKeyExpire(x => x.ListLeftPushAsync(Key, redisValues, commandFlags), Key, expiry, commandFlags).ForAwait();
                return Tracing.CreateSentAndReceived(new { values, expiry = expiry?.Value }, sentSize, vr, sizeof(long));
            });
        }

        /// <summary>
        /// RPUSH http://redis.io/commands/rpush
        /// </summary>
        public Task<long> RightPush(T value, RedisExpiry expiry = null, When when = When.Always, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                long sentSize;
                var v = Settings.ValueConverter.Serialize(value, out sentSize);
                var vr = await this.ExecuteWithKeyExpire(x => x.ListRightPushAsync(Key, v, when, commandFlags), Key, expiry, commandFlags).ForAwait();
                return Tracing.CreateSentAndReceived(new { value, expiry = expiry?.Value, when }, sentSize, vr, sizeof(long));
            });
        }

        /// <summary>
        /// RPUSH http://redis.io/commands/rpush
        /// </summary>
        public Task<long> RightPush(T[] values, RedisExpiry expiry = null, CommandFlags commandFlags = CommandFlags.None)
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

                var vr = await this.ExecuteWithKeyExpire(x => x.ListRightPushAsync(Key, redisValues, commandFlags), Key, expiry, commandFlags).ForAwait();
                return Tracing.CreateSentAndReceived(new { values, expiry = expiry?.Value }, sentSize, vr, sizeof(long));
            });
        }

        /// <summary>
        /// LINDEX http://redis.io/commands/lindex
        /// </summary>
        public Task<RedisResult<T>> GetByIndex(long index, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var value = await Command.ListGetByIndexAsync(Key, index, commandFlags).ForAwait();
                long valueSize;
                var result = RedisResult.FromRedisValue<T>(value, Settings, out valueSize);

                return Tracing.CreateSentAndReceived(new { index }, sizeof(long), result, valueSize);
            });
        }

        /// <summary>
        /// LLEN http://redis.io/commands/llen
        /// </summary>
        public Task<long> Length(CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordReceive(Settings, Key, CallType, async () =>
            {
                var result = await Command.ListLengthAsync(Key, commandFlags).ForAwait();
                return Tracing.CreateReceived(result, sizeof(long));
            });
        }

        /// <summary>
        /// LRANGE http://redis.io/commands/lrange
        /// </summary>
        public Task<T[]> Range(long start = 0, long stop = -1, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var results = await Command.ListRangeAsync(Key, start, stop, commandFlags).ForAwait();
                long receivedSize = 0;
                var resultArray = results.Select(x =>
                {
                    long size;
                    var r = Settings.ValueConverter.Deserialize<T>(x, out size);
                    receivedSize += size;
                    return r;
                }).ToArray();
                return Tracing.CreateSentAndReceived(new { start, stop }, sizeof(long) * 2, resultArray, receivedSize);
            });
        }

        /// <summary>
        /// <para>LREM http://redis.io/commands/lrem</para>
        /// <para>count &gt; 0: Remove elements equal to value moving from head to tail.</para>
        /// <para>count &lt; 0: Remove elements equal to value moving from tail to head.</para>
        /// <para>count = 0: Remove all elements equal to value.</para>
        /// </summary>
        public Task<long> Remove(T value, long count = 0, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                long size;
                var v = Settings.ValueConverter.Serialize(value, out size);
                var r = await Command.ListRemoveAsync(Key, v, count, commandFlags).ForAwait();

                return Tracing.CreateSentAndReceived(new { value, count }, sizeof(long), r, sizeof(long));
            });
        }

        /// <summary>
        /// LPOP http://redis.io/commands/lpop
        /// </summary>
        public Task<RedisResult<T>> LeftPop(CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordReceive(Settings, Key, CallType, async () =>
            {
                var result = await Command.ListLeftPopAsync(Key, commandFlags).ForAwait();
                long receivedSize;
                var r = RedisResult.FromRedisValue<T>(result, Settings, out receivedSize);
                return Tracing.CreateReceived(r, receivedSize);
            });
        }

        /// <summary>
        /// RPOP http://redis.io/commands/rpop
        /// </summary>
        public Task<RedisResult<T>> RightPop(CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordReceive(Settings, Key, CallType, async () =>
            {
                var result = await Command.ListRightPopAsync(Key, commandFlags).ForAwait();
                long receivedSize;
                var r = RedisResult.FromRedisValue<T>(result, Settings, out receivedSize);
                return Tracing.CreateReceived(r, receivedSize);
            });
        }

        /// <summary>
        /// LSET http://redis.io/commands/lset
        /// </summary>
        public Task SetByIndex(int index, T value, RedisExpiry expiry = null, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSend(Settings, Key, CallType, async () =>
            {
                long sentSize;
                var v = Settings.ValueConverter.Serialize(value, out sentSize);
                await this.ExecuteWithKeyExpire(x => x.ListSetByIndexAsync(Key, index, v, commandFlags), Key, expiry, commandFlags).ForAwait();
                return Tracing.CreateSent(new { index, value }, sentSize);
            });
        }

        /// <summary>
        /// LTRIM http://redis.io/commands/ltrim
        /// </summary>
        public Task Trim(long start, long stop, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSend(Settings, Key, CallType, async () =>
            {
                await Command.ListTrimAsync(Key, start, stop, commandFlags);
                return Tracing.CreateSent(new { start, stop }, sizeof(long) * 2);
            });
        }

        /// <summary>
        /// LINSERT http://redis.io/commands/linsert
        /// </summary>
        public Task<long> InsertAfter(T pivot, T value, RedisExpiry expiry = null, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                long sentSize1;
                long sentSize2;
                var p = Settings.ValueConverter.Serialize(pivot, out sentSize1);
                var v = Settings.ValueConverter.Serialize(value, out sentSize2);

                var vr = await this.ExecuteWithKeyExpire(x => x.ListInsertAfterAsync(Key, p, v, commandFlags), Key, expiry, commandFlags).ForAwait();
                return Tracing.CreateSentAndReceived(new { pivot, value, expiry = expiry?.Value }, sentSize1 + sentSize2, vr, sizeof(long));
            });
        }

        /// <summary>
        /// LINSERT http://redis.io/commands/linsert
        /// </summary>
        public Task<long> InsertBefore(T pivot, T value, RedisExpiry expiry = null, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                long sentSize1;
                long sentSize2;
                var p = Settings.ValueConverter.Serialize(pivot, out sentSize1);
                var v = Settings.ValueConverter.Serialize(value, out sentSize2);

                var vr = await this.ExecuteWithKeyExpire(x => x.ListInsertBeforeAsync(Key, p, v, commandFlags), Key, expiry, commandFlags).ForAwait();
                return Tracing.CreateSentAndReceived(new { pivot, value, expiry = expiry?.Value }, sentSize1 + sentSize2, vr, sizeof(long));
            });
        }

        // additional commands

        /// <summary>
        /// Simulate fixed size list includes LPUSH, TRIM.
        /// </summary>
        public Task<long> LeftPushAndFixLength(T value, long fixLength, RedisExpiry expiry = null, When when = When.Always, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                long sentSize;
                var v = Settings.ValueConverter.Serialize(value, out sentSize);
                var tx = CreateTransaction();
                var leftpush = tx.ListLeftPushAsync(Key, v, when, commandFlags);
                var trim = tx.ListTrimAsync(Key, 0, fixLength - 1, commandFlags);
                if (expiry != null)
                {
                    var expire = expiry.KeyExpire(tx, Key, commandFlags);
                }
                await tx.ExecuteAsync(commandFlags).ForAwait();
                var r = await leftpush.ForAwait();

                return Tracing.CreateSentAndReceived(new { value, fixLength, expiry = expiry?.Value, when }, sentSize, r, sizeof(long));
            });
        }
    }
}