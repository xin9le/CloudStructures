using StackExchange.Redis;
using System;
using System.Threading.Tasks;

namespace CloudStructures
{
    public sealed class RedisString<T> : RedisStructure
    {
        protected override string CallType
        {
            get { return "RedisString"; }
        }

        public RedisString(RedisSettings settings, RedisKey key)
            : base(settings, key)
        {
        }

        public RedisString(RedisGroup connectionGroup, RedisKey key)
            : base(connectionGroup, key)
        {
        }

        /// <summary>
        /// GET http://redis.io/commands/get
        /// </summary>
        public Task<RedisResult<T>> Get(CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordReceive(Settings, Key, CallType, async () =>
            {
                var value = await Command.StringGetAsync(Key, commandFlags).ForAwait();

                var size = 0L;
                var result = RedisResult.FromRedisValue<T>(value, Settings, out size);
                return Tracing.CreateReceived(result, size);
            });
        }

        /// <summary>
        /// GETSET http://redis.io/commands/getset
        /// </summary>
        public Task<T> GetSet(T value, RedisExpiry expiry = null, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                long sentSize;
                long receivedSize;
                var v = Settings.ValueConverter.Serialize(value, out sentSize);

                var result = await this.ExecuteWithKeyExpire(x => x.StringGetSetAsync(Key, v, commandFlags), Key, expiry, commandFlags).ForAwait();
                var r = Settings.ValueConverter.Deserialize<T>(result, out receivedSize);

                return Tracing.CreateSentAndReceived(new { value, expiry = expiry?.Value }, sentSize, r, receivedSize);
            });
        }

        /// <summary>
        /// GET, SET http://redis.io/commands/get http://redis.io/commands/set
        /// </summary>
        public async Task<T> GetOrSet(Func<T> valueFactory, TimeSpan? expiry = null, bool keepValueFactorySynchronizationContext = false, CommandFlags commandFlags = CommandFlags.None)
        {
            var value = await Get(commandFlags).ConfigureAwait(keepValueFactorySynchronizationContext); // can choose valueFactory synchronization context
            if (value.HasValue)
            {
                return value.Value;
            }
            else
            {
                var v = valueFactory();
                await Set(v, expiry, When.Always, commandFlags).ForAwait();
                return v;
            }
        }

        /// <summary>
        /// GET, SET http://redis.io/commands/get http://redis.io/commands/set
        /// </summary>
        public async Task<T> GetOrSet(Func<Task<T>> valueFactory, TimeSpan? expiry = null, bool keepValueFactorySynchronizationContext = false, CommandFlags commandFlags = CommandFlags.None)
        {
            var value = await Get(commandFlags).ConfigureAwait(keepValueFactorySynchronizationContext); // can choose valueFactory synchronization context
            if (value.HasValue)
            {
                return value.Value;
            }
            else
            {
                var v = await valueFactory().ForAwait();
                await Set(v, expiry, When.Always, commandFlags).ForAwait();
                return v;
            }
        }

        /// <summary>
        /// SET http://redis.io/commands/set
        /// </summary>
        public Task<bool> Set(T value, TimeSpan? expiry = null, When when = When.Always, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                long sentSize;
                var v = Settings.ValueConverter.Serialize(value, out sentSize);
                var r = await Command.StringSetAsync(Key, v, expiry, when, CommandFlags.None).ForAwait();

                return Tracing.CreateSentAndReceived(new { value, expiry, when }, sentSize, r, sizeof(bool));
            });
        }

        /// <summary>
        /// INCRBY http://redis.io/commands/incrby
        /// </summary>
        public Task<long> Increment(long value = 1, RedisExpiry expiry = null, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var r = await this.ExecuteWithKeyExpire(x => x.StringIncrementAsync(Key, value, commandFlags), Key, expiry, commandFlags).ForAwait();
                return Tracing.CreateSentAndReceived(new { value, expiry = expiry?.Value }, sizeof(long), r, sizeof(long));
            });
        }

        /// <summary>
        /// INCRBYFLOAT http://redis.io/commands/incrbyfloat
        /// </summary>
        public Task<double> Increment(double value, RedisExpiry expiry = null, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var r = await this.ExecuteWithKeyExpire(x => x.StringIncrementAsync(Key, value, commandFlags), Key, expiry, commandFlags).ForAwait();
                return Tracing.CreateSentAndReceived(new { value, expiry = expiry?.Value }, sizeof(double), r, sizeof(double));
            });
        }

        /// <summary>
        /// DECRBY http://redis.io/commands/decrby
        /// </summary>
        public Task<long> Decrement(long value = 1, RedisExpiry expiry = null, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var r = await this.ExecuteWithKeyExpire(x => x.StringDecrementAsync(Key, value, commandFlags), Key, expiry, commandFlags).ForAwait();
                return Tracing.CreateSentAndReceived(new { value, expiry = expiry?.Value }, sizeof(long), r, sizeof(long));
            });
        }

        /// <summary>
        /// INCRBYFLOAT http://redis.io/commands/incrbyfloat
        /// </summary>
        public Task<double> Decrement(double value = 1, RedisExpiry expiry = null, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var r = await this.ExecuteWithKeyExpire(x => x.StringDecrementAsync(Key, value, commandFlags), Key, expiry, commandFlags).ForAwait();
                return Tracing.CreateSentAndReceived(new { value, expiry = expiry?.Value }, sizeof(double), r, sizeof(double));
            });
        }

        /// <summary>
        /// LUA Script including incrby, set
        /// </summary>
        public Task<long> IncrementLimitByMax(long value, long max, RedisExpiry expiry = null, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var v = await this.ExecuteWithKeyExpire(x => x.ScriptEvaluateAsync(@"
local inc = tonumber(ARGV[1])
local max = tonumber(ARGV[2])
local x = redis.call('incrby', KEYS[1], inc)
if(x > max) then
    redis.call('set', KEYS[1], max)
    x = max
end
return x", new[] { Key }, new RedisValue[] { value, max }, commandFlags), Key, expiry, commandFlags).ForAwait();
                var r = (long)v;

                return Tracing.CreateSentAndReceived(new { value, max, expiry = expiry?.Value }, sizeof(long) * 2, r, sizeof(long));
            });
        }

        /// <summary>
        /// LUA Script including incrby, set
        /// </summary>
        public Task<long> IncrementLimitByMin(long value, long min, RedisExpiry expiry = null, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var v = await this.ExecuteWithKeyExpire(x => x.ScriptEvaluateAsync(@"
local inc = tonumber(ARGV[1])
local min = tonumber(ARGV[2])
local x = redis.call('incrby', KEYS[1], inc)
if(x < min) then
    redis.call('set', KEYS[1], min)
    x = min
end
return x", new[] { Key }, new RedisValue[] { value, min }, commandFlags), Key, expiry, commandFlags).ForAwait();
                var r = (long)v;

                return Tracing.CreateSentAndReceived(new { value, min, expiry = expiry?.Value }, sizeof(long) * 2, r, sizeof(long));
            });
        }

        /// <summary>
        /// LUA Script including incrbyfloat, set
        /// </summary>
        public Task<double> IncrementLimitByMax(double value, double max, RedisExpiry expiry = null, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var v = await this.ExecuteWithKeyExpire(x => x.ScriptEvaluateAsync(@"
local inc = tonumber(ARGV[1])
local max = tonumber(ARGV[2])
local x = tonumber(redis.call('incrbyfloat', KEYS[1], inc))
if(x > max) then
    redis.call('set', KEYS[1], max)
    x = max
end
return tostring(x)", new[] { Key }, new RedisValue[] { value, max }, commandFlags), Key, expiry, commandFlags).ForAwait();
                var r = double.Parse((string)v);

                return Tracing.CreateSentAndReceived(new { value, max, expiry = expiry?.Value }, sizeof(double) * 2, r, sizeof(double));
            });
        }

        /// <summary>
        /// LUA Script including incrbyfloat, set
        /// </summary>
        public Task<double> IncrementLimitByMin(double value, double min, RedisExpiry expiry = null, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var v = await this.ExecuteWithKeyExpire(x => x.ScriptEvaluateAsync(@"
local inc = tonumber(ARGV[1])
local min = tonumber(ARGV[2])
local x = tonumber(redis.call('incrbyfloat', KEYS[1], inc))
if(x < min) then
    redis.call('set', KEYS[1], min)
    x = min
end
return tostring(x)", new[] { Key }, new RedisValue[] { value, min }, commandFlags), Key, expiry, commandFlags).ForAwait();
                var r = double.Parse((string)v);

                return Tracing.CreateSentAndReceived(new { value, min, expiry = expiry?.Value }, sizeof(double) * 2, r, sizeof(double));
            });
        }

        /// <summary>
        /// SETBIT http://redis.io/commands/setbit
        /// </summary>
        public Task<bool> SetBit(long offset, bool bit, RedisExpiry expiry = null, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var r = await this.ExecuteWithKeyExpire(x => x.StringSetBitAsync(Key, offset, bit, commandFlags), Key, expiry, commandFlags).ForAwait();

                return Tracing.CreateSentAndReceived(new { offset, bit, expiry = expiry?.Value }, sizeof(long) * 2, r, sizeof(bool));
            });
        }

        /// <summary>
        /// GETBIT http://redis.io/commands/getbit
        /// </summary>
        public Task<bool> GetBit(long offset, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var r = await Command.StringGetBitAsync(Key, offset, commandFlags).ForAwait();

                return Tracing.CreateSentAndReceived(new { offset }, sizeof(long), r, sizeof(bool));
            });
        }

        /// <summary>
        /// BITCOUNT http://redis.io/commands/bitcount
        /// </summary>
        public Task<long> BitCount(long start = 0, long end = -1, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var r = await Command.StringBitCountAsync(Key, start, end, commandFlags).ForAwait();

                return Tracing.CreateSentAndReceived(new { start, end }, sizeof(long) * 2, r, sizeof(long));
            });
        }

        /// <summary>
        /// BITPOSITION http://redis.io/commands/bitpos
        /// </summary>
        public Task<long> BitPosition(bool bit, long start = 0, long end = -1, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var r = await Command.StringBitPositionAsync(Key, bit, start, end, commandFlags).ForAwait();

                return Tracing.CreateSentAndReceived(new { bit, start, end }, sizeof(bool) + sizeof(long) * 2, r, sizeof(long));
            });
        }
    }
}