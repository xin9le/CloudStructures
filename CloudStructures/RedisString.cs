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
        public Task<Tuple<bool, T>> TryGet(CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordReceive(Settings, Key, CallType, async () =>
            {
                var value = await Command.StringGetAsync(Key, commandFlags).ConfigureAwait(false);

                var size = 0L;
                return (value.IsNull)
                    ? Pair.CreateReceived(Tuple.Create(false, default(T)), size)
                    : Pair.CreateReceived(Tuple.Create(true, Settings.ValueConverter.Deserialize<T>(value, out size)), size);
            });
        }

        /// <summary>
        /// GET http://redis.io/commands/get
        /// </summary>
        public async Task<T> GetValueOrDefault(T defaultValue = default(T), CommandFlags commandFlags = CommandFlags.None)
        {
            var result = await TryGet(commandFlags).ConfigureAwait(false);
            return result.Item1 ? result.Item2 : defaultValue;
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

                var result = await this.ExecuteWithKeyExpire(x => x.StringGetSetAsync(Key, v, commandFlags), Key, expiry, commandFlags).ConfigureAwait(false);
                var r = Settings.ValueConverter.Deserialize<T>(result, out receivedSize);

                return Pair.CreatePair(new { value, expiry = expiry?.Value }, sentSize, r, receivedSize);
            });
        }

        /// <summary>
        /// GET, SET http://redis.io/commands/get http://redis.io/commands/set
        /// </summary>
        public async Task<T> GetOrSet(Func<T> valueFactory, TimeSpan? expiry = null, bool keepValueFactorySynchronizationContext = false, CommandFlags commandFlags = CommandFlags.None)
        {
            var value = await TryGet(commandFlags).ConfigureAwait(keepValueFactorySynchronizationContext); // can choose valueFactory synchronization context
            if (value.Item1)
            {
                return value.Item2;
            }
            else
            {
                var v = valueFactory();
                await Set(v, expiry, When.Always, commandFlags).ConfigureAwait(false);
                return v;
            }
        }

        /// <summary>
        /// GET, SET http://redis.io/commands/get http://redis.io/commands/set
        /// </summary>
        public async Task<T> GetOrSet(Func<Task<T>> valueFactory, TimeSpan? expiry = null, bool keepValueFactorySynchronizationContext = false, CommandFlags commandFlags = CommandFlags.None)
        {
            var value = await TryGet(commandFlags).ConfigureAwait(keepValueFactorySynchronizationContext); // can choose valueFactory synchronization context
            if (value.Item1)
            {
                return value.Item2;
            }
            else
            {
                var v = await valueFactory().ConfigureAwait(false);
                await Set(v, expiry, When.Always, commandFlags).ConfigureAwait(false);
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
                var r = await Command.StringSetAsync(Key, v, expiry, when, CommandFlags.None).ConfigureAwait(false);

                return Pair.CreatePair(new { value, expiry }, sentSize, r, sizeof(bool));
            });
        }

        /// <summary>
        /// INCRBY http://redis.io/commands/incrby
        /// </summary>
        public Task<long> Increment(long value = 1, RedisExpiry expiry = null, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var r = await this.ExecuteWithKeyExpire(x => x.StringIncrementAsync(Key, value, commandFlags), Key, expiry, commandFlags).ConfigureAwait(false);
                return Pair.CreatePair(new { value, expiry = expiry?.Value }, sizeof(long), r, sizeof(long));
            });
        }

        /// <summary>
        /// INCRBYFLOAT http://redis.io/commands/incrbyfloat
        /// </summary>
        public Task<double> Increment(double value, RedisExpiry expiry = null, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var r = await this.ExecuteWithKeyExpire(x => x.StringIncrementAsync(Key, value, commandFlags), Key, expiry, commandFlags).ConfigureAwait(false);
                return Pair.CreatePair(new { value, expiry = expiry?.Value }, sizeof(double), r, sizeof(double));
            });
        }

        /// <summary></summary>
        public Task<long> Decrement(long value = 1, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var r = await Command.StringDecrementAsync(Key, value, commandFlags).ConfigureAwait(false);
                return Pair.CreatePair(new { value }, sizeof(long), r, sizeof(long));
            });
        }

        /// <summary></summary>
        public Task<double> Decrement(double value = 1, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var r = await Command.StringDecrementAsync(Key, value, commandFlags).ConfigureAwait(false);
                return Pair.CreatePair(new { value }, sizeof(double), r, sizeof(double));
            });
        }

        /// <summary></summary>
        public Task<long> IncrementLimitByMax(long value, long max, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var v = Command.ScriptEvaluateAsync(@"
local inc = tonumber(ARGV[1])
local max = tonumber(ARGV[2])
local x = redis.call('incrby', KEYS[1], inc)
if(x > max) then
    redis.call('set', KEYS[1], max)
    x = max
end
return x", new[] { Key }, new RedisValue[] { value, max }, commandFlags);
                var r = (long)(await v.ConfigureAwait(false));

                return Pair.CreatePair(new { value, max }, sizeof(long) * 2, r, sizeof(long));
            });
        }

        /// <summary></summary>
        public Task<long> IncrementLimitByMin(long value, long min, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var v = Command.ScriptEvaluateAsync(@"
local inc = tonumber(ARGV[1])
local min = tonumber(ARGV[2])
local x = redis.call('incrby', KEYS[1], inc)
if(x < min) then
    redis.call('set', KEYS[1], min)
    x = min
end
return x", new[] { Key }, new RedisValue[] { value, min }, commandFlags);
                var r = (long)(await v.ConfigureAwait(false));

                return Pair.CreatePair(new { value, min }, sizeof(long) * 2, r, sizeof(long));
            });
        }

        /// <summary></summary>
        public Task<double> IncrementLimitByMax(double value, double max, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var v = Command.ScriptEvaluateAsync(@"
local inc = tonumber(ARGV[1])
local max = tonumber(ARGV[2])
local x = tonumber(redis.call('incrbyfloat', KEYS[1], inc))
if(x > max) then
    redis.call('set', KEYS[1], max)
    x = max
end
return tostring(x)", new[] { Key }, new RedisValue[] { value, max }, commandFlags);
                var r = double.Parse((string)(await v.ConfigureAwait(false)));

                return Pair.CreatePair(new { value, max }, sizeof(double) * 2, r, sizeof(double));
            });
        }

        /// <summary></summary>
        public Task<double> IncrementLimitByMin(double value, double min, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var v = Command.ScriptEvaluateAsync(@"
local inc = tonumber(ARGV[1])
local min = tonumber(ARGV[2])
local x = tonumber(redis.call('incrbyfloat', KEYS[1], inc))
if(x < min) then
    redis.call('set', KEYS[1], min)
    x = min
end
return tostring(x)", new[] { Key }, new RedisValue[] { value, min }, commandFlags);
                var r = double.Parse((string)(await v.ConfigureAwait(false)));

                return Pair.CreatePair(new { value, min }, sizeof(double) * 2, r, sizeof(double));
            });
        }

        /// <summary></summary>
        public Task<bool> SetBit(long offset, bool bit, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var r = await Command.StringSetBitAsync(Key, offset, bit, commandFlags).ConfigureAwait(false);

                return Pair.CreatePair(new { offset, bit }, sizeof(long) * 2, r, sizeof(bool));
            });
        }

        /// <summary></summary>
        public Task<bool> GetBit(long offset, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var r = await Command.StringGetBitAsync(Key, offset, commandFlags).ConfigureAwait(false);

                return Pair.CreatePair(new { offset }, sizeof(long), r, sizeof(bool));
            });
        }

        /// <summary></summary>
        public Task<long> BitCount(long start = 0, long end = -1, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var r = await Command.StringBitCountAsync(Key, start, end, commandFlags).ConfigureAwait(false);

                return Pair.CreatePair(new { start, end }, sizeof(long) * 2, r, sizeof(long));
            });
        }

        /// <summary></summary>
        public Task<long> BitPosition(bool bit, long start = 0, long end = -1, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var r = await Command.StringBitPositionAsync(Key, bit, start, end, commandFlags).ConfigureAwait(false);

                return Pair.CreatePair(new { bit, start, end }, sizeof(bool) + sizeof(long) * 2, r, sizeof(long));
            });
        }
    }
}