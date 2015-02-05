using StackExchange.Redis;
using System.Linq;
using System.Threading.Tasks;

namespace CloudStructures
{
    public sealed class RedisHyperLogLog<T> : RedisStructure
    {
        protected override string CallType
        {
            get { return "RedisHyperLogLog"; }
        }

        public RedisHyperLogLog(RedisSettings settings, RedisKey key)
            : base(settings, key)
        {
        }

        public RedisHyperLogLog(RedisGroup connectionGroup, RedisKey key)
            : base(connectionGroup, key)
        {
        }

        /// <summary>
        /// PFADD http://redis.io/commands/pfadd
        /// </summary>
        public Task<bool> Add(T value, CommandFlags flags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                long sentSize;
                var v = Settings.ValueConverter.Serialize(value, out sentSize);
                var r = await Command.HyperLogLogAddAsync(Key, v, flags).ForAwait();
                return Tracing.CreateSentAndReceived(new { value }, sentSize, r, sizeof(bool));
            });
        }

        /// <summary>
        /// PFADD http://redis.io/commands/pfadd
        /// </summary>
        public Task<bool> Add(T[] values, CommandFlags flags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                long sentSize = 0;
                var redisValues = values
                    .Select(x =>
                    {
                        long size;
                        var v = Settings.ValueConverter.Serialize(x, out size);
                        sentSize += size;
                        return v;
                    })
                    .ToArray();

                var r = await Command.HyperLogLogAddAsync(Key, redisValues, flags).ForAwait();
                return Tracing.CreateSentAndReceived(new { values }, sentSize, r, sizeof(bool));
            });
        }

        /// <summary>
        /// PFCOUNT http://redis.io/commands/pfcount
        /// </summary>
        public Task<long> Length(CommandFlags flags = CommandFlags.None)
        {
            return TraceHelper.RecordReceive(Settings, Key, CallType, async () =>
            {
                var r = await Command.HyperLogLogLengthAsync(Key, flags).ForAwait();
                return Tracing.CreateReceived(r, sizeof(long));
            });
        }
        
        // MERGE
    }
}