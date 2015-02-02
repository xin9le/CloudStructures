using StackExchange.Redis;
using System.Threading.Tasks;

namespace CloudStructures
{
    public class RedisLua : RedisStructure
    {
        protected override string CallType
        {
            get
            {
                return "RedisLua";
            }
        }

        public RedisLua(RedisSettings settings, RedisKey distributedKey)
            : base(settings, distributedKey)
        {
        }

        public RedisLua(RedisGroup connectionGroup, RedisKey distributedKey)
            : base(connectionGroup, distributedKey)
        {
        }

        /// <summary>
        /// EVALSHA http://redis.io/commands/evalsha
        /// </summary>
        public Task<T> ScriptEvaluate<T>(string script, RedisKey[] keys = null, RedisValue[] values = null, CommandFlags flags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var redisResult = await Command.ScriptEvaluateAsync(script, keys, values, flags).ForAwait();
                var result = (RedisValue)redisResult;

                long receivedSize;
                var r = Settings.ValueConverter.Deserialize<T>(result, out receivedSize);

                // script size is unknown but it's cached SHA.
                return Tracing.CreateSentAndReceived(new { script, keys, values }, 40, r, receivedSize);
            });
        }
    }
}