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
        public Task ScriptEvaluate(string script, RedisKey[] keys = null, RedisValue[] values = null, CommandFlags flags = CommandFlags.None)
        {
            return TraceHelper.RecordSend(Settings, Key, CallType, async () =>
            {
                var redisResult = await Command.ScriptEvaluateAsync(script, keys, values, flags).ForAwait();

                // script size is unknown but it's cached SHA.
                return Tracing.CreateSent(new { script, keys, values }, 40);
            });
        }

        /// <summary>
        /// EVALSHA http://redis.io/commands/evalsha
        /// </summary>
        public Task<RedisResult<T>> ScriptEvaluate<T>(string script, RedisKey[] keys = null, RedisValue[] values = null, CommandFlags flags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var redisResult = await Command.ScriptEvaluateAsync(script, keys, values, flags).ForAwait();

                RedisResult<T> r;
                long receivedSize;
                if (redisResult.IsNull)
                {
                    receivedSize = 0;
                    r = new RedisResult<T>(); // null
                }
                else
                {
                    var result = (RedisValue)redisResult;
                    r = RedisResult.FromRedisValue<T>(result, Settings, out receivedSize);
                }

                // script size is unknown but it's cached SHA.
                return Tracing.CreateSentAndReceived(new { script, keys, values }, 40, r, receivedSize);
            });
        }
    }
}