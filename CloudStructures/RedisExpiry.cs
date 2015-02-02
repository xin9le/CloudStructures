using StackExchange.Redis;
using System;
using System.Threading.Tasks;

namespace CloudStructures
{
    /// <summary>
    /// <pre>RedisExpiry represents TimeSpan or DateTime for Redis key expire operation.</pre>
    /// <pre>You can convert implicit operator.</pre>
    /// </summary>
    public class RedisExpiry
    {
        public bool IsDateTime { get; private set; }
        public bool IsTimeSpan { get; private set; }
        readonly TimeSpan timeSpan;
        readonly DateTime dateTime;

        RedisExpiry(TimeSpan timeSpan)
        {
            IsTimeSpan = true;
            this.timeSpan = timeSpan;
        }

        RedisExpiry(DateTime dateTime)
        {
            IsDateTime = true;
            this.dateTime = dateTime;
        }

        public static implicit operator RedisExpiry(TimeSpan timeSpan)
        {
            return new RedisExpiry(timeSpan);
        }

        public static implicit operator RedisExpiry(TimeSpan? timeSpan)
        {
            return (timeSpan == null) ? null : new RedisExpiry(timeSpan.Value);
        }

        public static implicit operator RedisExpiry(DateTime dateTime)
        {
            return new RedisExpiry(dateTime);
        }

        public static implicit operator RedisExpiry(DateTime? dateTime)
        {
            return (dateTime == null) ? null : new RedisExpiry(dateTime.Value);
        }

        internal object Value
        {
            get { return (IsDateTime) ? (object)dateTime : timeSpan; }
        }

        internal Task<bool> KeyExpire(IDatabaseAsync database, RedisKey key, CommandFlags flags)
        {
            if (IsDateTime)
            {
                return database.KeyExpireAsync(key, this.dateTime, flags);
            }
            else
            {
                return database.KeyExpireAsync(key, this.timeSpan, flags);
            }
        }
    }

    internal static class WithRedisExpiryExtensions
    {
        public static async Task<T> ExecuteWithKeyExpire<T>(this RedisStructure redisStructure, Func<IDatabaseAsync, Task<T>> command, RedisKey key, RedisExpiry expiry, CommandFlags commandFlags)
        {
            if (expiry == null)
            {
                return await command(redisStructure.Command).ConfigureAwait(false);
            }
            else
            {
                var tx = redisStructure.CreateTransaction();
                var future = command(tx);
                var expire = expiry.KeyExpire(tx, key, commandFlags);
                await tx.ExecuteAsync(commandFlags).ConfigureAwait(false);
                return await future.ConfigureAwait(false);
            }
        }
    }
}