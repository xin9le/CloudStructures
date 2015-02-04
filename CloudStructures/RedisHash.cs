using StackExchange.Redis;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

// RedisDictionary/Hash/Class
namespace CloudStructures
{
    internal class HashScript
    {
        public const string IncrementLimitByMax = @"
local inc = tonumber(ARGV[1])
local max = tonumber(ARGV[2])
local x = redis.call('hincrby', KEYS[1], KEYS[2], inc)
if(x > max) then
    redis.call('hset', KEYS[1], KEYS[2], max)
    x = max
end
return x";

        public const string IncrementLimitByMin = @"
local inc = tonumber(ARGV[1])
local min = tonumber(ARGV[2])
local x = redis.call('hincrby', KEYS[1], KEYS[2], inc)
if(x < min) then
    redis.call('hset', KEYS[1], KEYS[2], min)
    x = min
end
return x";

        public const string IncrementFloatLimitByMax = @"
local inc = tonumber(ARGV[1])
local max = tonumber(ARGV[2])
local x = tonumber(redis.call('hincrbyfloat', KEYS[1], KEYS[2], inc))
if(x > max) then
    redis.call('hset', KEYS[1], KEYS[2], max)
    x = max
end
return tostring(x)";

        public const string IncrementFloatLimitByMin = @"
local inc = tonumber(ARGV[1])
local min = tonumber(ARGV[2])
local x = tonumber(redis.call('hincrbyfloat', KEYS[1], KEYS[2], inc))
if(x < min) then
    redis.call('hset', KEYS[1], KEYS[2], min)
    x = min
end
return tostring(x)";
    }

    public class RedisDictionary<TKey, TValue> : RedisStructure
    {
        protected override string CallType
        {
            get { return "RedisDictionary"; }
        }

        public RedisDictionary(RedisSettings settings, RedisKey listKey)
            : base(settings, listKey)
        {
        }

        public RedisDictionary(RedisGroup connectionGroup, RedisKey listKey)
            : base(connectionGroup, listKey)
        {
        }

        /// <summary>
        /// HEXISTS http://redis.io/commands/hexists
        /// </summary>
        public Task<bool> Exists(TKey field, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                long keySize;
                var rKey = Settings.ValueConverter.Serialize(field, out keySize);

                var r = await Command.HashExistsAsync(Key, rKey, commandFlags).ForAwait();
                return Tracing.CreateSentAndReceived(new { field }, keySize, r, sizeof(bool));
            });
        }

        /// <summary>
        /// HGET http://redis.io/commands/hget
        /// </summary>
        public Task<RedisResult<TValue>> Get(TKey field, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                long keySize;
                var rKey = Settings.ValueConverter.Serialize(field, out keySize);

                var rValue = await Command.HashGetAsync(Key, rKey, commandFlags).ForAwait();

                long valueSize;
                var value = RedisResult.FromRedisValue<TValue>(rValue, Settings, out valueSize);

                return Tracing.CreateSentAndReceived(new { field }, keySize, value, valueSize);
            });
        }

        /// <summary>
        /// HMGET http://redis.io/commands/hmget
        /// </summary>
        public Task<Dictionary<TKey, TValue>> Get(TKey[] fields, IEqualityComparer<TKey> dictionaryEqualityComparer = null, CommandFlags commandFlags = CommandFlags.None)
        {
            dictionaryEqualityComparer = dictionaryEqualityComparer ?? EqualityComparer<TKey>.Default;

            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                long keySize = 0;
                var hashFields = fields.Select(x =>
                {
                    long s;
                    var rKey = Settings.ValueConverter.Serialize(x, out s);
                    keySize += s;
                    return rKey;
                }).ToArray();

                var rValues = await Command.HashGetAsync(Key, hashFields, commandFlags).ForAwait();

                long valueSize = 0;
                var result = fields
                    .Zip(rValues, (key, x) =>
                    {
                        if (!x.HasValue) return new { key, rValue = default(TValue), x.HasValue };

                        long s;
                        var rValue = Settings.ValueConverter.Deserialize<TValue>(x, out s);
                        valueSize += s;
                        return new { key, rValue, x.HasValue };
                    })
                    .Where(x => x.HasValue)
                    .ToDictionary(x => x.key, x => x.rValue, dictionaryEqualityComparer);

                return Tracing.CreateSentAndReceived(new { fields }, keySize, result, valueSize);
            });
        }

        /// <summary>
        /// HGETALL http://redis.io/commands/hgetall
        /// </summary>
        public Task<Dictionary<TKey, TValue>> GetAll(IEqualityComparer<TKey> dictionaryEqualityComparer = null, CommandFlags commandFlags = CommandFlags.None)
        {
            dictionaryEqualityComparer = dictionaryEqualityComparer ?? EqualityComparer<TKey>.Default;

            return TraceHelper.RecordReceive(Settings, Key, CallType, async () =>
            {
                var hashEntries = await Command.HashGetAllAsync(Key, commandFlags).ForAwait();
                long receivedSize = 0;
                var r = hashEntries.Select(x =>
                {
                    long ss;
                    var vk = Settings.ValueConverter.Deserialize<TKey>(x.Name, out ss);
                    long s;
                    var v = Settings.ValueConverter.Deserialize<TValue>(x.Value, out s);
                    receivedSize += ss;
                    receivedSize += s;
                    return new { key = vk, value = v };
                }).ToDictionary(x => x.key, x => x.value, dictionaryEqualityComparer);

                return Tracing.CreateReceived(r, receivedSize);
            });
        }

        /// <summary>
        /// HKEYS http://redis.io/commands/hkeys
        /// </summary>
        public Task<TKey[]> Keys(CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordReceive(Settings, Key, CallType, async () =>
             {
                 var r = await Command.HashKeysAsync(Key, commandFlags).ForAwait();
                 long keySize = 0;
                 var keys = r.Select(x =>
                 {
                     long s;
                     var key = Settings.ValueConverter.Deserialize<TKey>(x, out s);
                     keySize += s;
                     return key;
                 }).ToArray();

                 return Tracing.CreateReceived(keys, keySize);
             });
        }

        /// <summary>
        /// HVALS http://redis.io/commands/hvals
        /// </summary>
        public Task<TValue[]> Values(CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordReceive(Settings, Key, CallType, async () =>
            {
                var r = await Command.HashValuesAsync(Key, commandFlags).ForAwait();
                long valueSize = 0;
                var values = r.Select(x =>
                {
                    long s;
                    var value = Settings.ValueConverter.Deserialize<TValue>(x, out s);
                    valueSize += s;
                    return value;
                }).ToArray();

                return Tracing.CreateReceived(values, valueSize);
            });
        }

        /// <summary>
        /// HLEN http://redis.io/commands/hlen
        /// </summary>
        public Task<long> Length(CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordReceive(Settings, Key, CallType, async () =>
            {
                var r = await Command.HashLengthAsync(Key, commandFlags).ForAwait();
                return Tracing.CreateReceived(r, sizeof(long));
            });
        }

        /// <summary>
        /// HINCRBY http://redis.io/commands/hincrby
        /// </summary>
        public Task<long> Increment(TKey field, long value = 1, RedisExpiry expiry = null, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                long keySize;
                var rKey = Settings.ValueConverter.Serialize(field, out keySize);
                var r = await this.ExecuteWithKeyExpire(x => x.HashIncrementAsync(Key, rKey, value, commandFlags), Key, expiry, commandFlags).ForAwait();
                return Tracing.CreateSentAndReceived(new { field, value }, keySize, r, sizeof(long));
            });
        }

        /// <summary>
        /// HINCRBYFLOAT http://redis.io/commands/hincrbyfloat
        /// </summary>
        public Task<double> Increment(TKey field, double value, RedisExpiry expiry = null, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                long keySize;
                var rKey = Settings.ValueConverter.Serialize(field, out keySize);
                var r = await this.ExecuteWithKeyExpire(x => x.HashIncrementAsync(Key, rKey, value, commandFlags), Key, expiry, commandFlags).ForAwait();
                return Tracing.CreateSentAndReceived(new { field, value }, keySize, r, sizeof(long));
            });
        }

        /// <summary>
        /// LUA Script including hincrby, hset
        /// </summary>
        public Task<long> IncrementLimitByMax(TKey field, long value, long max, RedisExpiry expiry = null, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                long keySize;
                var rKey = (RedisKey)(byte[])Settings.ValueConverter.Serialize(field, out keySize);
                var v = await this.ExecuteWithKeyExpire(x => x.ScriptEvaluateAsync(HashScript.IncrementLimitByMax, new[] { Key, rKey }, new RedisValue[] { value, max }, commandFlags), Key, expiry, commandFlags).ForAwait();
                var r = (long)v;
                return Tracing.CreateSentAndReceived(new { field, value, max, expiry = expiry?.Value }, keySize + sizeof(long) * 2, r, sizeof(long));
            });
        }

        /// <summary>
        /// LUA Script including hincrbyfloat, hset
        /// </summary>
        public Task<double> IncrementLimitByMax(TKey field, double value, double max, RedisExpiry expiry = null, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                long keySize;
                var rKey = (RedisKey)(byte[])Settings.ValueConverter.Serialize(field, out keySize);
                var v = await this.ExecuteWithKeyExpire(x => x.ScriptEvaluateAsync(HashScript.IncrementFloatLimitByMax, new[] { Key, rKey }, new RedisValue[] { value, max }, commandFlags), Key, expiry, commandFlags).ForAwait();
                var r = (double)v;
                return Tracing.CreateSentAndReceived(new { field, value, max, expiry = expiry?.Value }, keySize + sizeof(double) * 2, r, sizeof(double));
            });
        }

        /// <summary>
        /// LUA Script including hincrby, hset
        /// </summary>
        public Task<long> IncrementLimitByMin(TKey field, long value, long max, RedisExpiry expiry = null, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                long keySize;
                var rKey = (RedisKey)(byte[])Settings.ValueConverter.Serialize(field, out keySize);
                var v = await this.ExecuteWithKeyExpire(x => x.ScriptEvaluateAsync(HashScript.IncrementLimitByMin, new[] { Key, rKey }, new RedisValue[] { value, max }, commandFlags), Key, expiry, commandFlags).ForAwait();
                var r = (long)v;
                return Tracing.CreateSentAndReceived(new { field, value, max, expiry = expiry?.Value }, keySize + sizeof(long) * 2, r, sizeof(long));
            });
        }

        /// <summary>
        /// LUA Script including hincrbyfloat, hset
        /// </summary>
        public Task<double> IncrementLimitByMin(TKey field, double value, double max, RedisExpiry expiry = null, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                long keySize;
                var rKey = (RedisKey)(byte[])Settings.ValueConverter.Serialize(field, out keySize);
                var v = await this.ExecuteWithKeyExpire(x => x.ScriptEvaluateAsync(HashScript.IncrementFloatLimitByMin, new[] { Key, rKey }, new RedisValue[] { value, max }, commandFlags), Key, expiry, commandFlags).ForAwait();
                var r = (double)v;
                return Tracing.CreateSentAndReceived(new { field, value, max, expiry = expiry?.Value }, keySize + sizeof(double) * 2, r, sizeof(double));
            });
        }

        /// <summary>
        /// HDEL http://redis.io/commands/hdel
        /// </summary>
        public Task<bool> Delete(TKey field, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                long keySize;
                var rKey = Settings.ValueConverter.Serialize(field, out keySize);

                var rValue = await Command.HashDeleteAsync(Key, rKey, commandFlags).ForAwait();

                return Tracing.CreateSentAndReceived(new { field }, keySize, rValue, sizeof(bool));
            });
        }

        /// <summary>
        /// HDEL http://redis.io/commands/hdel
        /// </summary>
        public Task<long> Delete(TKey[] fields, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                long keySize = 0;
                var hashFields = fields.Select(x =>
                {
                    long s;
                    var rKey = Settings.ValueConverter.Serialize(x, out s);
                    keySize += s;
                    return rKey;
                }).ToArray();

                var rValue = await Command.HashDeleteAsync(Key, hashFields, commandFlags).ForAwait();

                return Tracing.CreateSentAndReceived(new { fields }, keySize, rValue, sizeof(long));
            });
        }

        /// <summary>
        /// HSET, HSETNX http://redis.io/commands/hset http://redis.io/commands/hsetnx
        /// </summary>
        public Task<bool> Set(TKey field, TValue value, When when = When.Always, RedisExpiry expiry = null, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                long keySize;
                long valueSize;
                var rKey = Settings.ValueConverter.Serialize(field, out keySize);
                var rValue = Settings.ValueConverter.Serialize(value, out valueSize);

                var result = await this.ExecuteWithKeyExpire(x => x.HashSetAsync(Key, rKey, rValue, when, commandFlags), Key, expiry, commandFlags).ForAwait();

                return Tracing.CreateSentAndReceived(new { field, value, when }, keySize + valueSize, result, sizeof(bool));
            });
        }

        /// <summary>
        /// HMSET http://redis.io/commands/hmset
        /// </summary>
        public Task Set(IEnumerable<KeyValuePair<TKey, TValue>> values, RedisExpiry expiry = null, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSend(Settings, Key, CallType, async () =>
            {
                if (!(values is ICollection))
                {
                    values = values.ToArray(); // materialize
                }

                long keySize = 0;
                long valueSize = 0;
                var hashFields = values.Select(x =>
                {
                    long ks;
                    long vs;
                    var rKey = Settings.ValueConverter.Serialize(x.Key, out ks);
                    var rValue = Settings.ValueConverter.Serialize(x.Value, out vs);
                    keySize += ks;
                    valueSize += vs;
                    return new HashEntry(rKey, rValue);
                }).ToArray();

                await this.ExecuteWithKeyExpire(x => x.HashSetAsync(Key, hashFields, commandFlags), Key, expiry, commandFlags).ForAwait();

                return Tracing.CreateSent(new { values }, keySize + valueSize);
            });
        }
    }

    //public class RedisHash
    //{
    //    const string CallType = "RedisHash";

    //    public string Key { get; private set; }
    //    public RedisSettings Settings { get; private set; }

    //    public RedisHash(RedisSettings settings, string hashKey)
    //    {
    //        this.Settings = settings;
    //        this.Key = hashKey;
    //    }

    //    public RedisHash(RedisGroup connectionGroup, string hashKey)
    //        : this(connectionGroup.GetSettings(hashKey), hashKey)
    //    {
    //    }

    //    protected RedisConnection Connection
    //    {
    //        get
    //        {
    //            return Settings.GetConnection();
    //        }
    //    }

    //    protected IHashCommands Command
    //    {
    //        get
    //        {
    //            return Connection.Hashes;
    //        }
    //    }

    //    /// <summary>
    //    /// HEXISTS http://redis.io/commands/hexists
    //    /// </summary>
    //    public Task<bool> Exists(string field, CommandFlags commandFlags = CommandFlags.None)
    //    {
    //        return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
    //        {
    //            var r = await Command.Exists(Settings.Db, Key, field, commandFlags).ForAwait();
    //            return Pair.Create(new { field }, r);
    //        });
    //    }

    //    /// <summary>
    //    /// HGET http://redis.io/commands/hget
    //    /// </summary>
    //    public Task<T> Get<T>(string field, CommandFlags commandFlags = CommandFlags.None)
    //    {
    //        return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
    //        {
    //            var v = await Command.Get(Settings.Db, Key, field, commandFlags).ForAwait();
    //            var r = Settings.ValueConverter.Deserialize<T>(v);
    //            return Pair.Create(new { field }, r);
    //        });
    //    }

    //    /// <summary>
    //    /// HMGET http://redis.io/commands/hmget
    //    /// </summary>
    //    public Task<T[]> Get<T>(string[] fields, CommandFlags commandFlags = CommandFlags.None)
    //    {
    //        return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
    //        {
    //            var v = await Command.Get(Settings.Db, Key, fields, commandFlags).ForAwait();
    //            var r = v.Select(Settings.ValueConverter.Deserialize<T>).ToArray();
    //            return Pair.Create(new { fields }, r);
    //        });
    //    }

    //    /// <summary>
    //    /// HGETALL http://redis.io/commands/hgetall
    //    /// </summary>
    //    public Task<Dictionary<string, T>> GetAll<T>(CommandFlags commandFlags = CommandFlags.None)
    //    {
    //        return TraceHelper.RecordReceive(Settings, Key, CallType, async () =>
    //        {
    //            var v = await Command.GetAll(Settings.Db, Key, commandFlags).ForAwait();
    //            var r = v.ToDictionary(x => x.Key, x => Settings.ValueConverter.Deserialize<T>(x.Value));
    //            return r;
    //        });
    //    }

    //    /// <summary>
    //    /// HKEYS http://redis.io/commands/hkeys
    //    /// </summary>
    //    public Task<string[]> GetKeys(CommandFlags commandFlags = CommandFlags.None)
    //    {
    //        return TraceHelper.RecordReceive(Settings, Key, CallType, () =>
    //        {
    //            return Command.GetKeys(Settings.Db, Key, commandFlags);
    //        });
    //    }

    //    /// <summary>
    //    /// HLEN http://redis.io/commands/hlen
    //    /// </summary>
    //    public Task<long> GetLength(CommandFlags commandFlags = CommandFlags.None)
    //    {
    //        return TraceHelper.RecordReceive(Settings, Key, CallType, () =>
    //        {
    //            return Command.GetLength(Settings.Db, Key, commandFlags);
    //        });
    //    }

    //    /// <summary>
    //    /// HVALS http://redis.io/commands/hvals
    //    /// </summary>
    //    public Task<T[]> GetValues<T>(CommandFlags commandFlags = CommandFlags.None)
    //    {
    //        return TraceHelper.RecordReceive(Settings, Key, CallType, async () =>
    //        {
    //            var v = await Command.GetValues(Settings.Db, Key, commandFlags).ForAwait();
    //            return v.Select(Settings.ValueConverter.Deserialize<T>).ToArray();
    //        });
    //    }

    //    /// <summary>
    //    /// HINCRBY http://redis.io/commands/hincrby
    //    /// </summary>
    //    public Task<long> Increment(string field, int value = 1, CommandFlags commandFlags = CommandFlags.None)
    //    {
    //        return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
    //        {
    //            var r = await Command.Increment(Settings.Db, Key, field, value, commandFlags).ForAwait();
    //            return Pair.Create(new { field, value }, r);
    //        });
    //    }

    //    /// <summary>
    //    /// HINCRBYFLOAT http://redis.io/commands/hincrbyfloat
    //    /// </summary>
    //    public Task<double> Increment(string field, double value, CommandFlags commandFlags = CommandFlags.None)
    //    {
    //        return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
    //        {
    //            var r = await Command.Increment(Settings.Db, Key, field, value, commandFlags).ForAwait();
    //            return Pair.Create(new { field, value }, r);
    //        });
    //    }

    //    public Task<long> IncrementLimitByMax(string field, int value, int max, CommandFlags commandFlags = CommandFlags.None)
    //    {
    //        return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
    //        {
    //            var v = Connection.Scripting.Eval(Settings.Db, HashScript.IncrementLimitByMax, new[] { Key, field }, new object[] { value, max }, useCache: true, inferStrings: true, commandFlags);
    //            var r = (long)(await v.ConfigureAwait(false));
    //            return Pair.Create(new { field, value, max }, r);
    //        });
    //    }

    //    public Task<double> IncrementLimitByMax(string field, double value, double max, CommandFlags commandFlags = CommandFlags.None)
    //    {
    //        return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
    //        {
    //            var v = Connection.Scripting.Eval(Settings.Db, HashScript.IncrementFloatLimitByMax, new[] { Key, field }, new object[] { value, max }, useCache: true, inferStrings: true, commandFlags);
    //            var r = double.Parse((string)(await v.ConfigureAwait(false)));
    //            return Pair.Create(new { field, value, max }, r);
    //        });
    //    }

    //    public Task<long> IncrementLimitByMin(string field, int value, int min, CommandFlags commandFlags = CommandFlags.None)
    //    {
    //        return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
    //        {
    //            var v = Connection.Scripting.Eval(Settings.Db, HashScript.IncrementLimitByMin, new[] { Key, field }, new object[] { value, min }, useCache: true, inferStrings: true, commandFlags);
    //            var r = (long)(await v.ConfigureAwait(false));
    //            return Pair.Create(new { field, value, min }, r);
    //        });
    //    }

    //    public Task<double> IncrementLimitByMin(string field, double value, double min, CommandFlags commandFlags = CommandFlags.None)
    //    {
    //        return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
    //        {
    //            var v = Connection.Scripting.Eval(Settings.Db, HashScript.IncrementFloatLimitByMin, new[] { Key, field }, new object[] { value, min }, useCache: true, inferStrings: true, commandFlags);
    //            var r = double.Parse((string)(await v.ConfigureAwait(false)));
    //            return Pair.Create(new { field, value, min }, r);
    //        });
    //    }

    //    /// <summary>
    //    /// HDEL http://redis.io/commands/hdel
    //    /// </summary>
    //    public Task<bool> Remove(string field, CommandFlags commandFlags = CommandFlags.None)
    //    {
    //        return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
    //        {
    //            var r = await Command.Remove(Settings.Db, Key, field, commandFlags);
    //            return Pair.Create(new { field }, r);
    //        });
    //    }
    //    /// <summary>
    //    /// HDEL http://redis.io/commands/hdel
    //    /// </summary>
    //    public Task<long> Remove(string[] fields, CommandFlags commandFlags = CommandFlags.None)
    //    {
    //        return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
    //        {
    //            var r = await Command.Remove(Settings.Db, Key, fields, commandFlags);
    //            return Pair.Create(new { fields }, r);
    //        });
    //    }

    //    /// <summary>
    //    /// HMSET http://redis.io/commands/hmset
    //    /// </summary>
    //    public Task Set(Dictionary<string, object> values, CommandFlags commandFlags = CommandFlags.None)
    //    {
    //        return TraceHelper.RecordSend(Settings, Key, CallType, async () =>
    //        {
    //            var v = values.ToDictionary(x => x.Key, x => Settings.ValueConverter.Serialize(x.Value));
    //            await Command.Set(Settings.Db, Key, v, commandFlags);
    //            return new { values };
    //        });
    //    }

    //    /// <summary>
    //    /// HSET http://redis.io/commands/hset
    //    /// </summary>
    //    public Task<bool> Set(string field, object value, CommandFlags commandFlags = CommandFlags.None)
    //    {
    //        return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
    //        {
    //            var r = await Command.Set(Settings.Db, Key, field, Settings.ValueConverter.Serialize(value), commandFlags);
    //            return Pair.Create(new { field, value }, r);
    //        });
    //    }

    //    /// <summary>
    //    /// HSETNX http://redis.io/commands/hsetnx
    //    /// </summary>
    //    public Task<bool> SetIfNotExists(string field, object value, CommandFlags commandFlags = CommandFlags.None)
    //    {
    //        return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
    //        {
    //            var r = await Command.SetIfNotExists(Settings.Db, Key, field, Settings.ValueConverter.Serialize(value), commandFlags);
    //            return Pair.Create(new { field, value }, r);
    //        });
    //    }

    //    /// <summary>
    //    /// expire subtract Datetime.Now
    //    /// </summary>
    //    public Task<bool> SetExpire(DateTime expire, CommandFlags commandFlags = CommandFlags.None)
    //    {
    //        return SetExpire(expire - DateTime.Now, commandFlags);
    //    }

    //    public Task<bool> SetExpire(TimeSpan expire, CommandFlags commandFlags = CommandFlags.None)
    //    {
    //        return SetExpire((int)expire.TotalSeconds, commandFlags);
    //    }

    //    public Task<bool> SetExpire(int seconds, CommandFlags commandFlags = CommandFlags.None)
    //    {
    //        return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
    //        {
    //            var r = await Connection.Keys.Expire(Settings.Db, Key, seconds, commandFlags).ForAwait();
    //            return Pair.Create(new { seconds }, r);
    //        });
    //    }

    //    public Task<bool> KeyExists(CommandFlags commandFlags = CommandFlags.None)
    //    {
    //        return TraceHelper.RecordReceive(Settings, Key, CallType, () =>
    //        {
    //            return Connection.Keys.Exists(Settings.Db, Key, commandFlags);
    //        });
    //    }

    //    public Task<bool> Clear(CommandFlags commandFlags = CommandFlags.None)
    //    {
    //        return TraceHelper.RecordReceive(Settings, Key, CallType, () =>
    //        {
    //            return Connection.Keys.Remove(Settings.Db, Key, commandFlags);
    //        });
    //    }
    //}

    /// <summary>
    /// Class mapped RedisHash
    /// </summary>
    //public class RedisClass<T> where T : class, new()
    //{
    //    const string CallType = "RedisClass";

    //    public string Key { get; private set; }
    //    public RedisSettings Settings { get; private set; }

    //    public RedisClass(RedisSettings settings, string hashKey)
    //    {
    //        this.Settings = settings;
    //        this.Key = hashKey;
    //    }

    //    public RedisClass(RedisGroup connectionGroup, string hashKey)
    //        : this(connectionGroup.GetSettings(hashKey), hashKey)
    //    {
    //    }

    //    protected RedisConnection Connection
    //    {
    //        get
    //        {
    //            return Settings.GetConnection();
    //        }
    //    }

    //    protected IHashCommands Command
    //    {
    //        get
    //        {
    //            return Connection.Hashes;
    //        }
    //    }

    //    public Task<T> GetValue(CommandFlags commandFlags = CommandFlags.None)
    //    {
    //        return TraceHelper.RecordReceive(Settings, Key, CallType, async () =>
    //        {
    //            var data = await Command.GetAll(Settings.Db, Key, commandFlags).ForAwait();
    //            if (data == null || data.Count == 0)
    //            {
    //                return null;
    //            }

    //            var accessor = FastMember.TypeAccessor.Create(typeof(T), allowNonPublicAccessors: false);
    //            var result = (T)accessor.CreateNew();

    //            foreach (var member in accessor.GetMembers())
    //            {
    //                byte[] value;
    //                if (data.TryGetValue(member.Name, out value))
    //                {
    //                    accessor[result, member.Name] = Settings.ValueConverter.Deserialize(member.Type, value);
    //                }
    //            }

    //            return result;
    //        });
    //    }

    //    /// <summary>
    //    /// expire subtract Datetime.Now
    //    /// </summary>
    //    public Task<T> GetValueOrSet(Func<T> valueFactory, DateTime expire, bool configureAwait = true, CommandFlags commandFlags = CommandFlags.None)
    //    {
    //        return GetValueOrSet(valueFactory, expire - DateTime.Now, configureAwait, commandFlags);
    //    }

    //    public Task<T> GetValueOrSet(Func<T> valueFactory, TimeSpan expire, bool configureAwait = true, CommandFlags commandFlags = CommandFlags.None)
    //    {
    //        return GetValueOrSet(valueFactory, (int)expire.TotalSeconds, configureAwait, commandFlags);
    //    }

    //    public async Task<T> GetValueOrSet(Func<T> valueFactory, int? expirySeconds = null, bool configureAwait = true, CommandFlags commandFlags = CommandFlags.None)
    //    {
    //        var value = await GetValue(commandFlags).ConfigureAwait(configureAwait); // keep valueFactory synchronization context
    //        if (value == null)
    //        {
    //            value = valueFactory();
    //            if (expirySeconds != null)
    //            {
    //                var a = SetValue(value);
    //                var b = SetExpire(expirySeconds.Value, commandFlags);
    //                await Task.WhenAll(a, b).ForAwait();
    //            }
    //            else
    //            {
    //                await SetValue(value).ForAwait();
    //            }
    //        }

    //        return value;
    //    }

    //    /// <summary>
    //    /// expire subtract Datetime.Now
    //    /// </summary>
    //    public Task<T> GetValueOrSet(Func<Task<T>> valueFactory, DateTime expire, bool configureAwait = true, CommandFlags commandFlags = CommandFlags.None)
    //    {
    //        return GetValueOrSet(valueFactory, expire - DateTime.Now, configureAwait, commandFlags);
    //    }

    //    public Task<T> GetValueOrSet(Func<Task<T>> valueFactory, TimeSpan expire, bool configureAwait = true, CommandFlags commandFlags = CommandFlags.None)
    //    {
    //        return GetValueOrSet(valueFactory, (int)expire.TotalSeconds, configureAwait, commandFlags);
    //    }

    //    public async Task<T> GetValueOrSet(Func<Task<T>> valueFactory, int? expirySeconds = null, bool configureAwait = true, CommandFlags commandFlags = CommandFlags.None)
    //    {
    //        var value = await GetValue(commandFlags).ConfigureAwait(configureAwait); // keep valueFactory synchronization context
    //        if (value == null)
    //        {
    //            value = await valueFactory().ConfigureAwait(configureAwait);
    //            if (expirySeconds != null)
    //            {
    //                var a = SetValue(value);
    //                var b = SetExpire(expirySeconds.Value, commandFlags);
    //                await Task.WhenAll(a, b).ForAwait();
    //            }
    //            else
    //            {
    //                await SetValue(value).ForAwait();
    //            }
    //        }

    //        return value;
    //    }

    //    public Task SetValue(T value, CommandFlags commandFlags = CommandFlags.None)
    //    {
    //        return TraceHelper.RecordSend(Settings, Key, CallType, async () =>
    //        {
    //            var accessor = FastMember.TypeAccessor.Create(typeof(T), allowNonPublicAccessors: false);
    //            var members = accessor.GetMembers();
    //            var values = new Dictionary<string, byte[]>(members.Count);
    //            foreach (var member in members)
    //            {
    //                values.Add(member.Name, Settings.ValueConverter.Serialize(accessor[value, member.Name]));
    //            }

    //            await Command.Set(Settings.Db, Key, values, commandFlags).ForAwait();

    //            return new { value };
    //        });
    //    }

    //    public Task<bool> SetField(string field, object value, CommandFlags commandFlags = CommandFlags.None)
    //    {
    //        return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
    //        {
    //            var r = await Command.Set(Settings.Db, Key, field, Settings.ValueConverter.Serialize(value), commandFlags).ForAwait();
    //            return Pair.Create(new { field, value }, r);
    //        });
    //    }

    //    public Task SetFields(IEnumerable<KeyValuePair<string, object>> fields, CommandFlags commandFlags = CommandFlags.None)
    //    {
    //        return TraceHelper.RecordSend(Settings, Key, CallType, async () =>
    //        {
    //            var values = fields.ToDictionary(x => x.Key, x => Settings.ValueConverter.Serialize(x.Value));
    //            await Command.Set(Settings.Db, Key, values, commandFlags).ForAwait();

    //            return new { fields };
    //        });
    //    }

    //    public Task<TField> GetField<TField>(string field, CommandFlags commandFlags = CommandFlags.None)
    //    {
    //        return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
    //        {
    //            var v = await Command.Get(Settings.Db, Key, field, commandFlags).ForAwait();
    //            var r = Settings.ValueConverter.Deserialize<TField>(v);
    //            return Pair.Create(new { field }, r);
    //        });
    //    }

    //    public Task<long> Increment(string field, int value = 1, CommandFlags commandFlags = CommandFlags.None)
    //    {
    //        return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
    //        {
    //            var r = await Command.Increment(Settings.Db, Key, field, value, commandFlags).ForAwait();
    //            return Pair.Create(new { field, value }, r);
    //        });
    //    }

    //    public Task<double> Increment(string field, double value, CommandFlags commandFlags = CommandFlags.None)
    //    {
    //        return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
    //        {
    //            var r = await Command.Increment(Settings.Db, Key, field, value, commandFlags).ForAwait();
    //            return Pair.Create(new { field, value }, r);
    //        });
    //    }

    //    public Task<long> IncrementLimitByMax(string field, int value, int max, CommandFlags commandFlags = CommandFlags.None)
    //    {
    //        return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
    //        {
    //            var v = Connection.Scripting.Eval(Settings.Db, HashScript.IncrementLimitByMax, new[] { Key, field }, new object[] { value, max }, useCache: true, inferStrings: true, commandFlags);
    //            var r = (long)(await v.ConfigureAwait(false));
    //            return Pair.Create(new { field, value, max }, r);
    //        });
    //    }

    //    public Task<double> IncrementLimitByMax(string field, double value, double max, CommandFlags commandFlags = CommandFlags.None)
    //    {
    //        return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
    //        {
    //            var v = Connection.Scripting.Eval(Settings.Db, HashScript.IncrementFloatLimitByMax, new[] { Key, field }, new object[] { value, max }, useCache: true, inferStrings: true, commandFlags);
    //            var r = double.Parse((string)(await v.ConfigureAwait(false)));
    //            return Pair.Create(new { field, value, max }, r);
    //        });
    //    }

    //    public Task<long> IncrementLimitByMin(string field, int value, int min, CommandFlags commandFlags = CommandFlags.None)
    //    {
    //        return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
    //        {
    //            var v = Connection.Scripting.Eval(Settings.Db, HashScript.IncrementLimitByMin, new[] { Key, field }, new object[] { value, min }, useCache: true, inferStrings: true, commandFlags);
    //            var r = (long)(await v.ConfigureAwait(false));

    //            return Pair.Create(new { field, value, min }, r);
    //        });
    //    }

    //    public Task<double> IncrementLimitByMin(string field, double value, double min, CommandFlags commandFlags = CommandFlags.None)
    //    {
    //        return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
    //        {
    //            var v = Connection.Scripting.Eval(Settings.Db, HashScript.IncrementFloatLimitByMin, new[] { Key, field }, new object[] { value, min }, useCache: true, inferStrings: true, commandFlags);
    //            var r = double.Parse((string)(await v.ConfigureAwait(false)));
    //            return Pair.Create(new { field, value, min }, r);
    //        });
    //    }
    //}
}