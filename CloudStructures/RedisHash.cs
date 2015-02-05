using StackExchange.Redis;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
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

    public abstract class RedisHashBase<TKey> : RedisStructure
    {
        public RedisHashBase(RedisSettings settings, RedisKey hashKey)
            : base(settings, hashKey)
        {
        }

        public RedisHashBase(RedisGroup connectionGroup, RedisKey hashKey)
            : base(connectionGroup, hashKey)
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
    }

    public class RedisDictionary<TKey, TValue> : RedisHashBase<TKey>
    {
        protected override string CallType
        {
            get { return "RedisDictionary"; }
        }

        public RedisDictionary(RedisSettings settings, RedisKey hashKey)
            : base(settings, hashKey)
        {
        }

        public RedisDictionary(RedisGroup connectionGroup, RedisKey hashKey)
            : base(connectionGroup, hashKey)
        {
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
        /// HSET, HSETNX http://redis.io/commands/hset http://redis.io/commands/hsetnx
        /// </summary>
        public Task<bool> Set(TKey field, TValue value, RedisExpiry expiry = null, When when = When.Always, CommandFlags commandFlags = CommandFlags.None)
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

    public class RedisHash<TKey> : RedisHashBase<TKey>
    {
        protected override string CallType
        {
            get { return "RedisHash"; }
        }

        public RedisHash(RedisSettings settings, RedisKey hashKey)
            : base(settings, hashKey)
        {
        }

        public RedisHash(RedisGroup connectionGroup, RedisKey hashKey)
            : base(connectionGroup, hashKey)
        {
        }

        /// <summary>
        /// HGET http://redis.io/commands/hget
        /// </summary>
        public Task<RedisResult<TValue>> Get<TValue>(TKey field, CommandFlags commandFlags = CommandFlags.None)
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
        public Task<Dictionary<TKey, TValue>> Get<TValue>(TKey[] fields, IEqualityComparer<TKey> dictionaryEqualityComparer = null, CommandFlags commandFlags = CommandFlags.None)
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
        /// HSET, HSETNX http://redis.io/commands/hset http://redis.io/commands/hsetnx
        /// </summary>
        public Task<bool> Set<TValue>(TKey field, TValue value, RedisExpiry expiry = null, When when = When.Always, CommandFlags commandFlags = CommandFlags.None)
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
        public Task Set<TValue>(IEnumerable<KeyValuePair<TKey, TValue>> values, RedisExpiry expiry = null, CommandFlags commandFlags = CommandFlags.None)
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

    /// <summary>
    /// RedisClass is type mapped RedisHash.
    /// </summary>
    public class RedisClass<T> : RedisStructure where T : class, new()
    {
        protected override string CallType
        {
            get { return "RedisClass"; }
        }

        public RedisClass(RedisSettings settings, RedisKey hashKey)
            : base(settings, hashKey)
        {
        }

        public RedisClass(RedisGroup connectionGroup, RedisKey hashKey)
            : base(connectionGroup, hashKey)
        {
        }

        /// <summary>
        /// All hash value map to class if key can't find returns null, includes HGETALL.
        /// </summary>
        public Task<T> Get(CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordReceive(Settings, Key, CallType, async () =>
            {
                var data = await Command.HashGetAllAsync(Key, commandFlags).ForAwait();
                if (data == null || data.Length == 0)
                {
                    return null;
                }

                var accessor = TypeAccessor.Lookup(typeof(T));
                var result = (T)accessor.CreateNew();
                long resultSize = 0L;
                foreach (var item in data)
                {
                    IMemberAccessor memberAccessor;
                    if (accessor.TryGetValue(item.Name, out memberAccessor) && memberAccessor.IsReadable && memberAccessor.IsWritable)
                    {
                        long s;
                        var v = Settings.ValueConverter.Deserialize(memberAccessor.MemberType, item.Value, out s);
                        resultSize += s;
                        memberAccessor.SetValue(result, v);
                    }
                    else
                    {
                        var buf = (byte[])item.Value;
                        resultSize += buf.Length;
                    }
                }

                return Tracing.CreateReceived(result, resultSize);
            });
        }

        /// <summary>
        /// Class fields set to hash, includes HMSET. If return value is null value == null.
        /// </summary>
        public Task<bool> Set(T value, RedisExpiry expiry = null, CommandFlags commandFlags = CommandFlags.None)
        {
            if (value == null) return Task.FromResult(false);

            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var accessor = TypeAccessor.Lookup(typeof(T));
                long sentSize = 0;
                var hashFields = accessor.Where(kvp => kvp.Value.IsReadable && kvp.Value.IsWritable)
                    .Select(kvp =>
                    {
                        var field = kvp.Value.GetValue(value);
                        long s;
                        var rv = Settings.ValueConverter.Serialize(field, out s);
                        sentSize += s;
                        return new HashEntry(kvp.Key, rv);
                    })
                    .ToArray();

                await this.ExecuteWithKeyExpire(x => x.HashSetAsync(Key, hashFields), Key, expiry, commandFlags).ForAwait();

                return Tracing.CreateSentAndReceived(new { value }, sentSize, true, 0);
            });
        }

        /// <summary>
        /// GET(HGETALL), SET(HMSET)
        /// </summary>
        public async Task<T> GetOrSet(Func<T> valueFactory, RedisExpiry expiry = null, bool keepValueFactorySynchronizationContext = false, CommandFlags commandFlags = CommandFlags.None)
        {
            var value = await Get(commandFlags).ConfigureAwait(keepValueFactorySynchronizationContext);
            if (value == null)
            {
                value = valueFactory();
                await Set(value, expiry, commandFlags).ForAwait();
            }

            return value;
        }

        /// <summary>
        /// GET(HGETALL), SET(HMSET)
        /// </summary>
        public async Task<T> GetOrSet(Func<Task<T>> valueFactory, RedisExpiry expiry = null, bool keepValueFactorySynchronizationContext = false, CommandFlags commandFlags = CommandFlags.None)
        {
            var value = await Get(commandFlags).ConfigureAwait(keepValueFactorySynchronizationContext);
            if (value == null)
            {
                value = await valueFactory().ForAwait();
                await Set(value, expiry, commandFlags).ForAwait();
            }

            return value;
        }

        /// <summary>
        /// HGET http://redis.io/commands/hget
        /// </summary>
        public Task<RedisResult<TValue>> GetMember<TValue>(Expression<Func<T, TValue>> memberSelector, CommandFlags commandFlags = CommandFlags.None)
        {
            var memberExpr = memberSelector.Body as MemberExpression;
            if (memberExpr == null) throw new ArgumentException("can't analyze selector expression");

            return GetMember<TValue>(memberExpr.Member.Name, commandFlags);
        }

        /// <summary>
        /// HGET http://redis.io/commands/hget
        /// </summary>
        public Task<RedisResult<TValue>> GetMember<TValue>(string memberName, CommandFlags commandFlags = CommandFlags.None)
        {
            if (!TypeAccessor.Lookup(typeof(T)).ContainsKey(memberName)) return Task.FromResult(new RedisResult<TValue>());

            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var rValue = await Command.HashGetAsync(Key, memberName, commandFlags).ForAwait();
                long valueSize;
                var value = RedisResult.FromRedisValue<TValue>(rValue, Settings, out valueSize);
                return Tracing.CreateSentAndReceived(new { memberName }, 0, value, valueSize);
            });
        }

        /// <summary>
        /// HMGET http://redis.io/commands/hmget
        /// </summary>
        public Task<Dictionary<string, TValue>> GetMembers<TValue>(Expression<Func<T, TValue[]>> memberSelector, CommandFlags commandFlags = CommandFlags.None)
        {
            var newArrayExpr = memberSelector.Body as NewArrayExpression;
            if (newArrayExpr == null) throw new ArgumentException("can't analyze selector expression");
            var fields = newArrayExpr.Expressions.OfType<MemberExpression>().Select(x => x.Member.Name).ToArray();

            return GetMembers<TValue>(fields, commandFlags);
        }

        /// <summary>
        /// HMGET http://redis.io/commands/hmget
        /// </summary>
        public Task<Dictionary<string, TValue>> GetMembers<TValue>(string[] memberNames, CommandFlags commandFlags = CommandFlags.None)
        {
            var accesssor = TypeAccessor.Lookup(typeof(T));
            memberNames = memberNames.Where(x => accesssor.ContainsKey(x)).ToArray();
            if (memberNames.Length == 0) return Task.FromResult(new Dictionary<string, TValue>());

            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var hashFields = memberNames.Select(x => (RedisValue)x).ToArray();

                var rValues = await Command.HashGetAsync(Key, hashFields, commandFlags).ForAwait();

                long valueSize = 0;
                var result = memberNames
                    .Zip(rValues, (key, x) =>
                    {
                        if (!x.HasValue) return new { key, rValue = default(TValue), x.HasValue };

                        long s;
                        var rValue = Settings.ValueConverter.Deserialize<TValue>(x, out s);
                        valueSize += s;
                        return new { key, rValue, x.HasValue };
                    })
                    .Where(x => x.HasValue)
                    .ToDictionary(x => x.key, x => x.rValue);

                return Tracing.CreateSentAndReceived(new { memberNames }, 0, result, valueSize);
            });
        }

        /// <summary>
        /// HSET, HSETNX http://redis.io/commands/hset http://redis.io/commands/hsetnx
        /// </summary>
        public Task<bool> SetMember<TValue>(Expression<Func<T, TValue>> memberSelector, TValue value, RedisExpiry expiry = null, CommandFlags commandFlags = CommandFlags.None)
        {
            var memberExpr = memberSelector.Body as MemberExpression;
            if (memberExpr == null) throw new ArgumentException("can't analyze selector expression");

            return SetMember<TValue>(memberExpr.Member.Name, value, expiry, commandFlags);
        }

        /// <summary>
        /// HSET, HSETNX http://redis.io/commands/hset http://redis.io/commands/hsetnx
        /// </summary>
        public Task<bool> SetMember<TValue>(string memberName, TValue value, RedisExpiry expiry = null, CommandFlags commandFlags = CommandFlags.None)
        {
            if (!TypeAccessor.Lookup(typeof(T)).ContainsKey(memberName)) return Task.FromResult(false);

            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                RedisValue rKey = memberName;
                long valueSize;
                var rValue = Settings.ValueConverter.Serialize(value, out valueSize);

                var result = await this.ExecuteWithKeyExpire(x => x.HashSetAsync(Key, rKey, rValue, flags: commandFlags), Key, expiry, commandFlags).ForAwait();

                return Tracing.CreateSentAndReceived(new { memberName, value }, valueSize, true, sizeof(bool));
            });
        }

        /// <summary>
        /// HMSET http://redis.io/commands/hmset
        /// </summary>
        public Task SetMembers<TValue>(Expression<Func<T, TValue[]>> memberSelector, TValue[] values, RedisExpiry expiry = null, CommandFlags commandFlags = CommandFlags.None)
        {
            var newArrayExpr = memberSelector.Body as NewArrayExpression;
            if (newArrayExpr == null) throw new ArgumentException("can't analyze selector expression");
            var fields = newArrayExpr.Expressions.OfType<MemberExpression>().Select(x => x.Member.Name).ToArray();

            if (fields.Length != values.Length) throw new ArgumentException($"member and value's count is mismatch - memberSelector.Length:{fields.Length}, values.Length:{values.Length}");

            var pairs = fields.Zip(values, (key, value) => new KeyValuePair<string, TValue>(key, value)).ToArray();
            return SetMembers<TValue>(pairs, expiry, commandFlags);
        }

        /// <summary>
        /// HMSET http://redis.io/commands/hmset
        /// </summary>
        public Task SetMembers<TValue>(IEnumerable<KeyValuePair<string, TValue>> values, RedisExpiry expiry = null, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSend(Settings, Key, CallType, async () =>
            {
                if (!(values is ICollection))
                {
                    values = values.ToArray(); // materialize
                }

                long valueSize = 0;
                var hashFields = values.Select(x =>
                {
                    long vs;
                    RedisValue rKey = x.Key;
                    var rValue = Settings.ValueConverter.Serialize(x.Value, out vs);
                    valueSize += vs;
                    return new HashEntry(rKey, rValue);
                }).ToArray();

                await this.ExecuteWithKeyExpire(x => x.HashSetAsync(Key, hashFields, commandFlags), Key, expiry, commandFlags).ForAwait();

                return Tracing.CreateSent(new { values }, valueSize);
            });
        }

        /// <summary>
        /// HINCRBY http://redis.io/commands/hincrby
        /// </summary>
        public Task<long> Increment(Expression<Func<T, long>> memberSelector, long value = 1, RedisExpiry expiry = null, CommandFlags commandFlags = CommandFlags.None)
        {
            return Increment(GetMemberName(memberSelector), value, expiry, commandFlags);
        }

        /// <summary>
        /// HINCRBY http://redis.io/commands/hincrby
        /// </summary>
        public Task<long> Increment(string member, long value = 1, RedisExpiry expiry = null, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var rKey = member;
                var r = await this.ExecuteWithKeyExpire(x => x.HashIncrementAsync(Key, rKey, value, commandFlags), Key, expiry, commandFlags).ForAwait();
                return Tracing.CreateSentAndReceived(new { member, value }, 0, r, sizeof(long));
            });
        }

        /// <summary>
        /// HINCRBYFLOAT http://redis.io/commands/hincrbyfloat
        /// </summary>
        public Task<double> Increment(Expression<Func<T, double>> memberSelector, double value, RedisExpiry expiry = null, CommandFlags commandFlags = CommandFlags.None)
        {
            return Increment(GetMemberName(memberSelector), value, expiry, commandFlags);
        }

        /// <summary>
        /// HINCRBYFLOAT http://redis.io/commands/hincrbyfloat
        /// </summary>
        public Task<double> Increment(string member, double value, RedisExpiry expiry = null, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var rKey = member;
                var r = await this.ExecuteWithKeyExpire(x => x.HashIncrementAsync(Key, rKey, value, commandFlags), Key, expiry, commandFlags).ForAwait();
                return Tracing.CreateSentAndReceived(new { member, value }, 0, r, sizeof(long));
            });
        }

        /// <summary>
        /// LUA Script including hincrby, hset
        /// </summary>
        public Task<long> IncrementLimitByMax(Expression<Func<T, long>> memberSelector, long value, long max, RedisExpiry expiry = null, CommandFlags commandFlags = CommandFlags.None)
        {
            return IncrementLimitByMax(GetMemberName(memberSelector), value, max, expiry, commandFlags);
        }

        /// <summary>
        /// LUA Script including hincrby, hset
        /// </summary>
        public Task<long> IncrementLimitByMax(string member, long value, long max, RedisExpiry expiry = null, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var rKey = (RedisKey)member;
                var v = await this.ExecuteWithKeyExpire(x => x.ScriptEvaluateAsync(HashScript.IncrementLimitByMax, new[] { Key, rKey }, new RedisValue[] { value, max }, commandFlags), Key, expiry, commandFlags).ForAwait();
                var r = (long)v;
                return Tracing.CreateSentAndReceived(new { member, value, max, expiry = expiry?.Value }, sizeof(long) * 2, r, sizeof(long));
            });
        }

        /// <summary>
        /// LUA Script including hincrbyfloat, hset
        /// </summary>
        public Task<double> IncrementLimitByMax(Expression<Func<T, double>> memberSelector, double value, double max, RedisExpiry expiry = null, CommandFlags commandFlags = CommandFlags.None)
        {
            return IncrementLimitByMax(GetMemberName(memberSelector), value, max, expiry, commandFlags);
        }

        /// <summary>
        /// LUA Script including hincrbyfloat, hset
        /// </summary>
        public Task<double> IncrementLimitByMax(string member, double value, double max, RedisExpiry expiry = null, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var rKey = (RedisKey)member;
                var v = await this.ExecuteWithKeyExpire(x => x.ScriptEvaluateAsync(HashScript.IncrementFloatLimitByMax, new[] { Key, rKey }, new RedisValue[] { value, max }, commandFlags), Key, expiry, commandFlags).ForAwait();
                var r = (double)v;
                return Tracing.CreateSentAndReceived(new { member, value, max, expiry = expiry?.Value }, sizeof(double) * 2, r, sizeof(double));
            });
        }

        /// <summary>
        /// LUA Script including hincrby, hset
        /// </summary>
        public Task<long> IncrementLimitByMin(Expression<Func<T, long>> memberSelector, long value, long max, RedisExpiry expiry = null, CommandFlags commandFlags = CommandFlags.None)
        {
            return IncrementLimitByMin(GetMemberName(memberSelector), value, max, expiry, commandFlags);
        }

        /// <summary>
        /// LUA Script including hincrby, hset
        /// </summary>
        public Task<long> IncrementLimitByMin(string member, long value, long max, RedisExpiry expiry = null, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var rKey = (RedisKey)member;
                var v = await this.ExecuteWithKeyExpire(x => x.ScriptEvaluateAsync(HashScript.IncrementLimitByMin, new[] { Key, rKey }, new RedisValue[] { value, max }, commandFlags), Key, expiry, commandFlags).ForAwait();
                var r = (long)v;
                return Tracing.CreateSentAndReceived(new { member, value, max, expiry = expiry?.Value }, sizeof(long) * 2, r, sizeof(long));
            });
        }

        /// <summary>
        /// LUA Script including hincrbyfloat, hset
        /// </summary>
        public Task<double> IncrementLimitByMin(Expression<Func<T, double>> memberSelector, double value, double max, RedisExpiry expiry = null, CommandFlags commandFlags = CommandFlags.None)
        {
            return IncrementLimitByMin(GetMemberName(memberSelector), value, max, expiry, commandFlags);
        }

        /// <summary>
        /// LUA Script including hincrbyfloat, hset
        /// </summary>
        public Task<double> IncrementLimitByMin(string member, double value, double max, RedisExpiry expiry = null, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var rKey = (RedisKey)member;
                var v = await this.ExecuteWithKeyExpire(x => x.ScriptEvaluateAsync(HashScript.IncrementFloatLimitByMin, new[] { Key, rKey }, new RedisValue[] { value, max }, commandFlags), Key, expiry, commandFlags).ForAwait();
                var r = (double)v;
                return Tracing.CreateSentAndReceived(new { member, value, max, expiry = expiry?.Value }, sizeof(double) * 2, r, sizeof(double));
            });
        }

        string GetMemberName(LambdaExpression memberSelector)
        {
            var unary = memberSelector.Body as UnaryExpression;
            var memberExpr = (unary != null)
                ? unary.Operand as MemberExpression
                : memberSelector.Body as MemberExpression;
            if (memberExpr == null) throw new ArgumentException("can't analyze selector expression");

            return memberExpr.Member.Name;
        }
    }
}