using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CloudStructures.Internals;
using StackExchange.Redis;



namespace CloudStructures.Structures
{
    /// <summary>
    /// Provides dictionary related commands.
    /// </summary>
    /// <typeparam name="TKey">Key type</typeparam>
    /// <typeparam name="TValue">Value type</typeparam>
    public readonly struct RedisDictionary<TKey, TValue> : IRedisStructureWithExpiry
    {
        #region IRedisStructureWithExpiry implementations
        /// <summary>
        /// Gets connection.
        /// </summary>
        public RedisConnection Connection { get; }


        /// <summary>
        /// Gets key.
        /// </summary>
        public RedisKey Key { get; }


        /// <summary>
        /// Gets default expiration time.
        /// </summary>
        public TimeSpan? DefaultExpiry { get; }
        #endregion


        #region Constructors
        /// <summary>
        /// Creates instance.
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="key"></param>
        /// <param name="defaultExpiry"></param>
        public RedisDictionary(RedisConnection connection, in RedisKey key, TimeSpan? defaultExpiry)
        {
            this.Connection = connection ?? throw new ArgumentNullException(nameof(connection));
            this.Key = key;
            this.DefaultExpiry = defaultExpiry;
        }
        #endregion


        #region Commands
        //- [x] HashDecrementAsync
        //- [x] HashDeleteAsync
        //- [x] HashExistsAsync
        //- [x] HashGetAllAsync
        //- [x] HashGetAsync
        //- [x] HashIncrementAsync
        //- [x] HashKeysAsync
        //- [x] HashLengthAsync
        //- [x] HashSetAsync
        //- [x] HashValuesAsync


        /// <summary>
        /// HINCRBY : https://redis.io/commands/hincrby
        /// </summary>
        public Task<long> DecrementAsync(TKey field, long value = 1, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
        {
            expiry ??= this.DefaultExpiry;
            var hashField = this.Connection.Converter.Serialize(field);
            return this.ExecuteWithExpiryAsync
            (
                (db, a) => db.HashDecrementAsync(a.key, a.hashField, a.value, a.flags),
                (key: this.Key, hashField, value, flags),
                expiry,
                flags
            );
        }


        /// <summary>
        /// HINCRBYFLOAT : https://redis.io/commands/hincrbyfloat
        /// </summary>
        public Task DecrementAsync(TKey field, double value, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
        {
            expiry ??= this.DefaultExpiry;
            var hashField = this.Connection.Converter.Serialize(field);
            return this.ExecuteWithExpiryAsync
            (
                (db, a) => db.HashDecrementAsync(a.key, a.hashField, a.value, a.flags),
                (key: this.Key, hashField, value, flags),
                expiry,
                flags
            );
        }

        
        /// <summary>
        /// HDEL : https://redis.io/commands/hdel
        /// </summary>
        public Task<bool> DeleteAsync(TKey field, CommandFlags flags = CommandFlags.None)
        {
            var hashField = this.Connection.Converter.Serialize(field);
            return this.Connection.Database.HashDeleteAsync(this.Key, hashField, flags);
        }


        /// <summary>
        /// HDEL : https://redis.io/commands/hdel
        /// </summary>
        public Task<long> DeleteAsync(IEnumerable<TKey> fields, CommandFlags flags = CommandFlags.None)
        {
            var hashFields = fields.Select(this.Connection.Converter.Serialize).ToArray();
            return this.Connection.Database.HashDeleteAsync(this.Key, hashFields, flags);
        }


        /// <summary>
        /// HEXISTS : https://redis.io/commands/hexists
        /// </summary>
        public Task<bool> ExistsAsync(TKey field, CommandFlags flags = CommandFlags.None)
        {
            var hashField = this.Connection.Converter.Serialize(field);
            return this.Connection.Database.HashExistsAsync(this.Key, hashField, flags);
        }


        /// <summary>
        /// HGETALL : https://redis.io/commands/hgetall
        /// </summary>
        public async Task<Dictionary<TKey, TValue>> GetAllAsync(IEqualityComparer<TKey> dictionaryEqualityComparer = null, CommandFlags flags = CommandFlags.None)
        {
            var comparer = dictionaryEqualityComparer ?? EqualityComparer<TKey>.Default;
            var entries = await this.Connection.Database.HashGetAllAsync(this.Key, flags).ConfigureAwait(false);
            return entries
                .Select(this.Connection.Converter, (x, c) =>
                {
                    var field = c.Deserialize<TKey>(x.Name);
                    var value = c.Deserialize<TValue>(x.Value);
                    return (field, value);
                })
                .ToDictionary(x => x.field, x => x.value, comparer);
        }


        /// <summary>
        /// HGET : https://redis.io/commands/hget
        /// </summary>
        public async Task<RedisResult<TValue>> GetAsync(TKey field, CommandFlags flags = CommandFlags.None)
        {
            var hashField = this.Connection.Converter.Serialize(field);
            var value = await this.Connection.Database.HashGetAsync(this.Key, hashField, flags).ConfigureAwait(false);
            return value.ToResult<TValue>(this.Connection.Converter);
        }


        /// <summary>
        /// HMGET : https://redis.io/commands/hmget
        /// </summary>
        public async Task<Dictionary<TKey, TValue>> GetAsync(IEnumerable<TKey> fields, IEqualityComparer<TKey> dictionaryEqualityComparer = null, CommandFlags flags = CommandFlags.None)
        {
            fields = fields.Materialize(false);
            var comparer = dictionaryEqualityComparer ?? EqualityComparer<TKey>.Default;
            var hashFields = fields.Select(this.Connection.Converter.Serialize).ToArray();
            var values = await this.Connection.Database.HashGetAsync(this.Key, hashFields, flags).ConfigureAwait(false);
            return fields
                .Zip(values, (f, v) => (field: f, value: v))
                .Select(this.Connection.Converter, (x, c) =>
                {
                    var result = x.value.ToResult<TValue>(c);
                    return (x.field, result);
                })
                .Where(x => x.result.HasValue)
                .ToDictionary(x => x.field, x => x.result.Value, comparer);
        }


        /// <summary>
        /// HINCRBY : https://redis.io/commands/hincrby
        /// </summary>
        public Task<long> IncrementAsync(TKey field, long value = 1, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
        {
            expiry ??= this.DefaultExpiry;
            var hashField = this.Connection.Converter.Serialize(field);
            return this.ExecuteWithExpiryAsync
            (
                (db, a) => db.HashIncrementAsync(a.key, a.hashField, a.value, a.flags),
                (key: this.Key, hashField, value, flags),
                expiry,
                flags
            );
        }


        /// <summary>
        /// HINCRBYFLOAT : https://redis.io/commands/hincrbyfloat
        /// </summary>
        public Task IncrementAsync(TKey field, double value, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
        {
            expiry ??= this.DefaultExpiry;
            var hashField = this.Connection.Converter.Serialize(field);
            return this.ExecuteWithExpiryAsync
            (
                (db, a) => db.HashIncrementAsync(a.key, a.hashField, a.value, a.flags),
                (key: this.Key, hashField, value, flags),
                expiry,
                flags
            );
        }


        /// <summary>
        /// HKEYS : https://redis.io/commands/hkeys
        /// </summary>
        public async Task<TKey[]> KeysAsync(CommandFlags flags = CommandFlags.None)
        {
            var keys = await this.Connection.Database.HashKeysAsync(this.Key, flags).ConfigureAwait(false);
            return keys.Select(this.Connection.Converter, (x, c) => c.Deserialize<TKey>(x)).ToArray();
        }

        
        /// <summary>
        /// HLEN : https://redis.io/commands/hlen
        /// </summary>
        public Task<long> LengthAsync(CommandFlags flags = CommandFlags.None)
            => this.Connection.Database.HashLengthAsync(this.Key, flags);


        /// <summary>
        /// HSET : https://redis.io/commands/hset
        /// </summary>
        public Task<bool> SetAsync(TKey field, TValue value, TimeSpan? expiry = null, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            expiry ??= this.DefaultExpiry;
            var f = this.Connection.Converter.Serialize(field);
            var v = this.Connection.Converter.Serialize(value);
            return this.ExecuteWithExpiryAsync
            (
                (db, a) => db.HashSetAsync(a.key, a.f, a.v, a.when, a.flags),
                (key: this.Key, f, v, when, flags),
                expiry,
                flags
            );
        }


        /// <summary>
        /// HMSET : https://redis.io/commands/hmset
        /// </summary>
        public Task SetAsync(IEnumerable<KeyValuePair<TKey, TValue>> entries, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
        {
            expiry ??= this.DefaultExpiry;
            var hashEntries
                = entries
                .Select(this.Connection.Converter, (x, c) =>
                {
                    var field = c.Serialize(x.Key);
                    var value = c.Serialize(x.Value);
                    return new HashEntry(field, value);
                })
                .ToArray();
            return (hashEntries.Length == 0)
                ? Task.CompletedTask
                : this.ExecuteWithExpiryAsync
                (
                    (db, a) => db.HashSetAsync(a.key, a.hashEntries, a.flags),
                    (key: this.Key, hashEntries, flags),
                    expiry,
                    flags
                );
        }


        /// <summary>
        /// HVALS : https://redis.io/commands/hvals
        /// </summary>
        public async Task<TValue[]> ValuesAsync(CommandFlags flags = CommandFlags.None)
        {
            var values = await this.Connection.Database.HashValuesAsync(this.Key, flags).ConfigureAwait(false);
            return values.Select(this.Connection.Converter, (x, c) => c.Deserialize<TValue>(x)).ToArray();
        }
        #endregion


        #region Custom Commands
        /// <summary>
        /// HGET : https://redis.io/commands/hget
        /// HSET : https://redis.io/commands/hset
        /// </summary>
        public async Task<TValue> GetOrSetAsync(TKey field, Func<TKey, TValue> valueFactory, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
        {
            if (valueFactory == null)
                throw new ArgumentNullException(nameof(valueFactory));

            var result = await this.GetAsync(field, flags).ConfigureAwait(false);
            if (result.HasValue)
            {
                return result.Value;
            }
            else
            {
                var newValue = valueFactory(field);
                await this.SetAsync(field, newValue, expiry, When.Always, flags).ConfigureAwait(false);
                return newValue;
            }
        }


        /// <summary>
        /// HGET : https://redis.io/commands/hget
        /// HSET : https://redis.io/commands/hset
        /// </summary>
        public async Task<TValue> GetOrSetAsync(TKey field, Func<TKey, Task<TValue>> valueFactory, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
        {
            if (valueFactory == null)
                throw new ArgumentNullException(nameof(valueFactory));

            var result = await this.GetAsync(field, flags).ConfigureAwait(false);
            if (result.HasValue)
            {
                return result.Value;
            }
            else
            {
                var newValue = await valueFactory(field).ConfigureAwait(false);
                await this.SetAsync(field, newValue, expiry, When.Always, flags).ConfigureAwait(false);
                return newValue;
            }
        }


        /// <summary>
        /// HMGET : https://redis.io/commands/hmget
        /// HMSET : https://redis.io/commands/hmset
        /// </summary>
        public async Task<Dictionary<TKey, TValue>> GetOrSetAsync(IEnumerable<TKey> fields, Func<IEnumerable<TKey>, IEnumerable<KeyValuePair<TKey, TValue>>> valueFactory, TimeSpan? expiry = null, IEqualityComparer<TKey> dictionaryEqualityComparer = null, CommandFlags flags = CommandFlags.None)
        {
            if (valueFactory == null)
                throw new ArgumentNullException(nameof(valueFactory));

            var comparer = dictionaryEqualityComparer ?? EqualityComparer<TKey>.Default;
            fields = fields.Materialize(false);
            if (fields.IsEmpty())
                return new Dictionary<TKey, TValue>(comparer);

            //--- get
            var hashFields = fields.Select(this.Connection.Converter.Serialize).ToArray();
            var values = await this.Connection.Database.HashGetAsync(this.Key, hashFields, flags).ConfigureAwait(false);

            //--- divides cached / non cached
            var cached = new Dictionary<TKey, TValue>(comparer);
            var notCached = new LinkedList<TKey>();
            foreach (var x in fields.Zip(values, (f, v) => (f, v)))
            {
                if (x.v.HasValue)
                    cached[x.f] = this.Connection.Converter.Deserialize<TValue>(x.v);
                else
                    notCached.AddLast(x.f);
            }

            //--- load if non cached key exists
            if (notCached.Count > 0)
            {
                var loaded = valueFactory(notCached).Materialize();
                await this.SetAsync(loaded, expiry, flags).ConfigureAwait(false);
                foreach (var x in loaded)
                    cached[x.Key] = x.Value;
            }
            return cached;
        }

        
        /// <summary>
        /// HMGET : https://redis.io/commands/hmget
        /// HMSET : https://redis.io/commands/hmset
        /// </summary>
        public async Task<Dictionary<TKey, TValue>> GetOrSetAsync(IEnumerable<TKey> fields, Func<IEnumerable<TKey>, Task<IEnumerable<KeyValuePair<TKey, TValue>>>> valueFactory, TimeSpan? expiry = null, IEqualityComparer<TKey> dictionaryEqualityComparer = null, CommandFlags flags = CommandFlags.None)
        {
            if (valueFactory == null)
                throw new ArgumentNullException(nameof(valueFactory));

            var comparer = dictionaryEqualityComparer ?? EqualityComparer<TKey>.Default;
            fields = fields.Materialize(false);
            if (fields.IsEmpty())
                return new Dictionary<TKey, TValue>(comparer);

            //--- get
            var hashFields = fields.Select(this.Connection.Converter.Serialize).ToArray();
            var values = await this.Connection.Database.HashGetAsync(this.Key, hashFields, flags).ConfigureAwait(false);

            //--- divides cached / non cached
            var cached = new Dictionary<TKey, TValue>(comparer);
            var notCached = new LinkedList<TKey>();
            foreach (var x in fields.Zip(values, (f, v) => (f, v)))
            {
                if (x.v.HasValue)
                    cached[x.f] = this.Connection.Converter.Deserialize<TValue>(x.v);
                else
                    notCached.AddLast(x.f);
            }

            //--- load if non cached key exists
            if (notCached.Count > 0)
            {
                var loaded = (await valueFactory(notCached).ConfigureAwait(false)).Materialize();
                await this.SetAsync(loaded, expiry, flags).ConfigureAwait(false);
                foreach (var x in loaded)
                    cached[x.Key] = x.Value;
            }
            return cached;
        }


        /// <summary>
        /// HGET : https://redis.io/commands/hget
        /// HDEL : https://redis.io/commands/hdel
        /// </summary>
        public async Task<RedisResult<TValue>> GetAndDeleteAsync(TKey field, CommandFlags flags = CommandFlags.None)
        {
            //--- GetAsync
            var hashField = this.Connection.Converter.Serialize(field);
            var value = await this.Connection.Database.HashGetAsync(this.Key, hashField, flags).ConfigureAwait(false);

            //--- DeleteAsync
            await this.Connection.Database.HashDeleteAsync(this.Key, hashField, flags).ConfigureAwait(false);

            //--- Result
            return value.ToResult<TValue>(this.Connection.Converter);
        }


        /// <summary>
        /// HMGET : https://redis.io/commands/hmget
        /// HDEL : https://redis.io/commands/hdel
        /// </summary>
        public async Task<Dictionary<TKey, TValue>> GetAndDeleteAsync(IEnumerable<TKey> fields, IEqualityComparer<TKey> dictionaryEqualityComparer = null, CommandFlags flags = CommandFlags.None)
        {
            //--- GetAsync
            fields = fields.Materialize(false);
            var comparer = dictionaryEqualityComparer ?? EqualityComparer<TKey>.Default;
            var hashFields = fields.Select(this.Connection.Converter.Serialize).ToArray();
            var values = await this.Connection.Database.HashGetAsync(this.Key, hashFields, flags).ConfigureAwait(false);

            //--- DeleteAsync
            await this.Connection.Database.HashDeleteAsync(this.Key, hashFields, flags).ConfigureAwait(false);

            //--- Result
            return fields
                .Zip(values, (f, v) => (field: f, value: v))
                .Where(x => x.value.HasValue)
                .Select(this.Connection.Converter, (x, c) =>
                {
                    var value = c.Deserialize<TValue>(x.value);
                    return (x.field, value);
                })
                .ToDictionary(x => x.field, x => x.value, comparer);
        }
        #endregion
    }
}
