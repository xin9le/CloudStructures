using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CloudStructures.Internals;
using StackExchange.Redis;



namespace CloudStructures.Structures
{
    /// <summary>
    /// Provides hash set related commands.
    /// Like RedisDictionary&lt;TKey, bool&gt;.
    /// </summary>
    /// <typeparam name="T">Data type</typeparam>
    public readonly struct RedisHashSet<T> : IRedisStructureWithExpiry
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
        public RedisHashSet(RedisConnection connection, in RedisKey key, TimeSpan? defaultExpiry)
        {
            this.Connection = connection ?? throw new ArgumentNullException(nameof(connection));
            this.Key = key;
            this.DefaultExpiry = defaultExpiry;
        }
        #endregion


        #region Commands
        /// <summary>
        /// Deletes specified element.
        /// </summary>
        public Task<bool> DeleteAsync(T value, CommandFlags flags = CommandFlags.None)
        {
            // HDEL
            // https://redis.io/commands/hdel

            var hashField = this.Connection.Converter.Serialize(value);
            return this.Connection.Database.HashDeleteAsync(this.Key, hashField, flags);
        }


        /// <summary>
        /// Deletes specified elements.
        /// </summary>
        public Task<long> DeleteAsync(IEnumerable<T> values, CommandFlags flags = CommandFlags.None)
        {
            // HDEL
            // https://redis.io/commands/hdel

            var hashFields = values.Select(this.Connection.Converter.Serialize).ToArray();
            return this.Connection.Database.HashDeleteAsync(this.Key, hashFields, flags);
        }


        /// <summary>
        /// Checks specified element existence.
        /// </summary>
        public async Task<bool> ContainsAsync(T value, CommandFlags flags = CommandFlags.None)
        {
            // HGET
            // https://redis.io/commands/hget

            var hashField = this.Connection.Converter.Serialize(value);
            var element = await this.Connection.Database.HashGetAsync(this.Key, hashField, flags).ConfigureAwait(false);
            return element.HasValue;
        }


        /// <summary>
        /// Checks specified elements existence.
        /// </summary>
        public async Task<Dictionary<T, bool>> ContainsAsync(IEnumerable<T> values, CommandFlags flags = CommandFlags.None)
        {
            // HMGET
            // https://redis.io/commands/hmget

            values = values.Materialize(false);
            var hashFields = values.Select(this.Connection.Converter.Serialize).ToArray();
            var elements = await this.Connection.Database.HashGetAsync(this.Key, hashFields, flags).ConfigureAwait(false);
            return values
                .Zip(elements, (k, v) => (key: k, value: v))
                .ToDictionary(x => x.key, x => x.value.HasValue);
        }


        /// <summary>
        /// Gets all elements.
        /// </summary>
        public async Task<T[]> ValuesAsync(CommandFlags flags = CommandFlags.None)
        {
            // HKEYS で OK
            // https://redis.io/commands/hkeys

            var elements = await this.Connection.Database.HashKeysAsync(this.Key, flags).ConfigureAwait(false);
            return elements.Select(this.Connection.Converter, (x, c) => c.Deserialize<T>(x)).ToArray();
        }


        /// <summary>
        /// Gets length.
        /// </summary>
        public Task<long> LengthAsync(CommandFlags flags = CommandFlags.None)
            => this.Connection.Database.HashLengthAsync(this.Key, flags);  // HLEN https://redis.io/commands/hlen


        /// <summary>
        /// Adds value.
        /// </summary>
        public Task<bool> AddAsync(T value, TimeSpan? expiry = null, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            // HSET
            // https://redis.io/commands/hset

            expiry ??= this.DefaultExpiry;
            var f = this.Connection.Converter.Serialize(value);
            var v = this.Connection.Converter.Serialize(true);
            return this.ExecuteWithExpiryAsync
            (
                (db, a) => db.HashSetAsync(a.key, a.f, a.v, a.when, a.flags),
                (key: this.Key, f, v, when, flags),
                expiry,
                flags
            );
        }


        /// <summary>
        /// Adds values.
        /// </summary>
        public Task AddAsync(IEnumerable<T> values, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
        {
            // HMSET
            // https://redis.io/commands/hmset

            expiry ??= this.DefaultExpiry;
            var hashEntries
                = values
                .Select(this.Connection.Converter, (x, c) =>
                {
                    var f = c.Serialize(x);
                    var v = c.Serialize(true);
                    return new HashEntry(f, v);
                })
                .ToArray();
            return this.ExecuteWithExpiryAsync
            (
                (db, a) => db.HashSetAsync(a.key, a.hashEntries, a.flags),
                (key: this.Key, hashEntries, flags),
                expiry,
                flags
            );
        }
        #endregion
    }
}
