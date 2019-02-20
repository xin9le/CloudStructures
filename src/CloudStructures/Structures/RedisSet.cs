using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CloudStructures.Internals;
using StackExchange.Redis;



namespace CloudStructures.Structures
{
    /// <summary>
    /// Provides set related commands.
    /// </summary>
    /// <typeparam name="T">Data type</typeparam>
    public readonly struct RedisSet<T> : IRedisStructure
    {
        #region IRedisStructure implementations
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
        public RedisSet(RedisConnection connection, RedisKey key, TimeSpan? defaultExpiry)
        {
            this.Connection = connection ?? throw new ArgumentNullException(nameof(connection));
            this.Key = key;
            this.DefaultExpiry = defaultExpiry;
        }
        #endregion


        #region Commands
        //- [x] SetAddAsync
        //- [x] SetCombineAndStoreAsync
        //- [x] SetCombineAsync
        //- [x] SetContainsAsync
        //- [x] SetLengthAsync
        //- [x] SetMembersAsync
        //- [x] SetMoveAsync
        //- [x] SetPopAsync
        //- [x] SetRandomMemberAsync
        //- [x] SetRandomMembersAsync
        //- [x] SetRemoveAsync
        //- [x] SortAndStoreAsync
        //- [x] SortAsync


        /// <summary>
        /// SADD : http://redis.io/commands/sadd
        /// </summary>
        public Task<bool> Add(T value, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
        {
            expiry = expiry ?? this.DefaultExpiry;
            var serialised = this.Connection.Converter.Serialize(value);
            return this.ExecuteWithExpiry
            (
                (db, a) => db.SetAddAsync(a.key, a.serialised, a.flags),
                (key: this.Key, serialised, flags),
                expiry,
                flags
            );
        }


        /// <summary>
        /// SADD : http://redis.io/commands/sadd
        /// </summary>
        public Task<long> Add(IEnumerable<T> values, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
        {
            expiry = expiry ?? this.DefaultExpiry;
            var serialised = values.Select(this.Connection.Converter.Serialize).ToArray();
            return this.ExecuteWithExpiry
            (
                (db, a) => db.SetAddAsync(a.key, a.serialised, a.flags),
                (key: this.Key, serialised, flags),
                expiry,
                flags
            );
        }


        /// <summary>
        /// SDIFFSTORE  : https://redis.io/commands/sdiffstore
        /// SINTERSTORE : https://redis.io/commands/sinterstore
        /// SUNIONSTORE : https://redis.io/commands/sunionstore
        /// </summary>
        /// <remarks>
        /// Combine self and other, then save it to the destination.
        /// It does not work unless you pass keys located the same server.
        /// </remarks>
        public Task<long> CombineAndStore(SetOperation operation, RedisSet<T> destination, RedisSet<T> other, CommandFlags flags = CommandFlags.None)
            => this.Connection.Database.SetCombineAndStoreAsync(operation, destination.Key, this.Key, other.Key, flags);


        /// <summary>
        /// SDIFFSTORE  : https://redis.io/commands/sdiffstore
        /// SINTERSTORE : https://redis.io/commands/sinterstore
        /// SUNIONSTORE : https://redis.io/commands/sunionstore
        /// </summary>
        /// <remarks>
        /// Combine self and other, then save it to the destination.
        /// It does not work unless you pass keys located the same server.
        /// </remarks>
        public Task<long> CombineAndStore(SetOperation operation, RedisSet<T> destination, IReadOnlyCollection<RedisSet<T>> others, CommandFlags flags = CommandFlags.None)
        {
            if (others == null) throw new ArgumentNullException(nameof(others));
            if (others.Count == 0) throw new ArgumentException("others length is 0.");

            var keys = others.Select(x => x.Key).Concat(new []{ this.Key }).ToArray();
            return this.Connection.Database.SetCombineAndStoreAsync(operation, destination.Key, keys, flags);
        }


        /// <summary>
        /// SDIFF  : https://redis.io/commands/sdiff
        /// SINTER : https://redis.io/commands/sinter
        /// SUNION : https://redis.io/commands/sunion
        /// </summary>
        /// <remarks>It does not work unless you pass keys located the same server.</remarks>
        public async Task<T[]> Combine(SetOperation operation, RedisSet<T> other, CommandFlags flags = CommandFlags.None)
        {
            var values = await this.Connection.Database.SetCombineAsync(operation, this.Key, other.Key, flags).ConfigureAwait(false);
            return values.Select(this.Connection.Converter, (x, c) => c.Deserialize<T>(x)).ToArray();
        }


        /// <summary>
        /// SDIFF  : https://redis.io/commands/sdiff
        /// SINTER : https://redis.io/commands/sinter
        /// SUNION : https://redis.io/commands/sunion
        /// </summary>
        /// <remarks>It does not work unless you pass keys located the same server.</remarks>
        public async Task<T[]> Combine(SetOperation operation, IReadOnlyCollection<RedisSet<T>> others, CommandFlags flags = CommandFlags.None)
        {
            if (others == null) throw new ArgumentNullException(nameof(others));
            if (others.Count == 0) throw new ArgumentException("others length is 0.");

            var keys = new []{ this.Key }.Concat(others.Select(x => x.Key)).ToArray();
            var values = await this.Connection.Database.SetCombineAsync(operation, keys, flags).ConfigureAwait(false);
            return values.Select(this.Connection.Converter, (x, c) => c.Deserialize<T>(x)).ToArray();
        }


        /// <summary>
        /// SISMEMBER : http://redis.io/commands/sismember
        /// </summary>
        public Task<bool> Contains(T value, CommandFlags flags = CommandFlags.None)
        {
            var serialized = this.Connection.Converter.Serialize(value);
            return this.Connection.Database.SetContainsAsync(this.Key, serialized, flags);
        }


        /// <summary>
        /// SCARD : http://redis.io/commands/scard
        /// </summary>
        public Task<long> Length(CommandFlags flags = CommandFlags.None)
            => this.Connection.Database.SetLengthAsync(this.Key, flags);


        /// <summary>
        /// SMEMBERS : https://redis.io/commands/smembers
        /// </summary>
        public async Task<T[]> Members(CommandFlags flags = CommandFlags.None)
        {
            var members = await this.Connection.Database.SetMembersAsync(this.Key, flags).ConfigureAwait(false);
            return members
                .Select(this.Connection.Converter, (x, c) => c.Deserialize<T>(x))
                .ToArray();
        }


        /// <summary>
        /// SMOVE : https://redis.io/commands/smove
        /// </summary>
        public Task<bool> Move(RedisSet<T> destination, T value, CommandFlags flags = CommandFlags.None)
        {
            var serialized = this.Connection.Converter.Serialize(value);
            return this.Connection.Database.SetMoveAsync(this.Key, destination.Key, serialized, flags);
        }


        /// <summary>
        /// SPOP : http://redis.io/commands/spop
        /// </summary>
        public async Task<RedisResult<T>> Pop(CommandFlags flags = CommandFlags.None)
        {
            var value = await this.Connection.Database.SetPopAsync(this.Key, flags).ConfigureAwait(false);
            return value.ToResult<T>(this.Connection.Converter);
        }


        /// <summary>
        /// SRANDMEMBER : https://redis.io/commands/srandmember
        /// </summary>
        public async Task<RedisResult<T>> RandomMember(CommandFlags flags = CommandFlags.None)
        {
            var value = await this.Connection.Database.SetRandomMemberAsync(this.Key, flags).ConfigureAwait(false);
            return value.ToResult<T>(this.Connection.Converter);
        }


        /// <summary>
        /// SRANDMEMBER : https://redis.io/commands/srandmember
        /// </summary>
        public async Task<T[]> RandomMember(long count, CommandFlags flags = CommandFlags.None)
        {
            var values = await this.Connection.Database.SetRandomMembersAsync(this.Key, count, flags).ConfigureAwait(false);
            return values
                .Select(this.Connection.Converter, (x, c) => c.Deserialize<T>(x))
                .ToArray();
        }


        /// <summary>
        /// SREM : http://redis.io/commands/srem
        /// </summary>
        public Task<bool> Remove(T value, CommandFlags flags = CommandFlags.None)
        {
            var serialized = this.Connection.Converter.Serialize(value);
            return this.Connection.Database.SetRemoveAsync(this.Key, serialized, flags);
        }


        /// <summary>
        /// SORT : https://redis.io/commands/sort
        /// </summary>
        public Task<long> SortAndStore(RedisSet<T> destination, long skip = 0, long take = -1, Order order = Order.Ascending, SortType sortType = SortType.Numeric, /*RedisValue by = default, RedisValue[] get = null,*/ CommandFlags flags = CommandFlags.None)
        {
            //--- I don't know if serialization is necessary or not, so I will fix the default value.
            RedisValue by = default;
            RedisValue[] get = default;
            return this.Connection.Database.SortAndStoreAsync(destination.Key, this.Key, skip, take, order, sortType, by, get, flags);
        }


        /// <summary>
        /// SORT : https://redis.io/commands/sort
        /// </summary>
        public async Task<T[]> Sort(long skip = 0, long take = -1, Order order = Order.Ascending, SortType sortType = SortType.Numeric, /*RedisValue by = default, RedisValue[] get = null,*/ CommandFlags flags = CommandFlags.None)
        {
            //--- I don't know if serialization is necessary or not, so I will fix the default value.
            RedisValue by = default;
            RedisValue[] get = default;
            var values = await this.Connection.Database.SortAsync(this.Key, skip, take, order, sortType, by, get, flags).ConfigureAwait(false);
            return values.Select(this.Connection.Converter, (x, c) => c.Deserialize<T>(x)).ToArray();
        }
        #endregion
    }
}
