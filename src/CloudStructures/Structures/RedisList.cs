using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CloudStructures.Internals;
using StackExchange.Redis;



namespace CloudStructures.Structures
{
    /// <summary>
    /// Provides list related commands.
    /// </summary>
    /// <typeparam name="T">Data type</typeparam>
    public readonly struct RedisList<T> : IRedisStructureWithExpiry
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
        public RedisList(RedisConnection connection, RedisKey key, TimeSpan? defaultExpiry)
        {
            this.Connection = connection ?? throw new ArgumentNullException(nameof(connection));
            this.Key = key;
            this.DefaultExpiry = defaultExpiry;
        }
        #endregion


        #region Commands
        //- [x] ListGetByIndexAsync
        //- [x] ListInsertAfterAsync
        //- [x] ListInsertBeforeAsync
        //- [x] ListLeftPopAsync
        //- [x] ListLeftPushAsync
        //- [x] ListLengthAsync
        //- [x] ListRangeAsync
        //- [x] ListRemoveAsync
        //- [x] ListRightPopAsync
        //- [x] ListRightPopLeftPushAsync
        //- [x] ListRightPushAsync
        //- [x] ListSetByIndexAsync
        //- [x] ListTrimAsync
        //- [x] SortAndStoreAsync
        //- [x] SortAsync


        /// <summary>
        /// LINDEX : https://redis.io/commands/lindex
        /// </summary>
        public async Task<RedisResult<T>> GetByIndex(long index, CommandFlags flags = CommandFlags.None)
        {
            var value = await this.Connection.Database.ListGetByIndexAsync(this.Key, index, flags).ConfigureAwait(false);
            return value.ToResult<T>(this.Connection.Converter);
        }


        /// <summary>
        /// LINSERT : https://redis.io/commands/linsert
        /// </summary>
        public Task<long> InsertAfter(T pivot, T value, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
        {
            expiry = expiry ?? this.DefaultExpiry;
            var p = this.Connection.Converter.Serialize(pivot);
            var v = this.Connection.Converter.Serialize(value);
            return this.ExecuteWithExpiry
            (
                (db, a) => db.ListInsertAfterAsync(a.key, a.p, a.v, a.flags),
                (key: this.Key, p, v, flags),
                expiry,
                flags
            );
        }


        /// <summary>
        /// LINSERT : https://redis.io/commands/linsert
        /// </summary>
        public Task<long> InsertBefore(T pivot, T value, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
        {
            expiry = expiry ?? this.DefaultExpiry;
            var p = this.Connection.Converter.Serialize(pivot);
            var v = this.Connection.Converter.Serialize(value);
            return this.ExecuteWithExpiry
            (
                (db, a) => db.ListInsertBeforeAsync(a.key, a.p, a.v, a.flags),
                (key: this.Key, p, v, flags),
                expiry,
                flags
            );
        }


        /// <summary>
        /// LPOP : https://redis.io/commands/lpop
        /// </summary>
        public async Task<RedisResult<T>> LeftPop(CommandFlags flags = CommandFlags.None)
        {
            var value = await this.Connection.Database.ListLeftPopAsync(this.Key, flags).ConfigureAwait(false);
            return value.ToResult<T>(this.Connection.Converter);
        }


        /// <summary>
        /// LPUSH : https://redis.io/commands/lpush
        /// </summary>
        public Task<long> LeftPush(T value, TimeSpan? expiry = null, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            expiry = expiry ?? this.DefaultExpiry;
            var serialized = this.Connection.Converter.Serialize(value);
            return this.ExecuteWithExpiry
            (
                (db, a) => db.ListLeftPushAsync(a.key, a.serialized, a.when, a.flags),
                (key: this.Key, serialized, when, flags),
                expiry,
                flags
            );
        }


        /// <summary>
        /// LPUSH : https://redis.io/commands/lpush
        /// </summary>
        public Task<long> LeftPush(IEnumerable<T> values, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
        {
            expiry = expiry ?? this.DefaultExpiry;
            var serialized = values.Select(this.Connection.Converter.Serialize).ToArray();
            return this.ExecuteWithExpiry
            (
                (db, a) => db.ListLeftPushAsync(a.key, a.serialized, a.flags),
                (key: this.Key, serialized, flags),
                expiry,
                flags
            );
        }


        /// <summary>
        /// LLEN : https://redis.io/commands/llen
        /// </summary>
        public Task<long> Length(CommandFlags flags = CommandFlags.None)
            => this.Connection.Database.ListLengthAsync(this.Key, flags);


        /// <summary>
        /// LRANGE : https://redis.io/commands/lrange
        /// </summary>
        public async Task<T[]> Range(long start = 0, long stop = -1, CommandFlags flags = CommandFlags.None)
        {
            var values = await this.Connection.Database.ListRangeAsync(this.Key, start, stop, flags).ConfigureAwait(false);
            return values.Select(this.Connection.Converter, (x, c) => c.Deserialize<T>(x)).ToArray();
        }


        /// <summary>
        /// LREM : http://redis.io/commands/lrem
        /// </summary>
        /// <param name="value">削除する値</param>
        /// <param name="count">削除する件数
        /// <para>count &gt; 0 : 先頭から末尾に向かって検索しつつ削除</para>
        /// <para>count &lt; 0 : 末尾から先頭に向かって検索しつつ削除</para>
        /// <para>count = 0 : 一致するものを全件削除</para>
        /// </param>
        public Task<long> Remove(T value, long count = 0, CommandFlags flags = CommandFlags.None)
        {
            var serialized = this.Connection.Converter.Serialize(value);
            return this.Connection.Database.ListRemoveAsync(this.Key, serialized, count, flags);
        }


        /// <summary>
        /// RPOP : https://redis.io/commands/rpop
        /// </summary>
        public async Task<RedisResult<T>> RightPop(CommandFlags flags = CommandFlags.None)
        {
            var value = await this.Connection.Database.ListRightPopAsync(this.Key, flags).ConfigureAwait(false);
            return value.ToResult<T>(this.Connection.Converter);
        }


        /// <summary>
        /// RPOPLPUSH : https://redis.io/commands/rpoplpush
        /// </summary>
        public async Task<RedisResult<T>> RightPopLeftPush(RedisList<T> destination, CommandFlags flags = CommandFlags.None)
        {
            var value = await this.Connection.Database.ListRightPopLeftPushAsync(this.Key, destination.Key, flags).ConfigureAwait(false);
            return value.ToResult<T>(this.Connection.Converter);
        }


        /// <summary>
        /// RPUSH : https://redis.io/commands/rpush
        /// </summary>
        public Task<long> RightPush(T value, TimeSpan? expiry = null, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            expiry = expiry ?? this.DefaultExpiry;
            var serialized = this.Connection.Converter.Serialize(value);
            return this.ExecuteWithExpiry
            (
                (db, a) => db.ListRightPushAsync(a.key, a.serialized, a.when, a.flags),
                (key: this.Key, serialized, when, flags),
                expiry,
                flags
            );
        }


        /// <summary>
        /// RPUSH : https://redis.io/commands/rpush
        /// </summary>
        public Task<long> RightPush(IEnumerable<T> values, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
        {
            expiry = expiry ?? this.DefaultExpiry;
            var serialized = values.Select(this.Connection.Converter.Serialize).ToArray();
            return this.ExecuteWithExpiry
            (
                (db, a) => db.ListRightPushAsync(a.key, a.serialized, a.flags),
                (key: this.Key, serialized, flags),
                expiry,
                flags
            );
        }


        /// <summary>
        /// LSET : https://redis.io/commands/lset
        /// </summary>
        public Task SetByIndex(long index, T value, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
        {
            expiry = expiry ?? this.DefaultExpiry;
            var serialized = this.Connection.Converter.Serialize(value);
            return this.ExecuteWithExpiry
            (
                (db, a) => db.ListSetByIndexAsync(a.key, a.index, a.serialized, a.flags),
                (key: this.Key, index, serialized, flags),
                expiry,
                flags
            );
        }


        /// <summary>
        /// LTRIM : https://redis.io/commands/ltrim
        /// </summary>
        public Task Trim(long start, long stop, CommandFlags flags = CommandFlags.None)
            => this.Connection.Database.ListTrimAsync(this.Key, start, stop, flags);


        /// <summary>
        /// SORT : https://redis.io/commands/sort
        /// </summary>
        public Task<long> SortAndStore(RedisList<T> destination, long skip = 0, long take = -1, Order order = Order.Ascending, SortType sortType = SortType.Numeric, /*RedisValue by = default, RedisValue[] get = null,*/ CommandFlags flags = CommandFlags.None)
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


        #region Custom Commands
        /// <summary>
        /// First LPUSH, then LTRIM to the specified list length.
        /// </summary>
        public async Task<long> FixedLengthLeftPush(T value, long length, TimeSpan? expiry = null, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            expiry = expiry ?? this.DefaultExpiry;
            var serialized = this.Connection.Converter.Serialize(value);

            //--- execute multiple commands in transaction
            var t = this.Connection.Transaction;
            var leftPush = t.ListLeftPushAsync(this.Key, serialized, when, flags);
            _ = t.ListTrimAsync(this.Key, 0, length - 1, flags);  // forget
            if (expiry.HasValue)
                _ = t.KeyExpireAsync(this.Key, expiry.Value, flags);  // forget

            //--- commit
            await t.ExecuteAsync(flags).ConfigureAwait(false);

            //--- get result
            var pushLength = await leftPush.ConfigureAwait(false);
            return Math.Min(pushLength, length);
        }


        /// <summary>
        /// First LPUSH, then LTRIM to the specified list length.
        /// </summary>
        public async Task<long> FixedLengthLeftPush(IEnumerable<T> values, long length, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
        {
            expiry = expiry ?? this.DefaultExpiry;
            var serialized = values.Select(this.Connection.Converter.Serialize).ToArray();

            //--- execute multiple commands in transaction
            var t = this.Connection.Transaction;
            var leftPush = t.ListLeftPushAsync(this.Key, serialized, flags);
            _ = t.ListTrimAsync(this.Key, 0, length - 1, flags);  // forget
            if (expiry.HasValue)
                _ = t.KeyExpireAsync(this.Key, expiry.Value, flags);  // forget

            //--- commit
            await t.ExecuteAsync(flags).ConfigureAwait(false);

            //--- get result
            var pushLength = await leftPush.ConfigureAwait(false);
            return Math.Min(pushLength, length);
        }
        #endregion
    }
}
