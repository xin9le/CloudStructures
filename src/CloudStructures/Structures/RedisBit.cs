using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CloudStructures.Internals;
using StackExchange.Redis;



namespace CloudStructures.Structures
{
    /// <summary>
    /// Provides bit related commands.
    /// </summary>
    public readonly struct RedisBit : IRedisStructureWithExpiry
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
        public RedisBit(RedisConnection connection, in RedisKey key, TimeSpan? defaultExpiry)
        {
            this.Connection = connection ?? throw new ArgumentNullException(nameof(connection));
            this.Key = key;
            this.DefaultExpiry = defaultExpiry;
        }
        #endregion


        #region Commands
        //- [x] StringBitCountAsync
        //- [x] StringBitOperationAsync
        //- [x] StringBitPositionAsync
        //- [x] StringGetBitAsync
        //- [x] StringSetBitAsync


        /// <summary>
        /// BITCOUNT : http://redis.io/commands/bitcount
        /// </summary>
        public Task<long> CountAsync(long start = 0, long end = -1, CommandFlags flags = CommandFlags.None)
            => this.Connection.Database.StringBitCountAsync(this.Key, start, end, flags);


        /// <summary>
        /// BITOP : https://redis.io/commands/bitop
        /// </summary>
        public Task<long> OperationAsync(Bitwise operation, in RedisBit first, RedisBit? second = null, CommandFlags flags = CommandFlags.None)
        {
            var firstKey = first.Key;
            var secondKey = second?.Key ?? default; 
            return this.Connection.Database.StringBitOperationAsync(operation, this.Key, firstKey, secondKey, flags);
        }


        /// <summary>
        /// BITOP : https://redis.io/commands/bitop
        /// </summary>
        public Task<long> OperationAsync(Bitwise operation, IReadOnlyCollection<RedisBit> bits, CommandFlags flags = CommandFlags.None)
        {
            if (bits == null) throw new ArgumentNullException(nameof(bits));
            if (bits.Count == 0) throw new ArgumentException("bits length is 0.");

            var keys = bits.Select(x => x.Key).ToArray();
            return this.Connection.Database.StringBitOperationAsync(operation, this.Key, keys, flags);
        }


        /// <summary>
        /// BITPOSITION : http://redis.io/commands/bitpos
        /// </summary>
        public Task<long> PositionAsync(bool bit, long start = 0, long end = -1, CommandFlags flags = CommandFlags.None)
            => this.Connection.Database.StringBitPositionAsync(this.Key, bit, start, end, flags);


        /// <summary>
        /// GETBIT : http://redis.io/commands/getbit
        /// </summary>
        public Task<bool> GetAsync(long offset, CommandFlags flags = CommandFlags.None)
            => this.Connection.Database.StringGetBitAsync(this.Key, offset, flags);


        /// <summary>
        /// SETBIT : http://redis.io/commands/setbit
        /// </summary>
        public Task<bool> SetAsync(long offset, bool bit, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
        {
            expiry ??= this.DefaultExpiry;
            return this.ExecuteWithExpiryAsync
            (
                (db, a) => db.StringSetBitAsync(a.key, a.offset, a.bit, a.flags),
                (key: this.Key, offset, bit, flags),
                expiry,
                flags
            );
        }
        #endregion
    }
}
