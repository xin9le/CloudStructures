using System;
using System.Linq;
using System.Threading.Tasks;
using CloudStructures.Internals;
using StackExchange.Redis;



namespace CloudStructures.Structures
{
    /// <summary>
    /// Provides bit related commands.
    /// </summary>
    public readonly struct RedisBit : IRedisStructure
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
        public RedisBit(RedisConnection connection, RedisKey key, TimeSpan? defaultExpiry)
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
        public Task<long> Count(long start = 0, long end = -1, CommandFlags flags = CommandFlags.None)
            => this.Connection.Database.StringBitCountAsync(this.Key, start, end, flags);


        /// <summary>
        /// BITOP : https://redis.io/commands/bitop
        /// </summary>
        public Task<long> Operation(Bitwise operation, RedisBit first, RedisBit? second = null, CommandFlags flags = CommandFlags.None)
        {
            // RedisKey で受けてもいいけど、間違いがないように敢えて RedisBit で受ける
            var firstKey = first.Key;
            var secondKey = second?.Key ?? default; 
            return this.Connection.Database.StringBitOperationAsync(operation, this.Key, firstKey, secondKey, flags);
        }


        /// <summary>
        /// BITOP : https://redis.io/commands/bitop
        /// </summary>
        public Task<long> Operation(Bitwise operation, RedisBit[] bits, CommandFlags flags = CommandFlags.None)
        {
            // RedisKey[] で受けてもいいけど、間違いがないように敢えて RedisBit[] で受ける
            if (bits == null) throw new ArgumentNullException(nameof(bits));
            if (bits.Length == 0) throw new ArgumentException("bits length is 0.");

            var keys = bits.Select(x => x.Key).ToArray();
            return this.Connection.Database.StringBitOperationAsync(operation, this.Key, keys, flags);
        }


        /// <summary>
        /// BITPOSITION : http://redis.io/commands/bitpos
        /// </summary>
        public Task<long> Position(bool bit, long start = 0, long end = -1, CommandFlags flags = CommandFlags.None)
            => this.Connection.Database.StringBitPositionAsync(this.Key, bit, start, end, flags);


        /// <summary>
        /// GETBIT : http://redis.io/commands/getbit
        /// </summary>
        public Task<bool> Get(long offset, CommandFlags flags = CommandFlags.None)
            => this.Connection.Database.StringGetBitAsync(this.Key, offset, flags);


        /// <summary>
        /// SETBIT : http://redis.io/commands/setbit
        /// </summary>
        public Task<bool> Set(long offset, bool bit, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
        {
            expiry = expiry ?? this.DefaultExpiry;
            return this.ExecuteWithExpiry
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
