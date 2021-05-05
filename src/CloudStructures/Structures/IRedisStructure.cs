using System;
using System.Net;
using System.Threading.Tasks;
using StackExchange.Redis;



namespace CloudStructures.Structures
{
    /// <summary>
    /// Represents a base interface for Redis data structure.
    /// </summary>
    public interface IRedisStructure
    {
        #region Properties
        /// <summary>
        /// Gets connection.
        /// </summary>
        RedisConnection Connection { get; }


        /// <summary>
        /// Gets key.
        /// </summary>
        RedisKey Key { get; }
        #endregion
    }



    /// <summary>
    /// Represents a interface for Redis data structure with default expiration time.
    /// </summary>
    public interface IRedisStructureWithExpiry : IRedisStructure
    {
        #region Properties
        /// <summary>
        /// Gets default expiration time.
        /// </summary>
        TimeSpan? DefaultExpiry { get; }
        #endregion
    }



    /// <summary>
    /// Provides extension methods for <see cref="IRedisStructure"/> and <seealso cref="IRedisStructureWithExpiry"/>.
    /// </summary>
    public static class RedisStructureExtensions
    {
        #region Commands
        //- [] DebugObjectAsync
        //- [] ExecuteAsync
        //- [] IdentifyEndpointAsync
        //- [x] IsConnected
        //- [x] KeyDeleteAsync
        //- [x] KeyDumpAsync
        //- [x] KeyExistsAsync
        //- [x] KeyExpireAsync
        //- [x] KeyMigrateAsync
        //- [x] KeyMoveAsync
        //- [x] KeyPersistAsync
        //- [x] KeyRenameAsync
        //- [] KeyRestoreAsync
        //- [x] KeyTimeToLiveAsync
        //- [x] KeyTypeAsync
        //- [] PublishAsync


        /// <summary>
        /// Indicates whether the instance can communicate with the server (resolved using the supplied key and optional flags).
        /// </summary>
        public static bool IsConnected<T>(this T redis, CommandFlags flags = CommandFlags.None)
            where T : IRedisStructure
            => redis.Connection.Database.IsConnected(redis.Key, flags);


        /// <summary>
        /// DEL : <a href="http://redis.io/commands/del"></a>
        /// </summary>
        public static Task<bool> DeleteAsync<T>(this T redis, CommandFlags flags = CommandFlags.None)
            where T : IRedisStructure
            => redis.Connection.Database.KeyDeleteAsync(redis.Key, flags);


        /// <summary>
        /// DUMP : <a href="https://redis.io/commands/dump"></a>
        /// </summary>
        public static Task<byte[]> DumpAsync<T>(this T redis, CommandFlags flags = CommandFlags.None)
            where T : IRedisStructure
            => redis.Connection.Database.KeyDumpAsync(redis.Key, flags);


        /// <summary>
        /// EXISTS : <a href="http://redis.io/commands/exists"></a>
        /// </summary>
        public static Task<bool> ExistsAsync<T>(this T redis, CommandFlags flags = CommandFlags.None)
            where T : IRedisStructure
            => redis.Connection.Database.KeyExistsAsync(redis.Key, flags);


        /// <summary>
        /// SETEX : <a href="http://redis.io/commands/setex"></a><br/>
        /// PSETEX : <a href="http://redis.io/commands/psetex"></a>
        /// </summary>
        public static Task<bool> ExpireAsync<T>(this T redis, TimeSpan? expiry, CommandFlags flags = CommandFlags.None)
            where T : IRedisStructure
            => redis.Connection.Database.KeyExpireAsync(redis.Key, expiry, flags);


        /// <summary>
        /// MOVE : <a href="https://redis.io/commands/move"></a>
        /// </summary>
        public static Task<bool> MoveAsync<T>(this T redis, int database, CommandFlags flags = CommandFlags.None)
            where T : IRedisStructure
            => redis.Connection.Database.KeyMoveAsync(redis.Key, database, flags);


        /// <summary>
        /// MIGRATE : <a href="https://redis.io/commands/migrate"></a>
        /// </summary>
        public static Task MigrateAsync<T>(this T redis, EndPoint toServer, int toDatabase = 0, int timeoutMilliseconds = 0, MigrateOptions migrateOptions = MigrateOptions.None, CommandFlags flags = CommandFlags.None)
            where T : IRedisStructure
            => redis.Connection.Database.KeyMigrateAsync(redis.Key, toServer, toDatabase, timeoutMilliseconds, migrateOptions, flags);


        /// <summary>
        /// PERSIST : <a href="https://redis.io/commands/persist"></a>
        /// </summary>
        public static Task<bool> PersistAsync<T>(this T redis, CommandFlags flags = CommandFlags.None)
            where T : IRedisStructure
            => redis.Connection.Database.KeyPersistAsync(redis.Key, flags);


        /// <summary>
        /// RENAME : <a href="https://redis.io/commands/rename"></a>
        /// </summary>
        public static Task<bool> RenameAsync<T>(this T redis, RedisKey newKey, When when = When.Always, CommandFlags flags = CommandFlags.None)
            where T : IRedisStructure
            => redis.Connection.Database.KeyRenameAsync(redis.Key, newKey, when, flags);


        /// <summary>
        /// TTL : <a href="http://redis.io/commands/ttl"></a>
        /// </summary>
        public static Task<TimeSpan?> TimeToLiveAsync<T>(this T redis, CommandFlags flags = CommandFlags.None)
            where T : IRedisStructure
            => redis.Connection.Database.KeyTimeToLiveAsync(redis.Key, flags);


        /// <summary>
        /// TYPE : <a href="https://redis.io/commands/type"></a>
        /// </summary>
        public static Task<RedisType> TypeAsync<T>(this T redis, CommandFlags flags = CommandFlags.None)
            where T : IRedisStructure
            => redis.Connection.Database.KeyTypeAsync(redis.Key, flags);
        #endregion
    }
}
