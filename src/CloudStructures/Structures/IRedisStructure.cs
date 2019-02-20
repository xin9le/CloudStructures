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


        /// <summary>
        /// Gets default expiration time.
        /// </summary>
        TimeSpan? DefaultExpiry { get; }
        #endregion
    }



    /// <summary>
    /// Provides extension methods for <see cref="IRedisStructure"/>.
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
            => redis.Connection.Database.IsConnected(redis.Key);


        /// <summary>
        /// DEL : http://redis.io/commands/del
        /// </summary>
        public static Task<bool> Delete<T>(this T redis, CommandFlags flags = CommandFlags.None)
            where T : IRedisStructure
            => redis.Connection.Database.KeyDeleteAsync(redis.Key, flags);


        /// <summary>
        /// DUMP : https://redis.io/commands/dump
        /// </summary>
        public static Task<byte[]> Dump<T>(this T redis, CommandFlags flags = CommandFlags.None)
            where T : IRedisStructure
            => redis.Connection.Database.KeyDumpAsync(redis.Key, flags);


        /// <summary>
        /// EXISTS : http://redis.io/commands/exists
        /// </summary>
        public static Task<bool> Exists<T>(this T redis, CommandFlags flags = CommandFlags.None)
            where T : IRedisStructure
            => redis.Connection.Database.KeyExistsAsync(redis.Key, flags);


        /// <summary>
        /// SETEX  : http://redis.io/commands/setex
        /// PSETEX : http://redis.io/commands/psetex
        /// </summary>
        public static Task<bool> Expire<T>(this T redis, TimeSpan expiry, CommandFlags flags = CommandFlags.None)
            where T : IRedisStructure
            => redis.Connection.Database.KeyExpireAsync(redis.Key, expiry, flags);


        /// <summary>
        /// MOVE : https://redis.io/commands/move
        /// </summary>
        public static Task<bool> Move<T>(this T redis, int database, CommandFlags flags = CommandFlags.None)
            where T : IRedisStructure
            => redis.Connection.Database.KeyMoveAsync(redis.Key, database, flags);


        /// <summary>
        /// MIGRATE : https://redis.io/commands/migrate
        /// </summary>
        public static Task Migrate<T>(this T redis, EndPoint toServer, int toDatabase = 0, int timeoutMilliseconds = 0, MigrateOptions migrateOptions = MigrateOptions.None, CommandFlags flags = CommandFlags.None)
            where T : IRedisStructure
            => redis.Connection.Database.KeyMigrateAsync(redis.Key, toServer, toDatabase, timeoutMilliseconds, migrateOptions, flags);


        /// <summary>
        /// PERSIST : https://redis.io/commands/persist
        /// </summary>
        public static Task<bool> Persist<T>(this T redis, CommandFlags flags = CommandFlags.None)
            where T : IRedisStructure
            => redis.Connection.Database.KeyPersistAsync(redis.Key, flags);


        /// <summary>
        /// RENAME : https://redis.io/commands/rename
        /// </summary>
        public static Task<bool> Rename<T>(this T redis, RedisKey newKey, When when = When.Always, CommandFlags flags = CommandFlags.None)
            where T : IRedisStructure
            => redis.Connection.Database.KeyRenameAsync(redis.Key, newKey, when, flags);


        /// <summary>
        /// TTL http://redis.io/commands/ttl
        /// </summary>
        public static Task<TimeSpan?> TimeToLive<T>(this T redis, CommandFlags flags = CommandFlags.None)
            where T : IRedisStructure
            => redis.Connection.Database.KeyTimeToLiveAsync(redis.Key, flags);


        /// <summary>
        /// TYPE : https://redis.io/commands/type
        /// </summary>
        public static Task<RedisType> Type<T>(this T redis, CommandFlags flags = CommandFlags.None)
            where T : IRedisStructure
            => redis.Connection.Database.KeyTypeAsync(redis.Key, flags);
        #endregion
    }
}
