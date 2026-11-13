using System;
using System.Net;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace CloudStructures.Structures;



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
    extension<T>(T @this)
        where T : IRedisStructure
    {
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
        public bool IsConnected(CommandFlags flags = CommandFlags.None)
            => @this.Connection.Database.IsConnected(@this.Key, flags);


        /// <summary>
        /// DEL : <a href="http://redis.io/commands/del"></a>
        /// </summary>
        public Task<bool> DeleteAsync(CommandFlags flags = CommandFlags.None)
            => @this.Connection.Database.KeyDeleteAsync(@this.Key, flags);


        /// <summary>
        /// DUMP : <a href="https://redis.io/commands/dump"></a>
        /// </summary>
        public Task<byte[]?> DumpAsync(CommandFlags flags = CommandFlags.None)
            => @this.Connection.Database.KeyDumpAsync(@this.Key, flags);


        /// <summary>
        /// EXISTS : <a href="http://redis.io/commands/exists"></a>
        /// </summary>
        public Task<bool> ExistsAsync(CommandFlags flags = CommandFlags.None)
            => @this.Connection.Database.KeyExistsAsync(@this.Key, flags);


        /// <summary>
        /// SETEX : <a href="http://redis.io/commands/setex"></a><br/>
        /// PSETEX : <a href="http://redis.io/commands/psetex"></a>
        /// </summary>
        public Task<bool> ExpireAsync(TimeSpan? expiry, CommandFlags flags = CommandFlags.None)
            => @this.Connection.Database.KeyExpireAsync(@this.Key, expiry, flags);


        /// <summary>
        /// MOVE : <a href="https://redis.io/commands/move"></a>
        /// </summary>
        public Task<bool> MoveAsync(int database, CommandFlags flags = CommandFlags.None)
            => @this.Connection.Database.KeyMoveAsync(@this.Key, database, flags);


        /// <summary>
        /// MIGRATE : <a href="https://redis.io/commands/migrate"></a>
        /// </summary>
        public Task MigrateAsync(EndPoint toServer, int toDatabase = 0, int timeoutMilliseconds = 0, MigrateOptions migrateOptions = MigrateOptions.None, CommandFlags flags = CommandFlags.None)
            => @this.Connection.Database.KeyMigrateAsync(@this.Key, toServer, toDatabase, timeoutMilliseconds, migrateOptions, flags);


        /// <summary>
        /// PERSIST : <a href="https://redis.io/commands/persist"></a>
        /// </summary>
        public Task<bool> PersistAsync(CommandFlags flags = CommandFlags.None)
            => @this.Connection.Database.KeyPersistAsync(@this.Key, flags);


        /// <summary>
        /// RENAME : <a href="https://redis.io/commands/rename"></a>
        /// </summary>
        public Task<bool> RenameAsync(RedisKey newKey, When when = When.Always, CommandFlags flags = CommandFlags.None)
            => @this.Connection.Database.KeyRenameAsync(@this.Key, newKey, when, flags);


        /// <summary>
        /// TTL : <a href="http://redis.io/commands/ttl"></a>
        /// </summary>
        public Task<TimeSpan?> TimeToLiveAsync(CommandFlags flags = CommandFlags.None)
            => @this.Connection.Database.KeyTimeToLiveAsync(@this.Key, flags);


        /// <summary>
        /// TYPE : <a href="https://redis.io/commands/type"></a>
        /// </summary>
        public Task<RedisType> TypeAsync(CommandFlags flags = CommandFlags.None)
            => @this.Connection.Database.KeyTypeAsync(@this.Key, flags);
    }
}
