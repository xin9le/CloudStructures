using StackExchange.Redis;

namespace CloudStructures
{
    /// <summary>
    /// Represents a Redis connection interface.
    /// </summary>
    public interface IRedisConnection
    {
        /// <summary>
        /// Gets the asynchronous database accessor for Redis.
        /// </summary>
        IDatabaseAsync Database { get; }

        /// <summary>
        /// Gets the Redis transaction object.
        /// </summary>
        ITransaction Transaction { get; }

        /// <summary>
        /// Gets an array of Redis server instances connected to the current multiplexer.
        /// </summary>
        IServer[] Servers { get; }
    }
}
