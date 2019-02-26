using StackExchange.Redis;



namespace CloudStructures
{
    /// <summary>
    /// Provides connection event handling function.
    /// </summary>
    public interface IConnectionEventHandler
    {
        /// <summary>
        /// Raised when configuration changes are detected
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        void OnConfigurationChanged(RedisConnection sender, EndPointEventArgs e);


        /// <summary>
        /// Raised when nodes are explicitly requested to reconfigure via broadcast;
        /// this usually means master/slave changes
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        void OnConfigurationChangedBroadcast(RedisConnection sender, EndPointEventArgs e);


        /// <summary>
        /// Raised whenever a physical connection fails
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        void OnConnectionFailed(RedisConnection sender, ConnectionFailedEventArgs e);


        /// <summary>
        /// Raised whenever a physical connection is opened
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        void OnConnectionOpened(RedisConnection sender, ConnectionOpenedEventArgs e);


        /// <summary>
        /// Raised whenever a physical connection is established
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        void OnConnectionRestored(RedisConnection sender, ConnectionFailedEventArgs e);


        /// <summary>
        /// A server replied with an error message;
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        void OnErrorMessage(RedisConnection sender, RedisErrorEventArgs e);


        /// <summary>
        /// Raised when a hash-slot has been relocated
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        void OnHashSlotMoved(RedisConnection sender, HashSlotMovedEventArgs e);


        /// <summary>
        /// Raised whenever an internal error occurs (this is primarily for debugging)
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        void OnInternalError(RedisConnection sender, InternalErrorEventArgs e);
    }
}
