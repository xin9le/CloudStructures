using System;
using System.IO;
using System.Linq;
using CloudStructures.Converters;
using StackExchange.Redis;



namespace CloudStructures
{
    /// <summary>
    /// Provides connection to the server.
    /// </summary>
    /// <remarks>This connection needs to be used w/o destroying. Please hold as static field or static property.</remarks>
    public sealed class RedisConnection
    {
        #region Properties
        /// <summary>
        /// Gets configuration.
        /// </summary>
        public RedisConfig Config { get; }


        /// <summary>
        /// Gets value converter.
        /// </summary>
        internal ValueConverter Converter { get; }


        /// <summary>
        /// Gets an interactive connection to a database inside redis.
        /// </summary>
        internal IDatabaseAsync Database
            => this.Config.Database.HasValue
            ? this.InnerConnection.Value.GetDatabase(this.Config.Database.Value)
            : this.InnerConnection.Value.GetDatabase();


        /// <summary>
        /// Gets a transaction.
        /// </summary>
        internal ITransaction Transaction
            => ((IDatabase)this.Database).CreateTransaction();


        /// <summary>
        /// Gets target servers.
        /// </summary>
        internal IServer[] Servers
            => this.Config.Options
            .EndPoints
            .Select(x => this.InnerConnection.Value.GetServer(x))
            .ToArray();


        /// <summary>
        /// Gets an internal connection.
        /// </summary>
        private Lazy<ConnectionMultiplexer> InnerConnection { get; }
        #endregion


        #region Constructors
        /// <summary>
        /// Creates instance.
        /// </summary>
        /// <param name="config"></param>
        /// <param name="converter"></param>
        /// <param name="handler"></param>
        /// <param name="logger"></param>
        public RedisConnection(RedisConfig config, IValueConverter converter, IConnectionEventHandler handler = null, TextWriter logger = null)
        {
            if (converter == null)
                throw new ArgumentNullException(nameof(converter));

            this.Config = config ?? throw new ArgumentNullException(nameof(config));
            this.Converter = new ValueConverter(converter);
            this.InnerConnection = new Lazy<ConnectionMultiplexer>(() =>
            {
                var connection = ConnectionMultiplexer.Connect(this.Config.Options, logger);

                //--- attach events
                connection.ConfigurationChanged += (_, e) => handler?.OnConfigurationChanged(this, e);
                connection.ConfigurationChangedBroadcast += (_, e) => handler?.OnConfigurationChangedBroadcast(this, e);
                connection.ConnectionFailed += (_, e) => handler?.OnConnectionFailed(this, e);
                connection.ConnectionRestored += (_, e) => handler?.OnConnectionRestored(this, e);
                connection.ErrorMessage += (_, e) => handler?.OnErrorMessage(this, e);
                connection.HashSlotMoved += (_, e) => handler?.OnHashSlotMoved(this, e);
                connection.InternalError += (_, e) => handler?.OnInternalError(this, e);

                return connection;
            });
        }
        #endregion
    }
}
