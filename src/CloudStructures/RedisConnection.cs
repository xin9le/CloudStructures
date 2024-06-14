using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using CloudStructures.Converters;
using CloudStructures.Internals;
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
        public RedisConfig? Config { get; }

        /// <summary>
        /// Gets value converter.
        /// </summary>
        internal ValueConverter Converter { get; }


        /// <summary>
        /// Gets connection event handler.
        /// </summary>
        private IConnectionEventHandler? Handler { get; }


        /// <summary>
        /// Gets logger.
        /// </summary>
        private TextWriter? Logger { get; }


        /// <summary>
        /// Gets an interactive connection to a database inside redis.
        /// </summary>
        internal IDatabaseAsync Database => _database ?? (this.Config?.Database.HasValue ?? false
            ? this.GetConnection().GetDatabase(this.Config.Database.Value)
            : this.GetConnection().GetDatabase());

        /// <summary>
        /// Gets a transaction.
        /// </summary>
        public ITransaction Transaction => ((IDatabase)Database).CreateTransaction();

        /// <summary>
        /// Gets target servers.
        /// </summary>
        public IServer[] Servers
        {
            get
            {
                if (this.Config != null)
                {
                    return this.Config.Options
                        .EndPoints
                        .Select(this.GetConnection(), static (x, c) => c.GetServer(x))
                        .ToArray();
                }
                else
                {
                    var multiplexer = Database.Multiplexer;
                    var endpoints = multiplexer.GetEndPoints();

                    return endpoints.Select(endpoint => multiplexer.GetServer(endpoint))
                        .ToArray();
                }
            }
        }
        #endregion


        #region Constructors
        /// <summary>
        /// Creates instance with RedisConfig.
        /// </summary>
        /// <param name="config"></param>
        /// <param name="converter">If null, use <see cref="SystemTextJsonConverter"/> as default.</param>
        /// <param name="handler"></param>
        /// <param name="logger"></param>
        public RedisConnection(RedisConfig config, IValueConverter? converter = null, IConnectionEventHandler? handler = null, TextWriter? logger = null)
        {
            this.Config = config;
            this.Converter = new(converter);
            this.Handler = handler;
            this.Logger = logger;
        }

        /// <summary>
        /// Creates a RedisConnection instance with a pre-existing IDatabaseAsync instance.
        /// </summary>
        /// <param name="database">Pre-existing IDatabaseAsync instance.</param>
        /// <param name="converter">Optional value converter.</param>
        /// <param name="handler">Connection event handler.</param>
        /// <param name="logger">Logger.</param>
        public RedisConnection(IDatabaseAsync database, IValueConverter? converter = null, IConnectionEventHandler? handler = null, TextWriter? logger = null)
        {
            this._database = database ?? throw new ArgumentNullException(nameof(database));
            this.Converter = new ValueConverter(converter);
            this.Handler = handler;
            this.Logger = logger;
        }
        #endregion


        #region Connection management
        /// <summary>
        /// Gets underlying connection.
        /// </summary>
        /// <returns></returns>
        public ConnectionMultiplexer GetConnection()
        {
            lock (this._gate)
            {
                if (this._connection is null || !this._connection.IsConnected)
                {
                    try
                    {
                        //--- create inner connection
                        var stopwatch = Stopwatch.StartNew();
                        this._connection = ConnectionMultiplexer.Connect(this.Config!.Options, this.Logger);
                        stopwatch.Stop();
                        this.Handler?.OnConnectionOpened(this, new(stopwatch.Elapsed));

                        //--- attach events
                        this._connection.ConfigurationChanged += (_, e) => this.Handler?.OnConfigurationChanged(this, e);
                        this._connection.ConfigurationChangedBroadcast += (_, e) => this.Handler?.OnConfigurationChangedBroadcast(this, e);
                        this._connection.ConnectionFailed += (_, e) => this.Handler?.OnConnectionFailed(this, e);
                        this._connection.ConnectionRestored += (_, e) => this.Handler?.OnConnectionRestored(this, e);
                        this._connection.ErrorMessage += (_, e) => this.Handler?.OnErrorMessage(this, e);
                        this._connection.HashSlotMoved += (_, e) => this.Handler?.OnHashSlotMoved(this, e);
                        this._connection.InternalError += (_, e) => this.Handler?.OnInternalError(this, e);
                        this._connection.ServerMaintenanceEvent += (_, e) => this.Handler?.OnServerMaintenanceEvent(this, e);
                    }
                    catch
                    {
                        this._connection = null;
                        throw;
                    }
                }
                return this._connection;
            }
        }

        private readonly object _gate = new();
        private ConnectionMultiplexer? _connection = null;
        private readonly IDatabaseAsync? _database = null;
        #endregion
    }
}
