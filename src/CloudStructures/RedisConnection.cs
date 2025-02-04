using System;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Linq;
using CloudStructures.Converters;
using CloudStructures.Internals;
using StackExchange.Redis;

namespace CloudStructures;



/// <summary>
/// Provides connection to the server.
/// </summary>
/// <remarks>This connection needs to be used w/o destroying. Please hold as static field or static property.</remarks>
public sealed class RedisConnection :
    IDisposable
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
    internal IDatabaseAsync Database
    {
        get
        {
            this.CheckDisposed();

            return this.Config.Database.HasValue
                ? this.GetConnection().GetDatabase(this.Config.Database.Value)
                : this.GetConnection().GetDatabase();
        }
    }


    /// <summary>
    /// Gets a transaction.
    /// </summary>
    internal ITransaction Transaction
    {
        get
        {
            this.CheckDisposed();

            return ((IDatabase)this.Database).CreateTransaction();
        }
    }


    /// <summary>
    /// Gets target servers.
    /// </summary>
    internal IServer[] Servers
    {
        get
        {
            this.CheckDisposed();

            return this.Config.Options
                .EndPoints
                .Select(this.GetConnection(), static (x, c) => c.GetServer(x))
                .ToArray();
        }
    }

    #endregion


    #region Constructors
    /// <summary>
    /// Creates instance.
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
            this.CheckDisposed();

            if (this._connection is { IsConnected: false } oldConnection)
            {
                oldConnection.Dispose();
                this._connection = null;
            }

            if (this._connection is null)
            {
                ConnectionMultiplexer? connection = null;

                try
                {
                    //--- create inner connection
                    var stopwatch = Stopwatch.StartNew();
                    connection = ConnectionMultiplexer.Connect(this.Config.Options, this.Logger);
                    stopwatch.Stop();

                    var handler = this.Handler;
                    if (handler is not null)
                    {
                        handler.OnConnectionOpened(this, new(stopwatch.Elapsed));

                        //--- attach events
                        connection.ConfigurationChanged += (_, e) => handler.OnConfigurationChanged(this, e);
                        connection.ConfigurationChangedBroadcast += (_, e) => handler.OnConfigurationChangedBroadcast(this, e);
                        connection.ConnectionFailed += (_, e) => handler.OnConnectionFailed(this, e);
                        connection.ConnectionRestored += (_, e) => handler.OnConnectionRestored(this, e);
                        connection.ErrorMessage += (_, e) => handler.OnErrorMessage(this, e);
                        connection.HashSlotMoved += (_, e) => handler.OnHashSlotMoved(this, e);
                        connection.InternalError += (_, e) => handler.OnInternalError(this, e);
                        connection.ServerMaintenanceEvent += (_, e) => handler.OnServerMaintenanceEvent(this, e);
                    }

                    this._connection = connection;
                }
                catch
                {
                    connection?.Dispose();
                    throw;
                }
            }

            return this._connection;
        }
    }

    private readonly object _gate = new();
    private ConnectionMultiplexer? _connection = null;
    #endregion

    private bool _disposed;

    /// <inheritdoc />
    [EditorBrowsable(EditorBrowsableState.Advanced)]
    public void Dispose()
    {
        lock (this._gate)
        {
            if (this._connection is not null)
            {
                this._connection.Dispose();
                this._connection = null;

                this._disposed = true;
            }
        }
    }

    private void CheckDisposed()
    {
        if (this._disposed)
        {
            throw new ObjectDisposedException(this.GetType().FullName);
        }
    }
}
