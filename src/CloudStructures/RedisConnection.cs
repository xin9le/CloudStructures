using System;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Linq;
using CloudStructures.Converters;
using CloudStructures.Internals;
using StackExchange.Redis;
using StackExchange.Redis.Maintenance;

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
        this.CheckDisposed();

        lock (this._gate)
        {
            this._connection = CreateConnectionCore();
            return this._connection;
        }

        ConnectionMultiplexer CreateConnectionCore()
        {
            ConnectionMultiplexer? connection = null;

            try
            {
                //--- create inner connection
                var stopwatch = Stopwatch.StartNew();
                connection = ConnectionMultiplexer.Connect(this.Config.Options, this.Logger);
                stopwatch.Stop();

                if (this.Handler is { } handler)
                {
                    handler.OnConnectionOpened(this, new(stopwatch.Elapsed));

                    //--- attach events
                    connection.ConfigurationChanged += this.OnConfigurationChanged;
                    connection.ConfigurationChangedBroadcast += this.OnConfigurationChangedBroadcast;
                    connection.ConnectionFailed += this.OnConnectionFailed;
                    connection.ConnectionRestored += this.OnConnectionRestored;
                    connection.ErrorMessage += this.OnErrorMessage;
                    connection.HashSlotMoved += this.OnHashSlotMoved;
                    connection.InternalError += this.OnInternalError;
                    connection.ServerMaintenanceEvent += this.OnServerMaintenanceEvent;
                }

                return connection;
            }
            catch
            {
                connection?.Dispose();
                throw;
            }
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

    private void OnConfigurationChanged(object? sender, EndPointEventArgs e)
        => this.Handler?.OnConfigurationChanged(this, e);

    private void OnConfigurationChangedBroadcast(object? sender, EndPointEventArgs e)
        => this.Handler?.OnConfigurationChangedBroadcast(this, e);

    private void OnConnectionFailed(object? sender, ConnectionFailedEventArgs e)
        => this.Handler?.OnConnectionFailed(this, e);

    private void OnConnectionRestored(object? sender, ConnectionFailedEventArgs e)
        => this.Handler?.OnConnectionRestored(this, e);

    private void OnErrorMessage(object? sender, RedisErrorEventArgs e)
        => this.Handler?.OnErrorMessage(this, e);

    private void OnHashSlotMoved(object? sender, HashSlotMovedEventArgs e)
        => this.Handler?.OnHashSlotMoved(this, e);

    private void OnInternalError(object? sender, InternalErrorEventArgs e)
        => this.Handler?.OnInternalError(this, e);

    private void OnServerMaintenanceEvent(object? sender, ServerMaintenanceEvent e)
        => this.Handler?.OnServerMaintenanceEvent(this, e);
}
