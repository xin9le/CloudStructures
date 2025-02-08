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
public sealed class RedisConnection(
    RedisConfig config,
    IValueConverter? converter = null,
    IConnectionEventHandler? handler = null,
    TextWriter? logger = null) : IDisposable
{
    #region Properties
    /// <summary>
    /// Gets configuration.
    /// </summary>
    public RedisConfig Config { get; } = config;


    /// <summary>
    /// Gets value converter.
    /// </summary>
    internal ValueConverter Converter { get; } = new(converter);


    /// <summary>
    /// Gets connection event handler.
    /// </summary>
    private IConnectionEventHandler? Handler { get; } = handler;


    /// <summary>
    /// Gets logger.
    /// </summary>
    private TextWriter? Logger { get; } = logger;


    /// <summary>
    /// Gets an interactive connection to a database inside redis.
    /// </summary>
    /// <exception cref="ObjectDisposedException">
    /// This object has already been disposed.
    /// </exception>
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
    /// <exception cref="ObjectDisposedException">
    /// This object has already been disposed.
    /// </exception>
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
    /// <exception cref="ObjectDisposedException">
    /// This object has already been disposed.
    /// </exception>
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


    #region Connection management
    /// <summary>
    /// Gets underlying connection.
    /// </summary>
    /// <returns></returns>
    /// <exception cref="ObjectDisposedException">
    /// This object has already been disposed.
    /// </exception>
    public ConnectionMultiplexer GetConnection()
    {
        this.CheckDisposed();

        lock (this._gate)
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

                this._connection = connection;
            }
            catch
            {
                connection?.Dispose();
                throw;
            }

            return this._connection;
        }
    }


    /// <summary>
    /// The internal connection is destroyed without destroying this object.
    /// </summary>
    /// <exception cref="ObjectDisposedException">
    /// This object has already been disposed.
    /// </exception>
    /// <remarks>
    /// The internal connection will be recreated the next time the <see cref="GetConnection"/> method is called.
    /// </remarks>
    [EditorBrowsable(EditorBrowsableState.Advanced)]
    public void ReleaseConnection()
    {
        this.CheckDisposed();

        lock (this._gate)
        {
            if (this._connection is not { } connection)
            {
                return;
            }

            connection.ConfigurationChanged -= this.OnConfigurationChanged;
            connection.ConfigurationChangedBroadcast -= this.OnConfigurationChangedBroadcast;
            connection.ConnectionFailed -= this.OnConnectionFailed;
            connection.ConnectionRestored -= this.OnConnectionRestored;
            connection.ErrorMessage -= this.OnErrorMessage;
            connection.HashSlotMoved -= this.OnHashSlotMoved;
            connection.InternalError -= this.OnInternalError;
            connection.ServerMaintenanceEvent -= this.OnServerMaintenanceEvent;

            connection.Dispose();
            this._connection = null;
        }
    }


#if NET9_0_OR_GREATER
    private readonly System.Threading.Lock _gate = new();
#else
    private readonly object _gate = new();
#endif
    private ConnectionMultiplexer? _connection;
#endregion


    #region IDisposable
    /// <inheritdoc />
    void IDisposable.Dispose()
    {
        this._disposed = true;
        this.ReleaseConnection();
    }


    private void CheckDisposed()
    {
#if NET7_0_OR_GREATER
        ObjectDisposedException.ThrowIf(this._disposed, this);
#else
        if (this._disposed)
        {
            throw new ObjectDisposedException(this.GetType().FullName);
        }
#endif
    }


    private bool _disposed;
    #endregion


    #region Event handlers
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
    #endregion
}
