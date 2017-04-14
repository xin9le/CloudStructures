using StackExchange.Redis;
using System;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;

namespace CloudStructures
{
    public class RedisSettings
    {
        readonly ConfigurationOptions configuration;
        readonly System.IO.TextWriter connectionMultiplexerLog;

        ConnectionMultiplexer connection;
        readonly object connectionLock = new object();

        public int Db { get; private set; }
        public ConfigurationOptions ConfigurationOptions { get { return configuration; } }
        public IRedisValueConverter ValueConverter { get; private set; }
        public Func<ICommandTracer> CommandTracerFactory { get; private set; }

        // events
        public Action<OpenConnectionEventArgs> OnConnectionOpen { private get; set; }
        public Action<OpenConnectionFailedEventArgs> OnConnectionOpenFailed { private get; set; }
        public Action<OpenConnectionFailedEventArgs> OnConnectAsyncFailed { private get; set; }
        public Action<ConnectionMultiplexer, EndPointEventArgs> OnConfigurationChanged { private get; set; }
        public Action<ConnectionMultiplexer, EndPointEventArgs> OnConfigurationChangedBroadcast { private get; set; }
        public Action<ConnectionMultiplexer, ConnectionFailedEventArgs> OnConnectionFailed { private get; set; }
        public Action<ConnectionMultiplexer, ConnectionFailedEventArgs> OnConnectionRestored { private get; set; }
        public Action<ConnectionMultiplexer, RedisErrorEventArgs> OnErrorMessage { private get; set; }
        public Action<ConnectionMultiplexer, HashSlotMovedEventArgs> OnHashSlotMoved { private get; set; }
        public Action<ConnectionMultiplexer, InternalErrorEventArgs> OnInternalError { private get; set; }

        public RedisSettings(string connectionString, int db = 0, IRedisValueConverter converter = null, Func<ICommandTracer> tracerFactory = null, System.IO.TextWriter connectionMultiplexerLog = null)
            : this(ConfigurationOptions.Parse(connectionString), db, converter, tracerFactory, connectionMultiplexerLog)
        {
        }

        public RedisSettings(ConfigurationOptions configuration, int db = 0, IRedisValueConverter converter = null, Func<ICommandTracer> tracerFactory = null, System.IO.TextWriter connectionMultiplexerLog = null)
        {
            this.configuration = configuration;
            this.Db = db;
            this.ValueConverter = converter ?? new JsonRedisValueConverter();
            this.CommandTracerFactory = tracerFactory;
            this.connectionMultiplexerLog = connectionMultiplexerLog;
        }

        public ConnectionMultiplexer GetConnection()
        {
            lock (connectionLock)
            {
                if (connection != null && connection.IsConnected)
                {
                    return connection;
                }

                var sw = Stopwatch.StartNew();
                connection = ConnectionMultiplexer.Connect(configuration, connectionMultiplexerLog);
                sw.Stop();
                try
                {
                    OnConnectionOpen?.Invoke(new OpenConnectionEventArgs(configuration, sw.Elapsed));

                    // attach events
                    connection.ConfigurationChanged += connection_ConfigurationChanged;
                    connection.ConfigurationChangedBroadcast += connection_ConfigurationChangedBroadcast;
                    connection.ConnectionFailed += connection_ConnectionFailed;
                    connection.ConnectionRestored += connection_ConnectionRestored;
                    connection.ErrorMessage += connection_ErrorMessage;
                    connection.HashSlotMoved += connection_HashSlotMoved;
                    connection.InternalError += connection_InternalError;
                }
                catch
                {
                    connection = null;
                    throw;
                }
            }

            return connection;
        }

        void connection_InternalError(object sender, InternalErrorEventArgs e)
        {
            var ev = OnInternalError;
            if (ev != null) ev(sender as ConnectionMultiplexer, e);
        }

        void connection_HashSlotMoved(object sender, HashSlotMovedEventArgs e)
        {
            var ev = OnHashSlotMoved;
            if (ev != null) ev(sender as ConnectionMultiplexer, e);
        }

        void connection_ErrorMessage(object sender, RedisErrorEventArgs e)
        {
            var ev = OnErrorMessage;
            if (ev != null) ev(sender as ConnectionMultiplexer, e);
        }

        void connection_ConnectionRestored(object sender, ConnectionFailedEventArgs e)
        {
            var ev = OnConnectionRestored;
            if (ev != null) ev(sender as ConnectionMultiplexer, e);
        }

        void connection_ConnectionFailed(object sender, ConnectionFailedEventArgs e)
        {
            var ev = OnConnectionFailed;
            if (ev != null) ev(sender as ConnectionMultiplexer, e);
        }

        void connection_ConfigurationChangedBroadcast(object sender, EndPointEventArgs e)
        {
            var ev = OnConfigurationChangedBroadcast;
            if (ev != null) ev(sender as ConnectionMultiplexer, e);
        }

        void connection_ConfigurationChanged(object sender, EndPointEventArgs e)
        {
            var ev = OnConfigurationChanged;
            if (ev != null) ev(sender as ConnectionMultiplexer, e);
        }

        // shortcut

        /// <summary>Create RedisString used by this settings.</summary>
        public RedisString<T> String<T>(RedisKey key)
        {
            return new RedisString<T>(this, key);
        }

        /// <summary>Create RedisList used by this settings.</summary>
        public RedisList<T> List<T>(RedisKey key)
        {
            return new RedisList<T>(this, key);
        }
        /// <summary>Create RedisSet used by this settings.</summary>
        public RedisSet<T> Set<T>(RedisKey key)
        {
            return new RedisSet<T>(this, key);
        }

        /// <summary>Create RedisSortedSet used by this settings.</summary>
        public RedisSortedSet<T> SortedSet<T>(RedisKey key)
        {
            return new RedisSortedSet<T>(this, key);
        }

        /// <summary>Create RedisHash used by this settings.</summary>
        public RedisHash<TKey> Hash<TKey>(RedisKey key)
        {
            return new RedisHash<TKey>(this, key);
        }

        /// <summary>Create RedisDictionary used by this settings.</summary>
        public RedisDictionary<TKey, TValue> Dictionary<TKey, TValue>(RedisKey key)
        {
            return new RedisDictionary<TKey, TValue>(this, key);
        }

        /// <summary>Create RedisClass used by this settings.</summary>
        public RedisClass<T> Class<T>(RedisKey key) where T : class, new()
        {
            return new RedisClass<T>(this, key);
        }

        /// <summary>Create RedisHyperLogLog used by this settings.</summary>
        public RedisHyperLogLog<T> HyperLogLog<T>(RedisKey key)
        {
            return new RedisHyperLogLog<T>(this, key);
        }

        /// <summary>Create RedisLua used by this settings.</summary>
        public RedisLua Lua(RedisKey key)
        {
            return new RedisLua(this, key);
        }

        public IServer[] Servers()
        {
            var con = GetConnection();
            return ConfigurationOptions.EndPoints.Select(x => con.GetServer(x)).ToArray();
        }

        public class OpenConnectionEventArgs : EventArgs
        {
            public ConfigurationOptions ConfigurationOption { get; private set; }

            public TimeSpan Duration { get; private set; }

            public OpenConnectionEventArgs(ConfigurationOptions configurationOptions, TimeSpan duration)
            {
                this.ConfigurationOption = configurationOptions;
                this.Duration = duration;
            }
        }

        public class OpenConnectionFailedEventArgs : EventArgs
        {
            public ConfigurationOptions ConfigurationOption { get; private set; }

            public TimeSpan Duration { get; private set; }

            public Exception Exception { get; private set; }

            public OpenConnectionFailedEventArgs(ConfigurationOptions configurationOption, TimeSpan duration, Exception exception)
            {
                this.ConfigurationOption = configurationOption;
                this.Duration = duration;
                this.Exception = exception;
            }
        }
    }
}