using BookSleeve;
using System;
using System.Diagnostics;
using System.Text;
using System.Threading;

namespace CloudStructures.Redis
{
    public class RedisSettings : IDisposable
    {
        public string Host { get; private set; }
        public int Port { get; private set; }
        public int IoTimeout { get; private set; }
        public string Password { get; private set; }
        public int MaxUnsent { get; private set; }
        public bool AllowAdmin { get; private set; }
        public int SyncTimeout { get; private set; }
        public int Db { get; private set; }
        public IRedisValueConverter ValueConverter { get; private set; }
        public Func<ICommandTracer> CommandTracerFactory { get; private set; }

        public Action<RedisSettings, OpenConnectionEventArgs> OnConnectionOpen { private get; set; }
        public Action<RedisConnectionBase, ErrorEventArgs> OnConnectionError { private get; set; }
        public Action<RedisConnectionBase, EventArgs> OnConnectionClosed { private get; set; }
        public Action<RedisConnectionBase, ErrorEventArgs> OnConnectionShutdown { private get; set; }

        public RedisSettings(string host, int port = 6379, int ioTimeout = -1, string password = null, int maxUnsent = 2147483647, bool allowAdmin = false, int syncTimeout = 10000, int db = 0, IRedisValueConverter converter = null, Func<ICommandTracer> tracerFactory = null)
        {
            this.Host = host;
            this.Port = port;
            this.IoTimeout = ioTimeout;
            this.Password = password;
            this.MaxUnsent = maxUnsent;
            this.AllowAdmin = allowAdmin;
            this.SyncTimeout = syncTimeout;
            this.Db = db;
            this.ValueConverter = converter ?? new JsonRedisValueConverter();
            this.CommandTracerFactory = tracerFactory;
        }

        // Manage Connection

        RedisConnection connection;
        object connectionLock = new object();

        public RedisConnection GetConnection(bool waitOpen = true)
        {
            if ((connection == null)
            || ((connection.State != RedisConnectionBase.ConnectionState.Open) && (connection.State != RedisConnectionBase.ConnectionState.Opening)))
            {
                lock (connectionLock)
                {
                    if ((connection == null)
                    || ((connection.State != RedisConnectionBase.ConnectionState.Open) && (connection.State != RedisConnectionBase.ConnectionState.Opening)))
                    {
                        connection = new RedisConnection(Host, Port, IoTimeout, Password, MaxUnsent, AllowAdmin, SyncTimeout);

                        // attach events
                        connection.Error += connection_Error;
                        connection.Closed += connection_Closed;
                        connection.Shutdown += connection_Shutdown;

                        // tracing
                        var sw = Stopwatch.StartNew();
                        var traceEntered = -1; // not entered

                        var open = connection.Open() // open connection!
                            .ContinueWith((x) =>
                            {
                                if (Interlocked.Increment(ref traceEntered) == 0)
                                {
                                    sw.Stop();
                                    var ev = OnConnectionOpen;
                                    if (ev != null)
                                    {
                                        OnConnectionOpen(this, new OpenConnectionEventArgs(sw.Elapsed, isFatal: !x.IsCompleted, exception: x.Exception));
                                    }
                                }
                            });

                        if (waitOpen)
                        {
                            try
                            {
                                connection.Wait(open); // wait with SyncTimeout, if timeout throw TimeoutException
                            }
                            catch (Exception ex)
                            {
                                if (Interlocked.Increment(ref traceEntered) == 0)
                                {
                                    sw.Stop();
                                    var ev = OnConnectionOpen;
                                    if (ev != null)
                                    {
                                        OnConnectionOpen(this, new OpenConnectionEventArgs(sw.Elapsed, isFatal: true, exception: ex));
                                    }
                                }
                            }
                        }
                    }
                }
            }

            return connection;
        }

        void connection_Shutdown(object sender, ErrorEventArgs e)
        {
            var ev = OnConnectionShutdown;
            if (ev != null)
            {
                ev(sender as RedisConnectionBase, e);
            }
        }

        void connection_Error(object sender, ErrorEventArgs e)
        {
            var ev = OnConnectionError;
            if (ev != null)
            {
                ev(sender as RedisConnectionBase, e);
            }
        }
        void connection_Closed(object sender, EventArgs e)
        {
            var ev = OnConnectionClosed;
            if (ev != null)
            {
                ev(sender as RedisConnectionBase, e);
            }
        }

        public void Dispose()
        {
            if (connection != null)
            {
                connection.Dispose();
            }
        }

        // shortcut

        /// <summary>Create RedisString used by this settings.</summary>
        public RedisString<T> String<T>(string key)
        {
            return new RedisString<T>(this, key);
        }

        /// <summary>Create RedisList used by this settings.</summary>
        public RedisList<T> List<T>(string key)
        {
            return new RedisList<T>(this, key);
        }
        /// <summary>Create RedisSet used by this settings.</summary>
        public RedisSet<T> Set<T>(string key)
        {
            return new RedisSet<T>(this, key);
        }

        /// <summary>Create RedisSortedSet used by this settings.</summary>
        public RedisSortedSet<T> SortedSet<T>(string key)
        {
            return new RedisSortedSet<T>(this, key);
        }

        /// <summary>Create RedisHash used by this settings.</summary>
        public RedisHash Hash(string key)
        {
            return new RedisHash(this, key);
        }

        /// <summary>Create RedisDictionary used by this settings.</summary>
        public RedisDictionary<T> Dictionary<T>(string key)
        {
            return new RedisDictionary<T>(this, key);
        }

        /// <summary>Create RedisClass used by this settings.</summary>
        public RedisClass<T> Class<T>(string key) where T : class, new()
        {
            return new RedisClass<T>(this, key);
        }

        public class OpenConnectionEventArgs : EventArgs
        {
            public TimeSpan Duration { get; private set; }
            public Exception Exception { get; private set; }
            public bool IsFatal { get; private set; }

            public OpenConnectionEventArgs(TimeSpan duration, bool isFatal, Exception exception)
            {
                this.Duration = duration;
                this.IsFatal = isFatal;
                this.Exception = exception;
            }
        }
    }

    public class RedisGroup
    {
        public string GroupName { get; private set; }
        public RedisSettings[] Settings { get; private set; }
        IServerSelector serverSelector;

        public RedisGroup(string groupName, RedisSettings[] settings, IServerSelector selector = null)
        {
            this.GroupName = groupName;
            this.Settings = settings;
            this.serverSelector = selector ?? new SimpleHashingSelector();
        }

        public RedisSettings GetSettings(string key)
        {
            return serverSelector.Select(Settings, key);
        }

        // shortcut

        /// <summary>Create RedisString used by this group.</summary>
        public RedisString<T> String<T>(string key)
        {
            return new RedisString<T>(this, key);
        }

        /// <summary>Create RedisList used by this group.</summary>
        public RedisList<T> List<T>(string key)
        {
            return new RedisList<T>(this, key);
        }
        /// <summary>Create RedisSet used by this group.</summary>
        public RedisSet<T> Set<T>(string key)
        {
            return new RedisSet<T>(this, key);
        }

        /// <summary>Create RedisSortedSet used by this group.</summary>
        public RedisSortedSet<T> SortedSet<T>(string key)
        {
            return new RedisSortedSet<T>(this, key);
        }

        /// <summary>Create RedisHash used by this group.</summary>
        public RedisHash Hash(string key)
        {
            return new RedisHash(this, key);
        }

        /// <summary>Create RedisDictionary used by this group.</summary>
        public RedisDictionary<T> Dictionary<T>(string key)
        {
            return new RedisDictionary<T>(this, key);
        }

        /// <summary>Create RedisClass used by this group.</summary>
        public RedisClass<T> Class<T>(string key) where T : class, new()
        {
            return new RedisClass<T>(this, key);
        }
    }

    public interface IServerSelector
    {
        RedisSettings Select(RedisSettings[] settings, string key);
    }

    public class SimpleHashingSelector : IServerSelector
    {
        public RedisSettings Select(RedisSettings[] settings, string key)
        {
            if (settings.Length == 0) throw new ArgumentException("settings length is 0");
            if (settings.Length == 1) return settings[0];

            using (var md5 = new System.Security.Cryptography.MD5CryptoServiceProvider())
            {
                var hashBytes = md5.ComputeHash(Encoding.UTF8.GetBytes(key));
                var seed = System.Math.Abs(BitConverter.ToInt32(hashBytes, 0));
                var index = seed % settings.Length;
                return settings[index];
            }
        }
    }
}