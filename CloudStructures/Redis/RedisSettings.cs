using BookSleeve;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CloudStructures.Redis
{
    public class RedisSettings : IDisposable
    {
        public string Host { get; private set; }
        public int Port { get; private set; }
        public int Timeout { get; private set; }
        public int IoTimeout { get; private set; }
        public string Password { get; private set; }
        public int MaxUnsent { get; private set; }
        public bool AllowAdmin { get; private set; }
        public int SyncTimeout { get; private set; }
        public int Db { get; private set; }
        public IRedisValueConverter ValueConverter { get; private set; }
        public IPerformanceMonitor PerformanceMonitor { get; set; }

        public RedisSettings(string host, int port = 6379, int ioTimeout = -1, string password = null, int maxUnsent = 2147483647, bool allowAdmin = false, int syncTimeout = 10000, int db = 0, IRedisValueConverter converter = null)
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
        }

        // Manage Connection

        RedisConnection connection;
        object connectionLock = new object();

        public RedisConnection GetConnection()
        {
            if ((connection == null)
            || (connection.State != RedisConnectionBase.ConnectionState.Open)
            || (connection.State != RedisConnectionBase.ConnectionState.Opening))
            {
                lock (connectionLock)
                {
                    if ((connection == null)
                    || (connection.State != RedisConnectionBase.ConnectionState.Open)
                    || (connection.State != RedisConnectionBase.ConnectionState.Opening))
                    {
                        connection = new RedisConnection(Host, Port, IoTimeout, Password, MaxUnsent, AllowAdmin, SyncTimeout);
                        connection.Open().Wait(); // wait open
                    }
                }
            }

            return connection;
        }

        public void Dispose()
        {
            if (connection != null)
            {
                connection.Dispose();
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