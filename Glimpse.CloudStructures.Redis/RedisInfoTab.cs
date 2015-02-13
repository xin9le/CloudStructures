using CloudStructures;
using Glimpse.Core.Extensibility;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Glimpse.CloudStructures.Redis
{
    public class RedisInfoTab : TabBase
    {
        static RedisGroup[] redisGroups;

        public static void RegisiterConnection(RedisGroup[] redisGroups)
        {
            RedisInfoTab.redisGroups = redisGroups;
        }

        public class ServerInfo
        {
            public string Group { get; set; }
            public string EndPoint { get; set; }
            public Dictionary<string, KeyValuePair<string, string>[]> Info { get; set; }
        }

        public class ServerCmdStat
        {
            public string Group { get; set; }
            public string EndPoint { get; set; }
            public Dictionary<string, object> CmdStat { get; set; }
        }

        public class ServerConfig
        {
            public string Group { get; set; }
            public string EndPoint { get; set; }
            public KeyValuePair<string, string>[] Config { get; set; }
        }

        public class RedisInfoModel
        {
            public ServerInfo[] ServerInfo { get; set; }
            public ServerCmdStat[] ServerCmdStat { get; set; }
            public ServerConfig[] ServerConfig { get; set; }
            public RedisSettingsInfoModel[] SettingsInfo { get; set; }
        }

        public class RedisSettingsInfoModel
        {
            public string Group { get; set; }
            public ConfigurationOptions ConfigurationOptions { get; set; }
            public int Db { get; set; }
            public string ValueConverter { get; set; }
            public string CommandTracer { get; set; }
            public StackExchange.Redis.ServerCounters Counters { get; set; }
        }

        public override string Name
        {
            get { return "RedisInfo"; }
        }

        public override object GetData(ITabContext context)
        {
            if (redisGroups == null) return null;

            var servers = redisGroups
                .SelectMany(x => x.Settings, (x, y) => new { x.GroupName, Settings = y })
                .Select(x => new { x.GroupName, x.Settings, Conn = x.Settings.GetConnection() })
                .SelectMany(x => x.Conn.GetEndPoints(), (x, y) => new { settings = x, endPoint = y })
                .Distinct(x => x.endPoint.ToString())
                .Select(x => new { x.endPoint, x.settings, server = x.settings.Conn.GetServer(x.endPoint) })
                .ToArray();

            ServerInfo[] infos = new ServerInfo[0];
            try
            {
                infos = servers
                    .Select(async x => new { x.endPoint, x.settings, info = await x.server.InfoAsync("default").ConfigureAwait(false) })
                    .WhenAll()
                    .Result
                    .Select(x => new ServerInfo { EndPoint = x.endPoint.ToString(), Group = x.settings.GroupName, Info = x.info.ToDictionary(y => y.Key, y => y.ToArray()) })
                    .ToArray();
            }
            catch { }

            ServerCmdStat[] cmdstats = new ServerCmdStat[0];
            try
            {
                cmdstats = servers
                    .Select(async x => new { x.endPoint, x.settings, info = await x.server.InfoAsync("commandstats").ConfigureAwait(false) })
                    .WhenAll()
                    .Result
                    .Select(x =>
                    {
                        var info = x.info.First().ToDictionary(y => y.Key, y =>
                        {
                            var sp = y.Value.Split(',');
                            if (sp.Length <= 1) return y.Value;

                            return (object)sp.Select(z =>
                            {
                                var sp2 = z.Split('=');
                                if (sp2.Length == 2)
                                {
                                    return (object)new { Key = sp2[0], Value = sp2[1] };
                                }
                                else
                                {
                                    return z;
                                }
                            })
                            .ToArray();
                        });

                        return new ServerCmdStat { Group = x.settings.GroupName, EndPoint = x.endPoint.ToString(), CmdStat = info };
                    })
                    .ToArray();
            }
            catch { }

            ServerConfig[] configs = new ServerConfig[0];
            try
            {
                configs = servers
                   .Select(async x => new { x.endPoint, x.settings, config = await x.server.ConfigGetAsync("*").ConfigureAwait(false) })
                   .WhenAll()
                   .Result
                   .Select(x =>
                   {
                       return new ServerConfig { Config = x.config, Group = x.settings.GroupName, EndPoint = x.endPoint.ToString() };
                   })
                   .ToArray();
            }
            catch { }

            var settingsInfos = redisGroups
                .SelectMany(x => x.Settings, (x, y) => new { Group = x.GroupName, Settings = y })
                .Select(x => new RedisSettingsInfoModel
                {
                    Group = x.Group,
                    ConfigurationOptions = x.Settings.ConfigurationOptions,
                    Db = x.Settings.Db,
                    ValueConverter = (x.Settings.ValueConverter == null) ? null : x.Settings.ValueConverter.GetType().Name,
                    CommandTracer = (x.Settings.CommandTracerFactory == null) ? null : x.Settings.CommandTracerFactory().GetType().Name,
                    Counters = x.Settings.GetConnection().GetCounters()
                })
                .ToArray();

            return new RedisInfoModel
            {
                ServerInfo = infos,
                ServerConfig = configs,
                ServerCmdStat = cmdstats,
                SettingsInfo = settingsInfos
            };
        }
    }

    internal static class AnonymousComparer
    {
        public static IEnumerable<TSource> Distinct<TSource, TCompareKey>(this IEnumerable<TSource> source, Func<TSource, TCompareKey> compareKeySelector)
        {
            return source.Distinct(AnonymousComparer.Create(compareKeySelector));
        }

        public static IEqualityComparer<T> Create<T, TKey>(Func<T, TKey> compareKeySelector)
        {
            if (compareKeySelector == null) throw new ArgumentNullException("compareKeySelector");

            return new EqualityComparer<T>(
                (x, y) =>
                {
                    if (object.ReferenceEquals(x, y)) return true;
                    if (x == null || y == null) return false;
                    return compareKeySelector(x).Equals(compareKeySelector(y));
                },
                obj =>
                {
                    if (obj == null) return 0;
                    return compareKeySelector(obj).GetHashCode();
                });
        }

        public static IEqualityComparer<T> Create<T>(Func<T, T, bool> equals, Func<T, int> getHashCode)
        {
            if (equals == null) throw new ArgumentNullException("equals");
            if (getHashCode == null) throw new ArgumentNullException("getHashCode");

            return new EqualityComparer<T>(equals, getHashCode);
        }

        private class EqualityComparer<T> : IEqualityComparer<T>
        {
            private readonly Func<T, T, bool> equals;
            private readonly Func<T, int> getHashCode;

            public EqualityComparer(Func<T, T, bool> equals, Func<T, int> getHashCode)
            {
                this.equals = equals;
                this.getHashCode = getHashCode;
            }

            public bool Equals(T x, T y)
            {
                return equals(x, y);
            }

            public int GetHashCode(T obj)
            {
                return getHashCode(obj);
            }
        }
    }

    internal static class TaskEx
    {
        public static Task<T[]> WhenAll<T>(this IEnumerable<Task<T>> tasks)
        {
            return Task.WhenAll(tasks);
        }
    }
}