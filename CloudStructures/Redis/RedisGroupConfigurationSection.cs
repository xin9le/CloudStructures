using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration;
using System.Linq;

namespace CloudStructures.Redis
{
    public class CloudStructuresConfigurationSection : ConfigurationSection
    {
        [ConfigurationProperty("redis")]
        [ConfigurationCollection(typeof(RedisGroupElementCollection), AddItemName = "group")]
        public RedisGroupElementCollection Groups
        {
            get
            {
                return (RedisGroupElementCollection)base["redis"];
            }
        }

        public static CloudStructuresConfigurationSection GetSection()
        {
            return ConfigurationManager.GetSection("cloudStructures") as CloudStructuresConfigurationSection;
        }

        public RedisGroup[] ToRedisGroups()
        {
            return Groups.Select(x => x.ToRedisGroup()).ToArray();
        }
    }

    [ConfigurationCollection(typeof(RedisGroupElement))]
    public class RedisGroupElementCollection : ConfigurationElementCollection, IEnumerable<RedisGroupElement>
    {
        protected override object GetElementKey(ConfigurationElement element)
        {
            return (element as RedisGroupElement).Name;
        }

        protected override ConfigurationElement CreateNewElement()
        {
            return new RedisGroupElement();
        }

        public new IEnumerator<RedisGroupElement> GetEnumerator()
        {
            var e = base.GetEnumerator();
            while (e.MoveNext())
            {
                yield return (RedisGroupElement)e.Current;
            }
        }
    }

    [ConfigurationCollection(typeof(RedisSettingsElement))]
    public class RedisGroupElement : ConfigurationElementCollection, IEnumerable<RedisSettingsElement>
    {
        [ConfigurationProperty("name")]
        public string Name
        {
            get { return base["name"] as string; }
        }

        [ConfigurationProperty("serverSelector"), TypeConverter(typeof(TypeNameConverter))]
        public IServerSelector ServerSelector
        {
            get
            {
                var type = (Type)base["serverSelector"];
                if (type == null) return null;

                return (IServerSelector)Activator.CreateInstance(type);
            }
        }

        protected override ConfigurationElement CreateNewElement()
        {
            return new RedisSettingsElement();
        }

        protected override object GetElementKey(ConfigurationElement element)
        {
            return (element as RedisSettingsElement).Host;
        }

        public new IEnumerator<RedisSettingsElement> GetEnumerator()
        {
            var e = base.GetEnumerator();
            while (e.MoveNext())
            {
                yield return (RedisSettingsElement)e.Current;
            }
        }

        public RedisGroup ToRedisGroup()
        {
            var settings = this.Select(x => new RedisSettings(
                x.Host,
                x.Port ?? 6379,
                x.IoTimeout ?? -1,
                string.IsNullOrEmpty(x.Password) ? null : x.Password,
                x.MaxUnsent ?? 2147483647,
                x.AllowAdmin ?? false,
                x.SyncTimeout ?? 10000,
                x.Db ?? 0,
                x.ValueConverter));
            return new RedisGroup(Name, settings.ToArray(), ServerSelector);
        }
    }

    public class RedisSettingsElement : ConfigurationElement
    {
        [ConfigurationProperty("host", IsRequired = true, IsKey = true)]
        public string Host { get { return (string)base["host"]; } }

        [ConfigurationProperty("port")]
        public int? Port { get { return (int?)base["port"]; } }

        [ConfigurationProperty("ioTimeout")]
        public int? IoTimeout { get { return (int?)base["ioTimeout"]; } }

        [ConfigurationProperty("password")]
        public string Password { get { return (string)base["password"]; } }

        [ConfigurationProperty("maxUnsent")]
        public int? MaxUnsent { get { return (int?)base["maxUnsent"]; } }

        [ConfigurationProperty("allowAdmin")]
        public bool? AllowAdmin { get { return (bool?)base["allowAdmin"]; } }

        [ConfigurationProperty("syncTimeout")]
        public int? SyncTimeout { get { return (int?)base["syncTimeout"]; } }

        [ConfigurationProperty("db")]
        public int? Db { get { return (int?)base["db"]; } }

        [ConfigurationProperty("valueConverter"), TypeConverter(typeof(TypeNameConverter))]
        public IRedisValueConverter ValueConverter
        {
            get
            {
                var type = (Type)base["valueConverter"];
                if (type == null) return null;

                return (IRedisValueConverter)Activator.CreateInstance(type);
            }
        }
    }
}