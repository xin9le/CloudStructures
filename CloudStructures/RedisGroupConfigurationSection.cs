using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration;
using System.Linq;
using System.Linq.Expressions;

namespace CloudStructures
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
            var elem = (element as RedisSettingsElement);
            return Tuple.Create(elem.Configuration, elem.Db);
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
                x.Configuration,
                x.Db,
                x.ValueConverter,
                x.CommandTracer));
            return new RedisGroup(Name, settings.ToArray(), ServerSelector);
        }
    }

    public class RedisSettingsElement : ConfigurationElement
    {
        [ConfigurationProperty("configuration", IsRequired = true)]
        public string Configuration { get { return (string)base["configuration"]; } }

        [ConfigurationProperty("db", DefaultValue = 0)]
        public int Db { get { return (int)base["db"]; } }

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

        [ConfigurationProperty("commandTracer"), TypeConverter(typeof(TypeNameConverter))]
        public Func<ICommandTracer> CommandTracer
        {
            get
            {
                var type = (Type)base["commandTracer"];
                if (type == null) return null;

                try
                {
                    var factory = Expression.Lambda<Func<ICommandTracer>>(Expression.New(type)).Compile();
                    return factory;
                }
                catch (Exception ex)
                {
                    throw new Exception("CommandTracer must needs non parameter constructor", ex);
                }
            }
        }
    }
}