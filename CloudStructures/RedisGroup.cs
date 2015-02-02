using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CloudStructures
{
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

        public RedisSettings GetSettings(RedisKey key)
        {
            return serverSelector.Select(Settings, key);
        }

        // shortcut

        ///// <summary>Create RedisString used by this group.</summary>
        //public RedisString<T> String<T>(RedisKey key)
        //{
        //    return new RedisString<T>(this, key);
        //}

        ///// <summary>Create RedisList used by this group.</summary>
        //public RedisList<T> List<T>(RedisKey key)
        //{
        //    return new RedisList<T>(this, key);
        //}
        ///// <summary>Create RedisSet used by this group.</summary>
        //public RedisSet<T> Set<T>(RedisKey key)
        //{
        //    return new RedisSet<T>(this, key);
        //}

        ///// <summary>Create RedisSortedSet used by this group.</summary>
        //public RedisSortedSet<T> SortedSet<T>(RedisKey key)
        //{
        //    return new RedisSortedSet<T>(this, key);
        //}

        ///// <summary>Create RedisHash used by this group.</summary>
        //public RedisHash Hash(RedisKey key)
        //{
        //    return new RedisHash(this, key);
        //}

        ///// <summary>Create RedisDictionary used by this group.</summary>
        //public RedisDictionary<T> Dictionary<T>(RedisKey key)
        //{
        //    return new RedisDictionary<T>(this, key);
        //}

        ///// <summary>Create RedisClass used by this group.</summary>
        //public RedisClass<T> Class<T>(RedisKey key) where T : class, new()
        //{
        //    return new RedisClass<T>(this, key);
        //}

        /// <summary>Create RedisHyperLogLog used by this group.</summary>
        public RedisHyperLogLog<T> HyperLogLog<T>(RedisKey key)
        {
            return new RedisHyperLogLog<T>(this, key);
        }
    }
}
