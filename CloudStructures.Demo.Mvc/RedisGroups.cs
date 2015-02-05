using CloudStructures;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Web;

namespace CloudStructures.Demo.Mvc
{
    public static class RedisGroups
    {
        // load from web.config
        static Dictionary<string, RedisGroup> configDict = CloudStructures.CloudStructuresConfigurationSection
            .GetSection()
            .ToRedisGroups()
            .ToDictionary(x => x.GroupName);

        // setup group
        public static readonly RedisGroup Demo = configDict["Demo"];

        static RedisGroups()
        {
            // regisiter
            Glimpse.CloudStructures.Redis.RedisInfoTab.RegisiterConnection(new[] { Demo });
        }
    }
}