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
            // add event
            foreach (var settings in configDict.SelectMany(x => x.Value.Settings))
            {
                //settings.OnConnectionOpen = (sender, e) =>
                //{
                //    Debug.WriteLine(string.Format("OnOpen {0}:{1} {2}", sender.Host, sender.Port, e.Duration));
                //};

                //settings.OnConnectionClosed = (sender, e) =>
                //{
                //    Debug.WriteLine(string.Format("OnClose {0}:{1}", sender.Host, sender.Port));
                //};

                //settings.OnConnectionError = (sender, e) =>
                //{
                //    Debug.WriteLine(string.Format("OnError {0}:{1} {2} {3}", sender.Host, sender.Port, e.Cause, e.Exception));
                //};

                //settings.OnConnectionShutdown = (sender, e) =>
                //{
                //    Debug.WriteLine(string.Format("OnShutdown {0}:{1} {2} {3}", sender.Host, sender.Port, e.Cause, e.Exception));
                //};
            }
        }
    }
}