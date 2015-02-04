using CloudStructures;
using System.Web;
using System;
using StackExchange.Redis;

namespace Glimpse.CloudStructures.Redis
{
    public class GlimpseRedisCommandTracer : ICommandTracer
    {
        RedisInspector.TimelineRegion timelineRegion;

        public void CommandStart(RedisSettings usedSettings, string command, RedisKey key)
        {
            this.timelineRegion = RedisInspector.Start(usedSettings, command, key);
        }


        public void CommandFinish(object sentObject, long sentSize, object receivedObject, long receivedSize, bool isError)
        {
            timelineRegion.Publish(sentObject, sentSize, receivedObject, receivedSize, isError);
        }
    }
}