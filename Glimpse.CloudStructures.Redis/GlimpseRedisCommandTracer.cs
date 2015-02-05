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
            if (receivedObject != null)
            {
                var t = receivedObject.GetType();
                if (t.IsGenericType && t.GetGenericTypeDefinition() == typeof(RedisResult<>))
                {
                    dynamic d = receivedObject;
                    receivedObject = d.GetValueOrNull();
                }
            }

            timelineRegion.Publish(sentObject, sentSize, receivedObject, receivedSize, isError);
        }
    }
}