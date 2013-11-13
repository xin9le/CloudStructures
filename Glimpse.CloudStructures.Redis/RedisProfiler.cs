using CloudStructures.Redis;
using System.Web;

namespace Glimpse.CloudStructures.Redis
{
    public class RedisProfiler : ICommandTracer
    {
        RedisInspector.TimelineRegion timelineRegion;

        public void CommandStart(RedisSettings usedSettings, string command, string key)
        {
            this.timelineRegion = RedisInspector.Start(command, key);
        }

        public void CommandFinish(object sendObject, object receivedObject, bool isError)
        {
            timelineRegion.Publish(sendObject, receivedObject, isError);
        }
    }
}