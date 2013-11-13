using CloudStructures.Redis;
using System.Web;

namespace Glimpse.CloudStructures.Redis
{
    public class RedisProfiler : ICommandTracer
    {
        HttpContext context;
        RedisInspector.TimelineRegion timelineRegion;

        public void CommandStart(RedisSettings usedSettings, string command, string key)
        {
            var context = HttpContext.Current;
            this.timelineRegion = RedisInspector.Start(context, command, key);
        }

        public void CommandFinish(object sendObject, object receivedObject, bool isError)
        {
            timelineRegion.Publish(sendObject, receivedObject, isError);
        }
    }
}