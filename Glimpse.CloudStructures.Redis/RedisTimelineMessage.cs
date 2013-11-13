using Glimpse.Core.Message;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Glimpse.CloudStructures.Redis
{
    public class RedisTimelineMessage : MessageBase, ITimelineMessage
    {
        public string Command { get; set; }
        public string Key { get; set; }
        public object SendObject { get; set; }
        public object ReceivedObject { get; set; }
        public bool IsError { get; set; }

        // interface property

        public TimelineCategoryItem EventCategory { get; set; }
        public string EventName { get; set; }
        public string EventSubText { get; set; }
        public TimeSpan Duration { get; set; }
        public TimeSpan Offset { get; set; }
        public DateTime StartTime { get; set; }

        public RedisTimelineMessage(string command, string key, object sentObject, object receivedObject, bool isError)
        {
            Command = command;
            Key = key;
            SendObject = sentObject;
            ReceivedObject = receivedObject;
            IsError = isError;
        }
    }
}