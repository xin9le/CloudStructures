using CloudStructures;
using Glimpse.Core.Message;
using StackExchange.Redis;
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
        public RedisKey Key { get; set; }
        public object SentObject { get; set; }
        public long SentSize { get; set; }
        public object ReceivedObject { get; set; }
        public long ReceivedSize { get; set; }
        public bool IsError { get; set; }
        public RedisSettings UsedSettings { get; set; }

        // interface property

        public TimelineCategoryItem EventCategory { get; set; }
        public string EventName { get; set; }
        public string EventSubText { get; set; }
        public TimeSpan Duration { get; set; }
        public TimeSpan Offset { get; set; }
        public DateTime StartTime { get; set; }

        public RedisTimelineMessage(RedisSettings usedSettings, string command, RedisKey key, object sentObject, long sentSize, object receivedObject, long receivedSize, bool isError)
        {
            UsedSettings = usedSettings;
            Command = command;
            Key = key;
            SentObject = sentObject;
            SentSize = sentSize;
            ReceivedObject = receivedObject;
            ReceivedSize = receivedSize;
            IsError = isError;
        }
    }
}