using CloudStructures;
using System.Web;
using System;
using StackExchange.Redis;
using Glimpse.Core.Extensibility;
using Glimpse.Core.Framework;
using Glimpse.Core.Message;

namespace Glimpse.CloudStructures.Redis
{
    public class GlimpseRedisCommandTracer : ICommandTracer
    {
        private IMessageBroker _messageBroker;
        private IExecutionTimer _timerStrategy;

#pragma warning disable 618

        internal IMessageBroker MessageBroker
        {
            get { return _messageBroker ?? (_messageBroker = GlimpseConfiguration.GetConfiguredMessageBroker()); }
            set { _messageBroker = value; }
        }

        internal IExecutionTimer TimerStrategy
        {
            get { return _timerStrategy ?? (_timerStrategy = GlimpseConfiguration.GetConfiguredTimerStrategy()()); }
            set { _timerStrategy = value; }
        }

#pragma warning restore 618

        const string Label = "Redis";
        const string Color = "#555";
        const string ColorHighlight = "#55ff55";

        string command;
        RedisKey key;
        RedisSettings usedSettings;
        TimeSpan start;

        public void CommandStart(RedisSettings usedSettings, string command, RedisKey key)
        {
            if (TimerStrategy == null) return;
            this.start = TimerStrategy.Start();

            this.usedSettings = usedSettings;
            this.command = command;
            this.key = key;
        }


        public void CommandFinish(object sentObject, long sentSize, object receivedObject, long receivedSize, bool isError)
        {
            if (TimerStrategy == null || MessageBroker == null) return;

            var timerResult = TimerStrategy.Stop(start);
            if (receivedObject != null)
            {
                var t = receivedObject.GetType();
                if (t.IsGenericType && t.GetGenericTypeDefinition() == typeof(RedisResult<>))
                {
                    dynamic d = receivedObject;
                    receivedObject = d.GetValueOrNull();
                }
            }

            var message = new RedisTimelineMessage(usedSettings, command, key, sentObject, sentSize, receivedObject, receivedSize, isError)
                 .AsTimelineMessage(command + ": " + (string)key, new TimelineCategoryItem(Label, Color, ColorHighlight))
                 .AsTimedMessage(timerResult);

            MessageBroker.Publish(message);
        }
    }
}