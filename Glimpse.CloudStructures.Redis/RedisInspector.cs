using Glimpse.Core.Extensibility;
using Glimpse.Core.Message;
using System;
using System.Threading;

namespace Glimpse.CloudStructures.Redis
{
    public class RedisInspector : IInspector
    {
        // initialize at once
        static IMessageBroker messageBroker;
        static Func<IExecutionTimer> timerStrategy;

        public void Setup(IInspectorContext context)
        {
            messageBroker = context.MessageBroker;
            timerStrategy = context.TimerStrategy;
        }

        public static TimelineRegion Start(string command, string key)
        {
            return new TimelineRegion(messageBroker, timerStrategy, command, key);
        }

        public class TimelineRegion
        {
            const string Label = "Redis";
            const string Color = "#555";
            const string ColorHighlight = "#55ff55";

            string command;
            string key;

            IExecutionTimer timer;
            IMessageBroker messageBroker;
            TimeSpan offset;
            SynchronizationContext context;

            internal TimelineRegion(IMessageBroker messageBroker, Func<IExecutionTimer> timerStrategy, string command, string key)
            {
                this.command = command;
                this.key = key;

                if (messageBroker == null || timerStrategy == null) return;

                this.messageBroker = messageBroker;
                timer = timerStrategy();

                if (timer == null) return;

                offset = timer.Point().Offset;

                context = SynchronizationContext.Current; // capture context
            }

            public void Publish(object sentObject, object receivedObject, bool isError)
            {
                if (messageBroker != null && timer != null && context != null)
                {
                    var timerResult = timer.Stop(offset);

                    // Publish is called from ConfigureAwait(false) and sometimes messageBroker can't publish at doesn't have syncContext.
                    context.Post(_ =>
                    {
                        var message = new RedisTimelineMessage(command, key, sentObject, receivedObject, isError)
                            .AsTimelineMessage(command + ": " + key, new TimelineCategoryItem(Label, Color, ColorHighlight))
                            .AsTimedMessage(timerResult);

                        messageBroker.Publish(message);
                    }, null);
                }
            }
        }
    }
}