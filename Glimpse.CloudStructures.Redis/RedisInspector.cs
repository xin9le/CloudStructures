using Glimpse.Core.Extensibility;
using Glimpse.Core.Message;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Web;

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

        // TODO:Contextは必要か？
        public static TimelineRegion Start(HttpContext httpContext, string command, string key)
        {
            return new TimelineRegion(messageBroker, timerStrategy, httpContext, command, key);
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
            SynchronizationContext synchronizationContext;
            TimeSpan offset;

            internal TimelineRegion(IMessageBroker messageBroker, Func<IExecutionTimer> timerStrategy, HttpContext httpContext, string command, string key)
            {
                this.command = command;
                this.key = key;

                if (messageBroker == null || timerStrategy == null) return;
                if (httpContext == null) return;

                this.messageBroker = messageBroker;
                timer = timerStrategy();

                if (timer == null) return;

                offset = timer.Point().Offset;

                synchronizationContext = SynchronizationContext.Current;
            }

            public void Publish(object sentObject, object receivedObject, bool isError)
            {
                if (messageBroker != null && timer != null)
                {
                    var timerResult = timer.Stop(offset);

                    synchronizationContext.Post(state =>
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