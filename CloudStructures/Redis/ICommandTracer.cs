using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace CloudStructures.Redis
{
    public interface ICommandTracer
    {
        void CommandStart(RedisSettings usedSettings, string command, string key);
        void CommandFinish(object sentObject, object receivedObject, bool isError);
    }

    internal static class Pair
    {
        public static Pair<T> Create<T>(object sentObject, T receivedObject)
        {
            return new Pair<T> { SentObject = sentObject, ReceivedObject = receivedObject };
        }
    }

    internal class Pair<T>
    {
        public object SentObject { get; set; }
        public T ReceivedObject { get; set; }
    }

    internal static class TraceHelper
    {
        public static async Task RecordSend(RedisSettings usedSettings, string key, string callType, Func<Task<object>> executeAndReturnSentObject, [CallerMemberName]string commandName = "")
        {
            var tracerFactory = usedSettings.CommandTracerFactory;
            ICommandTracer tracer = null;
            if (tracerFactory != null)
            {
                tracer = tracerFactory();
                var command = callType + "." + commandName;

                tracer.CommandStart(usedSettings, command, key); // start within context
            }

            object sendObject = null;
            bool isError = true;
            try
            {
                sendObject = await executeAndReturnSentObject().ConfigureAwait(false);
                isError = false;
            }
            finally
            {
                if (tracer != null)
                {
                    tracer.CommandFinish(sendObject, null, isError); // finish without context
                }
            }
        }
        public static async Task<T> RecordReceive<T>(RedisSettings usedSettings, string key, string callType, Func<Task<T>> executeAndReturnReceivedObject, [CallerMemberName]string commandName = "")
        {
            var tracerFactory = usedSettings.CommandTracerFactory;
            ICommandTracer tracer = null;
            if (tracerFactory != null)
            {
                tracer = tracerFactory();
                var command = callType + "." + commandName;

                tracer.CommandStart(usedSettings, command, key); // start within context
            }

            T receivedObject = default(T);
            bool isError = true;
            try
            {
                receivedObject = await executeAndReturnReceivedObject().ConfigureAwait(false);
                isError = false;
            }
            finally
            {
                if (tracer != null)
                {
                    tracer.CommandFinish(null, receivedObject, isError); // finish without context
                }
            }

            return receivedObject;
        }

        public static async Task<T> RecordSendAndReceive<T>(RedisSettings usedSettings, string key, string callType, Func<Task<Pair<T>>> executeAndReturnSentAndReceivedObject, [CallerMemberName]string commandName = "")
        {
            var tracerFactory = usedSettings.CommandTracerFactory;
            ICommandTracer tracer = null;
            if (tracerFactory != null)
            {
                tracer = tracerFactory();
                var command = callType + "." + commandName;

                tracer.CommandStart(usedSettings, command, key); // start within context
            }

            object sendObject = null;
            T receivedObject = default(T);
            bool isError = true;
            try
            {
                var sendAndReceivedObject = await executeAndReturnSentAndReceivedObject().ConfigureAwait(false);
                sendObject = sendAndReceivedObject.SentObject;
                receivedObject = sendAndReceivedObject.ReceivedObject;
                isError = false;
            }
            finally
            {
                if (tracer != null)
                {
                    tracer.CommandFinish(sendObject, receivedObject, isError); // finish without context
                }
            }

            return receivedObject;
        }
    }
}