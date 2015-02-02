using System;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace CloudStructures
{
    public interface ICommandTracer
    {
        void CommandStart(RedisSettings usedSettings, string command, string key);
        void CommandFinish(object sentObject, object receivedObject, bool isError);
    }

    internal static class Pair
    {
        public static MonitorSingle CreateSent(object sentObject, long sentSize)
        {
            return new MonitorSingle { SentObject = sentObject, SentSize = sentSize };
        }

        public static MonitorSingle<T> CreateReceived<T>(T receivedObject, long receivedSize)
        {
            return new MonitorSingle<T> { ReceivedObject = receivedObject, ReceivedSize = receivedSize };
        }

        public static MonitorPair<T> CreatePair<T>(object sentObject, long sentSize, T receivedObject, long receivedSize)
        {
            return new MonitorPair<T> { SentObject = sentObject, SentSize = sentSize, ReceivedObject = receivedObject, ReceivedSize = receivedSize };
        }
    }

    internal class MonitorSingle
    {
        public object SentObject { get; set; }
        public long SentSize { get; set; }
    }

    internal class MonitorSingle<T>
    {
        public T ReceivedObject { get; set; }
        public long ReceivedSize { get; set; }
    }

    internal class MonitorPair<T>
    {
        public object SentObject { get; set; }
        public long SentSize { get; set; }
        public T ReceivedObject { get; set; }
        public long ReceivedSize { get; set; }
    }

    internal static class TraceHelper
    {
        public static async Task RecordSend(RedisSettings usedSettings, string key, string callType, Func<Task<MonitorSingle>> executeAndReturnSentObject, [CallerMemberName]string commandName = "")
        {
            var tracerFactory = usedSettings.CommandTracerFactory;
            ICommandTracer tracer = null;
            if (tracerFactory != null)
            {
                tracer = tracerFactory();
                var command = callType + "." + commandName;

                tracer.CommandStart(usedSettings, command, key); // start within context
            }

            MonitorSingle sendObject = null;
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
        public static async Task<T> RecordReceive<T>(RedisSettings usedSettings, string key, string callType, Func<Task<MonitorSingle<T>>> executeAndReturnReceivedObject, [CallerMemberName]string commandName = "")
        {
            var tracerFactory = usedSettings.CommandTracerFactory;
            ICommandTracer tracer = null;
            if (tracerFactory != null)
            {
                tracer = tracerFactory();
                var command = callType + "." + commandName;

                tracer.CommandStart(usedSettings, command, key); // start within context
            }

            MonitorSingle<T> receivedObject = null;
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

            return (receivedObject == null) ? default(T) : receivedObject.ReceivedObject;
        }

        public static async Task<T> RecordSendAndReceive<T>(RedisSettings usedSettings, string key, string callType, Func<Task<MonitorPair<T>>> executeAndReturnSentAndReceivedObject, [CallerMemberName]string commandName = "")
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