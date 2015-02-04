using StackExchange.Redis;
using System;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace CloudStructures
{
    public interface ICommandTracer
    {
        void CommandStart(RedisSettings usedSettings, string command, RedisKey key);
        void CommandFinish(object sentObject, long sentSize, object receivedObject, long receivedSize, bool isError);
    }

    internal static class Tracing
    {
        public static MonitorSingle CreateSent(object sentObject, long sentSize)
        {
            return new MonitorSingle { SentObject = sentObject, SentSize = sentSize };
        }

        public static MonitorSingle<T> CreateReceived<T>(T receivedObject, long receivedSize)
        {
            return new MonitorSingle<T> { ReceivedObject = receivedObject, ReceivedSize = receivedSize };
        }

        public static MonitorPair<T> CreateSentAndReceived<T>(object sentObject, long sentSize, T receivedObject, long receivedSize)
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
        public static async Task RecordSend(RedisSettings usedSettings, RedisKey key, string callType, Func<Task<MonitorSingle>> executeAndReturnSentObject, [CallerMemberName]string commandName = "")
        {
            var tracerFactory = usedSettings.CommandTracerFactory;
            ICommandTracer tracer = null;
            if (tracerFactory != null)
            {
                tracer = tracerFactory();
                var command = callType + "." + commandName;

                tracer.CommandStart(usedSettings, command, key); // start within context
            }

            MonitorSingle sentObject = null;
            bool isError = true;
            try
            {
                sentObject = await executeAndReturnSentObject().ForAwait();
                isError = false;
            }
            finally
            {
                if (tracer != null)
                {
                    tracer.CommandFinish(sentObject?.SentObject, sentObject?.SentSize ?? 0, null, 0, isError); // finish without context
                }
            }
        }
        public static async Task<T> RecordReceive<T>(RedisSettings usedSettings, RedisKey key, string callType, Func<Task<MonitorSingle<T>>> executeAndReturnReceivedObject, [CallerMemberName]string commandName = "")
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
                receivedObject = await executeAndReturnReceivedObject().ForAwait();
                isError = false;
            }
            finally
            {
                if (tracer != null)
                {
                    tracer.CommandFinish(null, 0, (receivedObject != null) ? receivedObject.ReceivedObject : default(T), receivedObject?.ReceivedSize ?? 0, isError); // finish without context
                }
            }

            return (receivedObject == null) ? default(T) : receivedObject.ReceivedObject;
        }

        public static async Task<T> RecordSendAndReceive<T>(RedisSettings usedSettings, RedisKey key, string callType, Func<Task<MonitorPair<T>>> executeAndReturnSentAndReceivedObject, [CallerMemberName]string commandName = "")
        {
            var tracerFactory = usedSettings.CommandTracerFactory;
            ICommandTracer tracer = null;
            if (tracerFactory != null)
            {
                tracer = tracerFactory();
                var command = callType + "." + commandName;

                tracer.CommandStart(usedSettings, command, key); // start within context
            }

            MonitorPair<T> pair = null;
            bool isError = true;
            try
            {
                pair = await executeAndReturnSentAndReceivedObject().ForAwait();
                isError = false;
            }
            finally
            {
                if (tracer != null)
                {
                    tracer.CommandFinish(pair?.SentObject, pair?.SentSize ?? 0, (pair != null) ? pair.ReceivedObject : default(T), pair?.ReceivedSize ?? 0, isError); // finish without context
                }
            }

            return (pair == null) ? default(T) : pair.ReceivedObject;
        }
    }
}