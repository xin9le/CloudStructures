using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace CloudStructures.Redis
{
    public interface ICommandTracer
    {
        void CommandStart(string command, string key);
        void CommandFinish();
    }

    internal class Monitor : IDisposable
    {
        static readonly IDisposable EmptyDisposable = new EmptyDisposable();

        readonly ICommandTracer tracer;

        public static IDisposable Start(Func<ICommandTracer> tracerFactory, string key, string callType, [CallerMemberName]string commandName = "")
        {
            if (tracerFactory != null)
            {
                var tracer = tracerFactory();
                var command = callType + "." + commandName;
                tracer.CommandStart(command, key);
                return new Monitor(tracer);
            }
            else
            {
                return EmptyDisposable;
            }
        }

        Monitor(ICommandTracer tracer)
        {
            this.tracer = tracer;
        }

        public void Dispose()
        {
            tracer.CommandFinish();
        }
    }

    internal class EmptyDisposable : IDisposable
    {
        public void Dispose()
        {

        }
    }
}