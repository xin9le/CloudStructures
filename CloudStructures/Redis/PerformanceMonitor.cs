using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;

namespace CloudStructures.Redis
{
    public interface IPerformanceMonitor
    {
        void Start(Guid identity, string command, string key);
        void End(Guid identity, string command, string key, double durationMilliseconds);
    }

    internal class Monitor : IDisposable
    {
        static readonly IDisposable EmptyDisposable = new EmptyDisposable();

        Guid id;
        string command;
        string key;
        Stopwatch stopwatch;
        IPerformanceMonitor monitor;

        public static IDisposable Start(IPerformanceMonitor monitor, string key, string callType, [CallerMemberName]string commandName = "")
        {
            if (monitor != null)
            {
                return new Monitor(monitor, key, callType, commandName);
            }
            else
            {
                return EmptyDisposable;
            }
        }

        private Monitor(IPerformanceMonitor monitor, string key, string callType, string commandName)
        {
            this.id = Guid.NewGuid();
            this.key = key;
            this.command = callType + "." + commandName;
            this.monitor = monitor;
            monitor.Start(id, command, key);
            stopwatch = Stopwatch.StartNew();
        }

        public void Dispose()
        {
            stopwatch.Stop();
            monitor.End(id, command, key, stopwatch.Elapsed.TotalMilliseconds);
        }
    }

    internal class EmptyDisposable : IDisposable
    {
        public void Dispose()
        {

        }
    }
}