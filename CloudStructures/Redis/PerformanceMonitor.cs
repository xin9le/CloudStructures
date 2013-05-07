using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;

namespace CloudStructures.Redis
{
    public interface IPerformanceMonitor
    {
        void Start(Guid identity, string command, string key);
        void End(Guid identity, string command, string key, long durationMilliseconds);
    }

    internal class Monitor : IDisposable
    {
        static readonly IDisposable EmptyDisposable = new EmptyDisposable();

        Guid id;
        string commandName;
        string key;
        Stopwatch stopwatch;
        IPerformanceMonitor monitor;

        public static IDisposable Start(IPerformanceMonitor monitor, string key, [CallerFilePath]string callerFilePath = "", [CallerMemberName]string commandName = "")
        {
            if (monitor != null)
            {
                return new Monitor(monitor, key, callerFilePath, commandName);
            }
            else
            {
                return EmptyDisposable;
            }
        }

        private Monitor(IPerformanceMonitor monitor, string key, string callerFilePath, string commandName)
        {
            this.id = Guid.NewGuid();
            this.key = key;
            this.commandName = Path.GetFileNameWithoutExtension(callerFilePath) + "." + commandName;
            monitor.Start(id, commandName, key);
            stopwatch.Start();
        }

        public void Dispose()
        {
            stopwatch.Stop();
            monitor.End(id, commandName, key, stopwatch.ElapsedMilliseconds);
        }
    }

    internal class EmptyDisposable : IDisposable
    {
        public void Dispose()
        {

        }
    }
}