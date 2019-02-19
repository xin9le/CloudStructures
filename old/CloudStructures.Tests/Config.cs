using System;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Threading.Tasks;
using System.Text;
using System.Threading;
using System.Diagnostics;
using StackExchange.Redis;

namespace CloudStructures.Tests
{
    public static class GlobalSettings
    {
        public static readonly RedisSettings Default = new RedisSettings("127.0.0.1,allowAdmin=true",
            converter: new LZ4JsonRedisValueConverter(), tracerFactory: () => new MyTracer());

        public static void Clear()
        {
            var conn = Default.GetConnection();
            conn.GetEndPoints().Select(x => conn.GetServer(x)).ToList().ForEach(x => x.FlushAllDatabases());
        }
    }

    public class MyTracer : ICommandTracer
    {
        public void CommandStart(RedisSettings usedSettings, string command, RedisKey key)
        {
            Debug.WriteLine(command + " " + (string)key);
        }

        public void CommandFinish(object sentObject, long sentSize, object receivedObject, long receivedSize, bool isError)
        {
            Debug.WriteLine($"sentSize{sentSize} receivedSize{receivedSize}");
        }
    }
}
