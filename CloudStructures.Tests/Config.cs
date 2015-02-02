using System;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Threading.Tasks;
using System.Text;
using System.Threading;

namespace CloudStructures.Tests
{
    public static class GlobalSettings
    {
        public static readonly RedisSettings Default = new RedisSettings("127.0.0.1,allowAdmin=true", converter: new GZipJsonRedisValueConverter());

        public static void Clear()
        {
            var conn = Default.GetConnection();
            conn.GetEndPoints().Select(x => conn.GetServer(x)).ToList().ForEach(x => x.FlushAllDatabases());
        }
    }
}
