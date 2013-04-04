using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using CloudStructures.Redis;
using System.Threading.Tasks;
using System.Text;
using System.Threading;

namespace CloudStructures.Tests
{
    public static class GlobalSettings
    {
        public static readonly RedisSettings Default = new RedisSettings("127.0.0.1", ioTimeout: 5);
    }
}
