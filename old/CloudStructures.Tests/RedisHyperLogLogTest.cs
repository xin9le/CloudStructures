using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Threading.Tasks;
using System.Text;
using System.Threading;

namespace CloudStructures.Tests
{
    [TestClass]
    public class RedisHyperLogLogTest
    {
        [TestMethod]
        public void Add()
        {
            GlobalSettings.Clear();
            var loglog = GlobalSettings.Default.HyperLogLog<int>("RedisHyperLogLogTest.Add");

            loglog.Add(100).Result.IsTrue();
            loglog.Add(100).Result.IsFalse();

            loglog.Length().Result.Is(1);

            loglog.Add(200).Result.IsTrue();
            loglog.Length().Result.Is(2);
        }
    }
}