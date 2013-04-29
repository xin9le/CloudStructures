using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using CloudStructures.Redis;
using System.Threading.Tasks;
using System.Text;
using System.Threading;

namespace CloudStructures.Tests
{
    [TestClass]
    public class RedisStringTest
    {
        [TestMethod]
        public void GetOrAdd()
        {
            var s = new RedisString<int>(GlobalSettings.Default, "test-string");
            s.Remove().Wait();

            var loaded = false;
            s.GetOrSet(() =>
            {
                loaded = true;
                return 1000;
            }).Result.Is(1000);

            loaded.IsTrue();

            s.GetOrSet(() =>
            {
                Assert.Fail();
                return 2000;
            }).Result.Is(1000);
        }
    }
}