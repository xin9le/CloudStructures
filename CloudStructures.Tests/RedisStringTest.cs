using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Threading.Tasks;
using System.Text;
using System.Threading;

namespace CloudStructures.Tests
{
    // a class
    public class Person
    {
        public string Name { get; set; }
        public int Age { get; set; }
    }

    [TestClass]
    public class RedisStringTest
    {
        [TestMethod]
        public void Get_Set()
        {
            var s = new RedisString<int>(GlobalSettings.Default, "RedisStringTest.Get_Set");
            s.Settings.GetConnection().GetServer("127.0.0.1:6379").FlushAllDatabases();

            s.GetValueOrDefault(-1).Result.Is(-1);

            s.Set(1000).Result.IsTrue();

            s.TryGet().Result.Item2.Is(1000);
        }

        [TestMethod]
        public void GetOrSet()
        {
            var s = new RedisString<int>(GlobalSettings.Default, "test-string");
            s.Delete().Wait();

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

        [TestMethod]
        public void Bit()
        {
            var s = new RedisString<int>(GlobalSettings.Default, "test-bit");
            s.Delete().Wait();

            var db = s.Settings.GetConnection().GetDatabase();

            s.SetBit(7, true).Result.Is(false);
            s.GetBit(0).Result.Is(false);
            s.GetBit(7).Result.Is(true);
            s.GetBit(100).Result.Is(false);

            s.SetBit(7, false).Result.Is(true);
        }

        [TestMethod]
        public void BitCount()
        {
            var s = new RedisString<int>(GlobalSettings.Default, "test-bitcount");
            s.Delete().Wait();

            s.SetBit(7, true).Result.Is(false);
            s.GetBit(0).Result.Is(false);
            s.GetBit(7).Result.Is(true);
            s.GetBit(100).Result.Is(false);

            s.SetBit(7, false).Result.Is(true);
        }

        [TestMethod]
        public void Incr()
        {
            var s = new RedisString<int>(GlobalSettings.Default, "test-incr");
            s.Delete().Wait();

            s.Increment(100).Result.Is(100);

            s.Increment(100, TimeSpan.FromSeconds(1)).Result.Is(200);

            s.TryGet().Result.Item1.IsTrue();
            Thread.Sleep(TimeSpan.FromSeconds(2));
            s.TryGet().Result.Item1.IsFalse();

        }
    }
}