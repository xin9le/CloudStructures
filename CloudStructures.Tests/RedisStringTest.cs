using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using CloudStructures.Redis;
using System.Threading.Tasks;
using System.Text;
using System.Threading;

namespace CloudStructures.Tests
{
    public static class RedisServer
    {
        public static readonly RedisSettings Default = new RedisSettings("127.0.0.1");
    }

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


        public async void MyTestMethod()
        {

            // redis list
            var redis = new RedisList<Person>(RedisServer.Default, "test-list-key");
            await redis.AddLast(new Person { Name = "Tom" });
            await redis.AddLast(new Person { Name = "Mary" });

            var persons = await redis.Range(0, 10);
        }
    }
}