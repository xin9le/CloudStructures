using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CloudStructures.Tests
{
    [TestClass]
    public class Scripting
    {
        static RedisSettings settings = new RedisSettings("127.0.0.1"); // server?

        // string

        [Microsoft.VisualStudio.TestTools.UnitTesting.TestMethod]
        public async Task StringIncrMax()
        {
            var v = new RedisString<int>(settings, "test-incr");

            await v.Set(0);
            (await v.IncrementLimitByMax(10, 100)).Is(10);
            (await v.IncrementLimitByMax(30, 100)).Is(40);
            (await v.IncrementLimitByMax(50, 100)).Is(90);
            (await v.IncrementLimitByMax(10, 100)).Is(100);
            (await v.IncrementLimitByMax(1, 100)).Is(100);
            (await v.IncrementLimitByMax(10, 100)).Is(100);

            (await v.GetValueOrDefault()).Is(100);

            await v.Set(40);
            (await v.IncrementLimitByMax(100, 100)).Is(100);
            (await v.GetValueOrDefault()).Is(100);
        }


        [Microsoft.VisualStudio.TestTools.UnitTesting.TestMethod]
        public async Task StringIncrMaxWithExpiry()
        {
            var v = new RedisString<int>(settings, "test-incr2");

            await v.Set(0);
            (await v.IncrementLimitByMax(10, 100)).Is(10);
            (await v.IncrementLimitByMax(30, 100)).Is(40);
            (await v.IncrementLimitByMax(50, 100, TimeSpan.FromSeconds(1))).Is(90);
            (await v.TryGet()).Item1.IsTrue();
            await Task.Delay(TimeSpan.FromMilliseconds(1500));
            (await v.TryGet()).Item1.IsFalse();
        }

        [Microsoft.VisualStudio.TestTools.UnitTesting.TestMethod]
        public async Task StringDecrMax()
        {
            var v = new RedisString<int>(settings, "test-incr");
            await v.Set(100);
            (await v.IncrementLimitByMax(-10, 50)).Is(50);
            (await v.IncrementLimitByMax(-30, 100)).Is(20);
            (await v.IncrementLimitByMax(-50, 100)).Is(-30);
            (await v.IncrementLimitByMax(-10, 100)).Is(-40);

            (await v.GetValueOrDefault()).Is(-40);
        }

        [Microsoft.VisualStudio.TestTools.UnitTesting.TestMethod]
        public async Task StringIncrMin()
        {
            var v = new RedisString<int>(settings, "test-incr");
            await v.Set(0);
            (await v.IncrementLimitByMin(10, 100)).Is(100);
            (await v.IncrementLimitByMin(30, 100)).Is(130);
            (await v.IncrementLimitByMin(50, 100)).Is(180);

            (await v.GetValueOrDefault()).Is(180);
        }

        [Microsoft.VisualStudio.TestTools.UnitTesting.TestMethod]
        public async Task StringDecrMin()
        {
            var v = new RedisString<int>(settings, "test-incr");
            await v.Set(100);
            (await v.IncrementLimitByMin(-10, 0)).Is(90);
            (await v.IncrementLimitByMin(-30, 0)).Is(60);
            (await v.IncrementLimitByMin(-50, 0)).Is(10);
            (await v.IncrementLimitByMin(-10, 0)).Is(0);
            (await v.IncrementLimitByMin(-25, 0)).Is(0);

            (await v.GetValueOrDefault()).Is(0);

            await v.Set(50);
            (await v.IncrementLimitByMin(-100, 0)).Is(0);
            (await v.GetValueOrDefault()).Is(0);
        }

        // string/dopuble

        [TestMethod]
        public async Task StringDoubleIncrMax()
        {
            var v = new RedisString<double>(settings, "test-incr");
            await v.Set(0);
            (await v.IncrementLimitByMax(10.5, 105.3)).Is(10.5);
            (await v.IncrementLimitByMax(10.5, 105.3)).Is(21);
            (await v.IncrementLimitByMax(60.5, 105.3)).Is(81.5);
            (await v.IncrementLimitByMax(25.5, 105.3)).Is(105.3);

            (await v.GetValueOrDefault()).Is(105.3);
        }

        [TestMethod]
        public async Task StringDoubleIncrMin()
        {
            var v = new RedisString<double>(settings, "test-incr");
            await v.Set(100);
            (await v.IncrementLimitByMin(-10.5, 0.25)).Is(89.5);
            (await v.IncrementLimitByMin(-10.5, 0.25)).Is(79.0);
            (await v.IncrementLimitByMin(-60.5, 0.25)).Is(18.5);
            (await v.IncrementLimitByMin(-25.5, 0.25)).Is(0.25);

            (await v.GetValueOrDefault()).Is(0.25);
        }

        // dictionary

        //[TestMethod]
        //public async Task DictIncrMax()
        //{
        //    var v = new RedisDictionary<int>(settings, "test-hash");

        //    await v.Set("a", 0);
        //    (await v.IncrementLimitByMax("a", 10, 100)).Is(10);
        //    (await v.IncrementLimitByMax("a", 20, 100)).Is(30);
        //    (await v.IncrementLimitByMax("a", 30, 100)).Is(60);
        //    (await v.IncrementLimitByMax("a", 40, 100)).Is(100);
        //    (await v.IncrementLimitByMax("a", 50, 100)).Is(100);

        //    (await v.Get("a")).Is(100);

        //    var v2 = new RedisDictionary<double>(settings, "test-hash");
        //    await v2.Set("a", 0);
        //    (await v2.IncrementLimitByMax("a", 10.5, 100)).Is(10.5);
        //    (await v2.IncrementLimitByMax("a", 20.5, 100)).Is(31);
        //    (await v2.IncrementLimitByMax("a", 40.5, 100)).Is(71.5);
        //    (await v2.IncrementLimitByMax("a", 40.5, 100.1)).Is(100.1);
        //    (await v2.IncrementLimitByMax("a", 50.0, 100)).Is(100);

        //    (await v2.Get("a")).Is(100);
        //}

        //[TestMethod]
        //public async Task DictDecrMin()
        //{
        //    var v = new RedisDictionary<int>(settings, "test-hash");

        //    await v.Set("a", 100);
        //    (await v.IncrementLimitByMin("a", -10, 0)).Is(90);
        //    (await v.IncrementLimitByMin("a", -20, 0)).Is(70);
        //    (await v.IncrementLimitByMin("a", -30, 0)).Is(40);
        //    (await v.IncrementLimitByMin("a", -42, 0)).Is(0);
        //    (await v.IncrementLimitByMin("a", -50, 0)).Is(0);

        //    (await v.Get("a")).Is(0);

        //    var v2 = new RedisDictionary<double>(settings, "test-hash");
        //    await v2.Set("a", 100);
        //    (await v2.IncrementLimitByMin("a", -10.5, 0.5)).Is(89.5);
        //    (await v2.IncrementLimitByMin("a", -20.5, 0.5)).Is(69);
        //    (await v2.IncrementLimitByMin("a", -40.5, 0.5)).Is(28.5);
        //    (await v2.IncrementLimitByMin("a", -40.5, 0.5)).Is(0.5);

        //    (await v2.Get("a")).Is(0.5);
        //}

        //// hash

        //[TestMethod]
        //public async Task HashIncrMax()
        //{
        //    var v = new RedisHash(settings, "test-hash");

        //    await v.Set("a", 0);
        //    (await v.IncrementLimitByMax("a", 10, 100)).Is(10);
        //    (await v.IncrementLimitByMax("a", 20, 100)).Is(30);
        //    (await v.IncrementLimitByMax("a", 30, 100)).Is(60);
        //    (await v.IncrementLimitByMax("a", 40, 100)).Is(100);
        //    (await v.IncrementLimitByMax("a", 50, 100)).Is(100);

        //    (await v.Get<long>("a")).Is(100);

        //    var v2 = new RedisHash(settings, "test-hash");
        //    await v2.Set("a", 0);
        //    (await v2.IncrementLimitByMax("a", 10.5, 100)).Is(10.5);
        //    (await v2.IncrementLimitByMax("a", 20.5, 100)).Is(31);
        //    (await v2.IncrementLimitByMax("a", 40.5, 100)).Is(71.5);
        //    (await v2.IncrementLimitByMax("a", 40.5, 100.1)).Is(100.1);
        //    (await v2.IncrementLimitByMax("a", 50.0, 100)).Is(100);

        //    (await v2.Get<double>("a")).Is(100);
        //}

        //[TestMethod]
        //public async Task HashDecrMin()
        //{
        //    var v = new RedisHash(settings, "test-hash");

        //    await v.Set("a", 100);
        //    (await v.IncrementLimitByMin("a", -10, 0)).Is(90);
        //    (await v.IncrementLimitByMin("a", -20, 0)).Is(70);
        //    (await v.IncrementLimitByMin("a", -30, 0)).Is(40);
        //    (await v.IncrementLimitByMin("a", -42, 0)).Is(0);
        //    (await v.IncrementLimitByMin("a", -50, 0)).Is(0);

        //    (await v.Get<long>("a")).Is(0);

        //    var v2 = new RedisHash(settings, "test-hash");
        //    await v2.Set("a", 100);
        //    (await v2.IncrementLimitByMin("a", -10.5, 0.5)).Is(89.5);
        //    (await v2.IncrementLimitByMin("a", -20.5, 0.5)).Is(69);
        //    (await v2.IncrementLimitByMin("a", -40.5, 0.5)).Is(28.5);
        //    (await v2.IncrementLimitByMin("a", -40.5, 0.5)).Is(0.5);

        //    (await v2.Get<double>("a")).Is(0.5);
        //}

        // class

        public class MyClass
        {
            public int a { get; set; }
            public double b { get; set; }
        }

        //[TestMethod]
        //public async Task ClassIncrMax()
        //{
        //    var v = new RedisClass<MyClass>(settings, "test-hash");

        //    await v.SetField("a", 0);
        //    (await v.IncrementLimitByMax("a", 10, 100)).Is(10);
        //    (await v.IncrementLimitByMax("a", 20, 100)).Is(30);
        //    (await v.IncrementLimitByMax("a", 30, 100)).Is(60);
        //    (await v.IncrementLimitByMax("a", 40, 100)).Is(100);
        //    (await v.IncrementLimitByMax("a", 50, 100)).Is(100);

        //    (await v.GetValue()).a.Is(100);
        //    (await v.GetField<int>("a")).Is(100);

        //    await v.SetField("b", 0.0);
        //    (await v.IncrementLimitByMax("b", 10.5, 100)).Is(10.5);
        //    (await v.IncrementLimitByMax("b", 20.5, 100)).Is(31);
        //    (await v.IncrementLimitByMax("b", 40.5, 100)).Is(71.5);
        //    (await v.IncrementLimitByMax("b", 40.5, 100.1)).Is(100.1);
            
        //    (await v.GetValue()).b.Is(100.1);
        //    (await v.GetField<double>("b")).Is(100.1);
        //}

        //[TestMethod]
        //public async Task ClassDecrMin()
        //{
        //    var v = new RedisClass<MyClass>(settings, "test-hash");

        //    await v.SetField("a", 100);
        //    (await v.IncrementLimitByMin("a", -10, 0)).Is(90);
        //    (await v.IncrementLimitByMin("a", -20, 0)).Is(70);
        //    (await v.IncrementLimitByMin("a", -30, 0)).Is(40);
        //    (await v.IncrementLimitByMin("a", -40, 0)).Is(0);
        //    (await v.IncrementLimitByMin("a", -50, 0)).Is(0);

        //    (await v.GetValue()).a.Is(0);
        //    (await v.GetField<int>("a")).Is(0);

        //    await v.SetField("b", 100.0);
        //    (await v.IncrementLimitByMin("b", -10.5, 0.5)).Is(89.5);
        //    (await v.IncrementLimitByMin("b", -20.5, 0.5)).Is(69);
        //    (await v.IncrementLimitByMin("b", -40.5, 0.5)).Is(28.5);
        //    (await v.IncrementLimitByMin("b", -40.5, 0.5)).Is(0.5);

        //    (await v.GetValue()).b.Is(0.5);
        //    (await v.GetField<double>("b")).Is(0.5);
        //}
    }
}
