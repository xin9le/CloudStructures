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

            (await v.Get()).Value.Is(100);

            await v.Set(40);
            (await v.IncrementLimitByMax(100, 100)).Is(100);
            (await v.Get()).Value.Is(100);
        }


        [Microsoft.VisualStudio.TestTools.UnitTesting.TestMethod]
        public async Task StringIncrMaxWithExpiry()
        {
            var v = new RedisString<int>(settings, "test-incr2");

            await v.Set(0);
            (await v.IncrementLimitByMax(10, 100)).Is(10);
            (await v.IncrementLimitByMax(30, 100)).Is(40);
            (await v.IncrementLimitByMax(50, 100, TimeSpan.FromSeconds(1))).Is(90);
            (await v.Get()).HasValue.IsTrue();
            await Task.Delay(TimeSpan.FromMilliseconds(1500));
            (await v.Get()).HasValue.IsFalse();
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

            (await v.Get()).Value.Is(-40);
        }

        [Microsoft.VisualStudio.TestTools.UnitTesting.TestMethod]
        public async Task StringIncrMin()
        {
            var v = new RedisString<int>(settings, "test-incr");
            await v.Set(0);
            (await v.IncrementLimitByMin(10, 100)).Is(100);
            (await v.IncrementLimitByMin(30, 100)).Is(130);
            (await v.IncrementLimitByMin(50, 100)).Is(180);

            (await v.Get()).Value.Is(180);
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

            (await v.Get()).Value.Is(0);

            await v.Set(50);
            (await v.IncrementLimitByMin(-100, 0)).Is(0);
            (await v.Get()).Value.Is(0);
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

            (await v.Get()).Value.Is(105.3);
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

            (await v.Get()).Value.Is(0.25);
        }
    }
}
