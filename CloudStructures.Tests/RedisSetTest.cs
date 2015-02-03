using System;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Threading.Tasks;
using System.Text;
using System.Threading;

namespace CloudStructures.Tests
{
    public class MySetTestClass
    {
        public int MyProperty { get; set; }
    }

    [TestClass]
    public class RedisSetTest
    {
        [TestMethod]
        public async Task SetAdd()
        {
            var set = new RedisSet<int>(GlobalSettings.Default, "set");
            await set.Delete();

            (await set.Add(1)).Is(true);
            (await set.Add(10)).Is(true);
            (await set.Add(1)).Is(false);

            (await set.Contains(1)).IsTrue();
            (await set.Contains(1000)).IsFalse();
            (await set.Members()).OrderBy(x => x).Is(1, 10);
            (await set.Length()).Is(2);

            var set2 = new RedisSet<MySetTestClass[]>(GlobalSettings.Default, "set2");
            await set2.Delete();

            (await set2.Add(new[] { new MySetTestClass { MyProperty = 100 }, new MySetTestClass { MyProperty = 300 } })).Is(true);
            (await set2.Add(new[] { new MySetTestClass { MyProperty = 500 }, new MySetTestClass { MyProperty = 900 } })).Is(true);
            (await set2.Add(new[] { new MySetTestClass { MyProperty = 100 }, new MySetTestClass { MyProperty = 300 } }, TimeSpan.FromSeconds(1))).Is(false);

            (await set2.Members()).SelectMany(xs => xs, (xs, x) => x.MyProperty).OrderBy(x => x).Is(100, 300, 500, 900);

            await Task.Delay(TimeSpan.FromMilliseconds(1500));

            (await set.KeyExists()).IsTrue();
            (await set2.KeyExists()).IsFalse();
        }


        [TestMethod]
        public async Task Hoge()
        {
            var set = new RedisList<int>(GlobalSettings.Default, "l");
            await set.Delete();
            (await set.LeftPop()).Is(0);

        }
    }
}