using System;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Threading.Tasks;
using System.Text;
using System.Threading;
using StackExchange.Redis;
using System.Collections.Generic;

namespace CloudStructures.Tests
{
    [TestClass]
    public class RedisHashTest
    {
        [TestMethod]
        public async Task DictionaryAdd()
        {
            var dict = new RedisDictionary<string, int>(GlobalSettings.Default, "dict");
            await dict.Delete();

            (await dict.Set("a", -1)).IsTrue();
            (await dict.Set("a", 0)).IsFalse(); // already exists
            (await dict.Set("b", 1)).IsTrue();
            (await dict.Set("b", -1, when: When.NotExists)).IsFalse();
            (await dict.Set("c", 2)).IsTrue();
            (await dict.Set("d", 3)).IsTrue();
            await dict.Set(new Dictionary<string, int> { { "e", 4 }, { "f", 5 } });

            var xs = (await dict.GetAll()).OrderBy(x => x.Key).ToArray();
            xs.Select(x => x.Key).Is("a", "b", "c", "d", "e", "f");
            xs.Select(x => x.Value).Is(0, 1, 2, 3, 4, 5);
            (await dict.Keys()).OrderBy(x => x).Is("a", "b", "c", "d", "e", "f");
            (await dict.Values()).OrderBy(x => x).Is(0, 1, 2, 3, 4, 5);
            (await dict.Length()).Is(6);

            (await dict.Exists("a")).IsTrue();
            (await dict.Exists("z")).IsFalse();

            (await dict.Get("a")).Value.Is(0);
            (await dict.Get("c")).Value.Is(2);
            (await dict.Get("z")).HasValue.IsFalse();

            var mget = (await dict.Get(new[] { "a", "b", "u", "d", "z" })).OrderBy(x => x.Key).ToArray();
            mget.Select(x => x.Key).Is("a", "b", "d");
            mget.Select(x => x.Value).Is(0, 1, 3);

            (await dict.Delete("c")).IsTrue();
            (await dict.Delete("c")).IsFalse();
            (await dict.Keys()).OrderBy(x => x).Is("a", "b", "d", "e", "f");

            (await dict.Delete(new[] { "a", "c", "d", "z" })).Is(2);
            (await dict.Keys()).OrderBy(x => x).Is("b", "e", "f");
        }

        [TestMethod]
        public async Task DictionaryIncr()
        {
            var dict = new RedisDictionary<string, long>(GlobalSettings.Default, "dict");
            await dict.Delete();

            (await dict.Increment("hogehoge", 100)).Is(100);
            (await dict.Increment("hogehoge", 100)).Is(200);
            (await dict.Increment("hogehoge", -100)).Is(100);
            (await dict.Get("hogehoge")).Value.Is(100);

            (await dict.IncrementLimitByMax("hogehoge", 40, 150)).Is(140);
            (await dict.IncrementLimitByMax("hogehoge", 40, 150)).Is(150);
            (await dict.IncrementLimitByMin("hogehoge", -40, 100)).Is(110);
            (await dict.IncrementLimitByMin("hogehoge", -40, 100)).Is(100);

            var dict2 = new RedisDictionary<string, double>(GlobalSettings.Default, "dict2");
            await dict2.Delete();
            (await dict2.Increment("hogehoge", 100.5)).Is(100.5);
            (await dict2.Increment("hogehoge", 100.0)).Is(200.5);
            (await dict2.Increment("hogehoge", -100.0)).Is(100.5);
            (await dict2.Get("hogehoge")).Value.Is(100.5);

            (await dict2.IncrementLimitByMax("hogehoge", 40.1, 150.9)).Is(140.6);
            (await dict2.IncrementLimitByMax("hogehoge", 40.3, 150.9)).Is(150.9);
            (await dict2.IncrementLimitByMin("hogehoge", -40.1, 100.9)).Is(110.8);
            (await dict2.IncrementLimitByMin("hogehoge", -40.3, 100.9)).Is(100.9);
        }

        [TestMethod]
        public async Task Hash()
        {
            var hash = new RedisHash<string>(GlobalSettings.Default, "hash");
            await hash.Delete();

            await hash.Set("foo", 100);
            await hash.Set("bar", "aiueo!");

            (await hash.Get<int>("foo")).Value.Is(100);
            (await hash.Get<string>("bar")).Value.Is("aiueo!");
        }


        public class MyClass
        {
            public int Foo { get; set; }
            public int Zoo { get; set; }
            public double Doo { get; set; }
            public string Bar { get; set; }
            public long Zooom { get; set; }
            public float FFFFFF { get; set; }
        }

        [TestMethod]
        public async Task Class()
        {
            var klass = new RedisClass<MyClass>(GlobalSettings.Default, "class");
            await klass.Delete();

            await klass.Set(new MyClass { Foo = 1000, Bar = "hogehoge", Zoo = 300, Doo = 10.5 });
            var mc = await klass.Get();
            mc.Foo.Is(1000);
            mc.Bar.Is("hogehoge");

            (await klass.SetMember("Bar", "aiueo")).IsTrue();
            (await klass.GetMember(x => x.Bar)).Value.Is("aiueo");
            (await klass.SetMember(x => x.Foo, 10000)).IsTrue();
            (await klass.GetMember(x => x.Foo)).Value.Is(10000);
            (await klass.GetMember<int>("nai")).HasValue.IsFalse();

            var members = await klass.GetMembers(x => new[] { x.Foo, x.Zoo });
            members["Foo"].Is(10000);
            members["Zoo"].Is(300);

            await klass.SetMembers(new Dictionary<string, int>
            {
                {"Foo", 10 }, {"Zoo", 300 }
            });
            (await klass.GetMember(x => x.Foo)).Value.Is(10);
            (await klass.GetMember(x => x.Zoo)).Value.Is(300);

            await klass.SetMembers(x => new[] { x.Foo, x.Zoo }, new[] { 5000, 4000 });
            (await klass.GetMember(x => x.Foo)).Value.Is(5000);
            (await klass.GetMember(x => x.Zoo)).Value.Is(4000);

            (await klass.Increment("Foo", 100)).Is(5100);
            (await klass.Increment("Foo", 100)).Is(5200);
            (await klass.Increment("Foo", -100)).Is(5100);
            (await klass.GetMember<int>("Foo")).Value.Is(5100);
            (await klass.IncrementLimitByMax("Foo", 100, 5250)).Is(5200);
            (await klass.IncrementLimitByMax("Foo", 100, 5250)).Is(5250);
            (await klass.IncrementLimitByMin("Foo", -3000, 100)).Is(2250);
            (await klass.IncrementLimitByMin("Foo", -3000, 100)).Is(100);
            (await klass.Increment(x => x.Foo, 10)).Is(110);
            (await klass.Increment(x => x.Zooom, 10000)).Is(10000);
            (await klass.IncrementLimitByMax(x => x.Foo, 500, 140)).Is(140);
            (await klass.IncrementLimitByMin(x => x.Foo, -500, 30)).Is(30);

            (await klass.Increment("Doo", 20.1)).Is(30.6);
            (await klass.Increment("Doo", 20.1)).Is(50.7);
            Math.Round(await klass.Increment("Doo", -20.3), 1).Is(30.4);
            Math.Round((await klass.GetMember<double>("Doo")).Value, 1).Is(30.4);
            Math.Round((await klass.IncrementLimitByMax("Doo", 50.5, 102.4)), 1).Is(80.9);
            Math.Round((await klass.IncrementLimitByMax("Doo", 50.5, 102.3)), 1).Is(102.3);
            Math.Round((await klass.IncrementLimitByMin("Doo", -50.4, 30.3)), 1).Is(51.9);
            Math.Round((await klass.IncrementLimitByMin("Doo", -40.2, 30.3)), 1).Is(30.3);
            Math.Round((await klass.Increment(x => x.Doo, 20.1)), 1).Is(50.4);
            Math.Round((await klass.IncrementLimitByMax(x => x.Doo, 909.2, 88.8)), 1).Is(88.8);
            Math.Round((await klass.IncrementLimitByMin(x => x.Doo, -909.2, 40.4)), 1).Is(40.4);
        }
    }
}