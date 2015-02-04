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
    }
}