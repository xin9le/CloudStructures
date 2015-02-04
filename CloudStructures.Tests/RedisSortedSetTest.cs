using System;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Threading.Tasks;
using System.Text;
using System.Threading;
using StackExchange.Redis;

namespace CloudStructures.Tests
{
    [TestClass]
    public class RedisSortedSetTest
    {
        [TestMethod]
        public async Task SortedSetAdd()
        {
            var set = new RedisSortedSet<string>(GlobalSettings.Default, "set");
            await set.Delete();

            (await set.Add("hogehoge", 10)).Is(true);
            (await set.Add("hogehoge", 100)).Is(false); // already added but updated
            (await set.Add("huga", 1)).Is(true);

            (await set.RangeByRank()).Is("huga", "hogehoge");
            (await set.RangeByRank(order: Order.Descending)).Is("hogehoge", "huga");
        }

        [TestMethod]
        public async Task SortedSetRangeByRankWithScore()
        {
            var set = new RedisSortedSet<string>(GlobalSettings.Default, "set");
            await set.Delete();

            await set.Add("a", 10);
            await set.Add("d", 10000);
            await set.Add("b", 100);
            await set.Add("f", 1000000);
            await set.Add("e", 100000);
            await set.Add("c", 1000);

            var range = await set.RangeByRankWithScoresAndRank();
            range.Select(x => x.Value).Is("a", "b", "c", "d", "e", "f");
            range.Select(x => x.Score).Is(10, 100, 1000, 10000, 100000, 1000000);
            range.Select(x => x.Rank).Is(0, 1, 2, 3, 4, 5);

            range = await set.RangeByRankWithScoresAndRank(start: 3); // 3 to last
            range.Select(x => x.Rank).Is(3, 4, 5);

            range = await set.RangeByRankWithScoresAndRank(start: 4, stop: 2);
            range.Length.Is(0);

            range = await set.RangeByRankWithScoresAndRank(order: Order.Descending);
            range.Select(x => x.Value).Is("f", "e", "d", "c", "b", "a");
            range.Select(x => x.Score).Is(1000000, 100000, 10000, 1000, 100, 10);
            range.Select(x => x.Rank).Is(0, 1, 2, 3, 4, 5);

            range = await set.RangeByRankWithScoresAndRank(start: 3, order: Order.Descending); // 3 to last
            range.Select(x => x.Value).Is("c", "b", "a");
            range.Select(x => x.Rank).Is(3, 4, 5);

            range = await set.RangeByRankWithScoresAndRank(start: 2, stop: 4, order: Order.Descending); // 2 to 4
            range.Select(x => x.Value).Is("d", "c", "b");
            range.Select(x => x.Rank).Is(2, 3, 4);

            // start < 0

            range = await set.RangeByRankWithScoresAndRank(start: -1, stop: -1, order: Order.Ascending); // last to last
            range.Select(x => x.Value).Is("f");
            range.Select(x => x.Score).Is(1000000);
            range.Select(x => x.Rank).Is(5);

            range = await set.RangeByRankWithScoresAndRank(start: -3, stop: -2, order: Order.Ascending);
            range.Select(x => x.Value).Is("d", "e");
            range.Select(x => x.Rank).Is(3, 4);

            range = await set.RangeByRankWithScoresAndRank(start: -2, stop: 5, order: Order.Descending);
            range.Select(x => x.Value).Is("b", "a");
            range.Select(x => x.Rank).Is(4, 5);



            range = await set.RangeByRankWithScoresAndRank(start: -100, stop: -120, order: Order.Descending);
            range.Length.Is(0);

        }

        [TestMethod]
        public async Task RangeByScore()
        {
            var set = new RedisSortedSet<string>(GlobalSettings.Default, "set");
            await set.Delete();

            await set.Add("a", 10);
            await set.Add("b", 100);
            await set.Add("c", 1000);
            await set.Add("d", 10000);
            await set.Add("e", 100000);
            await set.Add("f", 1000000);

            (await set.RangeByScore(100, 10000, exclude: Exclude.None)).Is("b", "c", "d");
            (await set.RangeByScore(100, 10000, exclude: Exclude.Start)).Is("c", "d");
            (await set.RangeByScore(100, 10000, exclude: Exclude.Stop)).Is("b", "c");
            (await set.RangeByScore(100, 10000, exclude: Exclude.Both)).Is("c");

            (await set.RangeByScore(100, 10000, exclude: Exclude.None, order: Order.Descending)).Is("d", "c", "b");
            var hoge = await set.RangeByScore(100, 10000, exclude: Exclude.Start, order: Order.Descending);
            (await set.RangeByScore(100, 10000, exclude: Exclude.Start, order: Order.Descending)).Is("d", "c");
            (await set.RangeByScore(100, 10000, exclude: Exclude.Stop, order: Order.Descending)).Is("c", "b");

            (await set.RangeByScore(100, 10000, skip: 1)).Is("c", "d");
            (await set.RangeByScore(100, 10000, skip: 2)).Is("d");
            (await set.RangeByScore(100, 10000, take: 2)).Is("b", "c");
            (await set.RangeByScore(100, 10000, skip: 1, take: 1)).Is("c");

            var r = await set.RangeByScoreWithScores(100, 10000);
            r.Select(x => x.Value).Is("b", "c", "d");
            r.Select(x => x.Score).Is(100, 1000, 10000);

            // length
            (await set.Length()).Is(6);
            (await set.Length(100, 10000)).Is(3);
            (await set.Length(100, 10000, Exclude.Both)).Is(3);
        }

        [TestMethod]
        public async Task RangeByValue()
        {
            var set = new RedisSortedSet<string>(GlobalSettings.Default, "set");
            await set.Delete();

            await set.Add("a", 10);
            await set.Add("b", 100);
            await set.Add("c", 1000);
            await set.Add("d", 10000);
            await set.Add("e", 100000);
            await set.Add("f", 1000000);

            (await set.RangeByValue("b", "d", exclude: Exclude.None)).Is("b", "c", "d");
        }

        [TestMethod]
        public async Task Incr()
        {
            var set = new RedisSortedSet<string>(GlobalSettings.Default, "set");
            await set.Delete();

            await set.Add("a", 10);

            (await set.Increment("a", 1)).Is(11);
            (await set.Increment("a", 100)).Is(111);

            (await set.Decrement("a", 100)).Is(11);
            (await set.Decrement("a", 1)).Is(10);

            (await set.Decrement("a", -1)).Is(11);
            (await set.Decrement("a", -100)).Is(111);

            (await set.Increment("a", -100)).Is(11);
            (await set.Increment("a", -1)).Is(10);
        }

        [TestMethod]
        public async Task RankScore()
        {
            var set = new RedisSortedSet<string>(GlobalSettings.Default, "set");
            await set.Delete();
            await set.Add("a", 10);
            await set.Add("b", 100);
            await set.Add("c", 1000);
            await set.Add("d", 10000);
            await set.Add("e", 100000);
            await set.Add("f", 1000000);

            (await set.Rank("a")).Is(0);
            (await set.Rank("b")).Is(1);
            (await set.Rank("c")).Is(2);
            (await set.Rank("d")).Is(3);
            (await set.Rank("e")).Is(4);
            (await set.Rank("f")).Is(5);
            (await set.Rank("f", Order.Descending)).Is(0);
            (await set.Rank("e", Order.Descending)).Is(1);
            (await set.Rank("d", Order.Descending)).Is(2);
            (await set.Rank("c", Order.Descending)).Is(3);
            (await set.Rank("b", Order.Descending)).Is(4);
            (await set.Rank("a", Order.Descending)).Is(5);

            (await set.Rank("z")).IsNull();

            (await set.Score("a")).Is(10);
            (await set.Score("b")).Is(100);
            (await set.Score("c")).Is(1000);
            (await set.Score("d")).Is(10000);
            (await set.Score("e")).Is(100000);
            (await set.Score("f")).Is(1000000);
        }

        [TestMethod]
        public async Task Remove()
        {
            var set = new RedisSortedSet<string>(GlobalSettings.Default, "set");
            await set.Delete();
            await set.Add("a", 10);
            await set.Add("b", 100);
            await set.Add("c", 1000);
            await set.Add("d", 10000);
            await set.Add("e", 100000);
            await set.Add("f", 1000000);

            var c = await set.Get("c");
            c.Value.Is("c");
            c.Rank.Is(2);
            c.Score.Is(1000);
            (await set.Remove("c")).IsTrue();
            (await set.Remove("c")).IsFalse();
            (await set.Get("c")).IsNull();

            await set.Add("c", 1000);
            (await set.RemoveRangeByRank(2, 3)).Is(2);
            (await set.RangeByRank()).Is("a", "b", "e", "f");

            await set.Delete();
            await set.Add("a", 10);
            await set.Add("b", 100);
            await set.Add("c", 1000);
            await set.Add("d", 10000);
            await set.Add("e", 100000);
            await set.Add("f", 1000000);
            (await set.RemoveRangeByScore(100, 10000)).Is(3);
            (await set.RangeByRank()).Is("a", "e", "f");
        }
    }
}