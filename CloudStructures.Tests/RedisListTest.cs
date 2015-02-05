using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Threading.Tasks;
using System.Text;
using System.Threading;

namespace CloudStructures.Tests
{
    [TestClass]
    public class RedisListTest
    {
        [TestMethod]
        public async Task Push()
        {
            var list = new RedisList<int>(GlobalSettings.Default, "listkey1");
            await list.Delete();

            (await list.LeftPush(1)).Is(1);
            (await list.LeftPush(10)).Is(2);
            (await list.LeftPush(new[] { 100, 1000, 10000 }, TimeSpan.FromMilliseconds(1000))).Is(5);

            (await list.Range()).Is(10000, 1000, 100, 10, 1);

            await Task.Delay(TimeSpan.FromMilliseconds(1500));

            (await list.KeyExists()).IsFalse();

            (await list.RightPush(1)).Is(1);
            (await list.RightPush(10)).Is(2);
            (await list.RightPush(new[] { 100, 1000, 10000 }, TimeSpan.FromMilliseconds(1000))).Is(5);

            (await list.Range()).Is(1, 10, 100, 1000, 10000);

            await Task.Delay(TimeSpan.FromMilliseconds(1500));

            (await list.KeyExists()).IsFalse();
        }

        [TestMethod]
        public async Task GetByIndexRemove()
        {
            var list = new RedisList<int>(GlobalSettings.Default, "listkey2");
            await list.Delete();

            await list.RightPush(new[] { 1, 2, 3, 4, 5 });

            (await list.GetByIndex(1)).Value.Is(2);

            (await list.GetByIndex(10)).HasValue.IsFalse();

            (await list.Length()).Is(5);

            await list.RightPush(new[] { 1, 2, 3, 4, 5, 3 });

            await list.Remove(3);
            (await list.Range()).Is(1, 2, 4, 5, 1, 2, 4, 5);

            await list.Remove(4, 1);
            (await list.Range()).Is(1, 2, 5, 1, 2, 4, 5);

            await list.Remove(5, -1);
            (await list.Range()).Is(1, 2, 5, 1, 2, 4);
        }

        [TestMethod]
        public async Task LeftPushAndFixLength()
        {
            var list = new RedisList<int>(GlobalSettings.Default, "listkey3");
            await list.Delete();

            await list.LeftPush(new[] { 1, 2, 3, 4, 5 });
            await list.LeftPush(new[] { 6, 7, 8, 9, 10 });

            (await list.Range()).Is(10, 9, 8, 7, 6, 5, 4, 3, 2, 1);
            await list.LeftPushAndFixLength(100, 10);
            (await list.Range()).Is(100, 10, 9, 8, 7, 6, 5, 4, 3, 2);
            await list.LeftPushAndFixLength(1000, 3);
            (await list.Range()).Is(1000, 100, 10);
        }

        [TestMethod]
        public async Task Insert()
        {
            var list = new RedisList<int>(GlobalSettings.Default, "listkey4");
            await list.Delete();

            await list.RightPush(new[] { 1, 2, 3, 4, 5 });
            (await list.Range()).Is(1, 2, 3, 4, 5);

            (await list.InsertBefore(4, 1000)).Is(6);
            (await list.Range()).Is(1, 2, 3, 1000, 4, 5);

            (await list.InsertAfter(4, 2000)).Is(7);
            (await list.Range()).Is(1, 2, 3, 1000, 4, 2000, 5);
        }

        [TestMethod]
        public async Task EmptyRange()
        {
            var list = new RedisList<int>(GlobalSettings.Default, "listkey5");
            await list.Delete();

            (await list.Range()).Length.Is(0);
        }

        [TestMethod]
        public async Task Pop()
        {
            var list = new RedisList<int>(GlobalSettings.Default, "listkey6");
            await list.Delete();

            await list.RightPush(new[] { 1, 2, 3, 4, 5, 6, 7, 8, 9 });

            (await list.LeftPop()).Value.Is(1);
            (await list.RightPop()).Value.Is(9);

            (await list.Range()).Is(2, 3, 4, 5, 6, 7, 8);
        }
    }
}