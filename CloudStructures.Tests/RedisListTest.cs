using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using CloudStructures.Redis;
using System.Threading.Tasks;
using System.Text;
using System.Threading;

namespace CloudStructures.Tests
{
    [TestClass]
    public class RedisListTest
    {
        [TestMethod]
        public async Task Add()
        {
            var list = new CloudStructures.Redis.RedisList<int>(GlobalSettings.Default, "listkey1");

            await list.Clear();

            (await list.AddLast(1)).Is(1);
            (await list.AddLast(10)).Is(2);
            (await list.AddFirst(100)).Is(3);
            (await list.AddFirst(1000)).Is(4);

            (await list.GetLength()).Is(4);

            (await list.Range(0, 4)).Is(1000, 100, 1, 10);

            (await list.Range(2, 3)).Is(1, 10);
        }
    }
}