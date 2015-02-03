using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Threading.Tasks;
using System.Text;
using System.Threading;

namespace CloudStructures.Tests
{
    [TestClass]
    public class CommandTracerTests
    {
        public class MyClass
        {
            public int MyProperty { get; set; }
            public string Moge { get; set; }
            public Nest[] Array { get; set; }
        }

        public class Nest
        {
            public string Huga { get; set; }
        }

        [TestMethod]
        public void Object()
        {
            var s = new RedisString<MyClass>(GlobalSettings.Default, "CommandTracerTests1");
            s.Settings.GetConnection().GetServer("127.0.0.1:6379").FlushAllDatabases();

            s.Set(null).Result.IsFalse();
            s.Get().Result.HasValue.IsFalse();

            s.Set(new MyClass()).Result.IsTrue();
            s.Get().Result.Value.Array.IsNull();

            s.Set(new MyClass() { Array = new Nest[0] }).Result.IsTrue();
            s.Get().Result.Value.Array.Length.Is(0);


            s.Set(new MyClass() { Array = new Nest[] { new Nest { Huga = "hoge" } } }).Result.IsTrue();
            s.Get().Result.Value.Array[0].Huga.Is("hoge");
        }

        [TestMethod]
        public void Nullable()
        {
            var s = new RedisString<int?>(GlobalSettings.Default, "CommandTracerTests1");
            s.Settings.GetConnection().GetServer("127.0.0.1:6379").FlushAllDatabases();

            s.Set(null).Result.IsFalse();
            s.Get().Result.HasValue.IsFalse();

            s.Set(100).Result.IsTrue();
            s.Get().Result.Value.Is(100);

            s.Increment(200).Result.Is(300);
        }
    }
}