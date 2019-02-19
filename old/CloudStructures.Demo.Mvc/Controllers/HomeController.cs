using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using System.Web.Mvc;

namespace CloudStructures.Demo.Mvc.Controllers
{
    public class Person
    {
        public string Name { get; set; }
        public int Age { get; set; }
    }

    public class HomeController : Controller
    {
        public async Task<ActionResult> Index()
        {
            var list = RedisGroups.Demo.List<Person>("ListDemo");

            await list.Delete();
            await list.RightPush(new Person { Name = "Hoge", Age = 10 });
            await list.Expire(TimeSpan.FromSeconds(15));

            var ids = new[] { 12 };//, 3124, 51, 636, 6714 };
            var rand = new Random();

            // you can watch parallel execution
            await Task.WhenAll(ids.Select(async x =>
            {
                await RedisGroups.Demo.String<int>("TestInc.Id." + x).Increment(rand.Next(1, 10));
            }).ToArray());


            await list.Range(0, 10);
            await list.Range(0, 10);

            var str = RedisGroups.Demo.String<int>("aaa");
            await str.Set(1000);
            await str.Get();
            await str.Delete();
            await str.Get(); // it's null!


            return View();
        }

    }
}
