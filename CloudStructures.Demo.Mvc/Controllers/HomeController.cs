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
            await RedisGroups.Demo.List<Person>("ListDemo").AddLast(new Person { Name = "Hoge", Age = 10 });

            var ids = new[] { 12, 3124, 51, 636, 6714 };
            var rand = new Random();
            
            // you can watch parallel execution
            await Task.WhenAll(ids.Select(async x =>
            {
                await RedisGroups.Demo.String<int>("TestInc.Id." + x).Increment(rand.Next(1, 10));
            }).ToArray());


            await RedisGroups.Demo.List<Person>("ListDemo").Range(1, 10);
            await RedisGroups.Demo.List<Person>("ListDemo").Range(1, 10);

            return View();
        }

    }
}
