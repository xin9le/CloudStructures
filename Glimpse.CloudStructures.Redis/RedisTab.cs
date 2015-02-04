using Glimpse.CloudStructures.Redis;
using Glimpse.Core.Extensibility;
using System.Linq;
using Glimpse.Core.Extensions;
using Glimpse.Core.Tab.Assist;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace Glimpse.CloudStructures.Redis
{
    public class RedisTab : TabBase, ITabSetup, ITabLayout, ILayoutControl
    {
        private static readonly object Layout = TabLayout.Create()
            .Row(r =>
            {
                r.Cell(0).WithTitle("Command").WidthInPercent(10);
                r.Cell(1).WithTitle("Key").WidthInPercent(10);
                r.Cell(2).WithTitle("Sent").WidthInPercent(30);
                r.Cell(3).WithTitle("Size");
                r.Cell(4).WithTitle("Received").WidthInPercent(30);
                r.Cell(5).WithTitle("Size");
                r.Cell(6).WithTitle("Expire").WidthInPercent(5);
                r.Cell(7).Suffix(" ms").WithTitle("Duration").WidthInPercent(5);
            }).Build();


        public override string Name
        {
            get { return "Redis"; }
        }

        public bool KeysHeadings
        {
            get { return true; }
        }

        public void Setup(ITabSetupContext context)
        {
            context.PersistMessages<RedisTimelineMessage>();
        }

        public object GetLayout()
        {
            return Layout;
        }

        public override object GetData(ITabContext context)
        {
            var plugin = Plugin.Create("Command", "Key", "Sent", "Size", "Received", "Size", "Expire", "Duration");

            var messages = context.GetMessages<RedisTimelineMessage>().ToArray();

            var ttl = Task.WhenAll(messages.Select(async x =>
            {
                var conn = x.UsedSettings.GetConnection();
                var db = conn.GetDatabase(x.UsedSettings.Db);
                var exists = await db.KeyExistsAsync(x.Key).ConfigureAwait(false);
                if (exists)
                {
                    var v = await db.KeyTimeToLiveAsync(x.Key).ConfigureAwait(false);
                    return (v.HasValue) ? v.Value.ToString(@"hh\:mm\:ss") : "-";
                }
                else
                {
                    return null;
                }
            }).ToArray()).Result;

            var duplicatedKey = new HashSet<Tuple<string, RedisKey>>();
            foreach (var item in messages.Zip(ttl, (message, expire) => new { message, expire }))
            {
                var message = item.message;
                var key = Tuple.Create(message.Command, message.Key);

                var columns = plugin.AddRow()
                    .Column(message.Command)
                    .Column((string)message.Key)
                    .Column((message.SentObject == null) ? null : message.SentObject)
                    .Column((message.SentObject == null) ? null : (long?)message.SentSize)
                    .Column((message.ReceivedObject == null) ? null : message.ReceivedObject)
                    .Column((message.ReceivedObject == null) ? null : (long?)message.ReceivedSize)
                    .Column(item.expire)
                    .Column(message.Duration);
                columns.WarnIf(duplicatedKey.Contains(key));
                columns.ErrorIf(message.IsError);
                duplicatedKey.Add(key);
            }

            return plugin;
        }
    }
}