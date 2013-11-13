using Glimpse.CloudStructures.Redis;
using Glimpse.Core.Extensibility;
using Glimpse.Core.Extensions;
using Glimpse.Core.Tab.Assist;
using Newtonsoft.Json;
using System.Collections.Generic;

namespace Grani.Glimpse.Tab
{
    public class RedisTab : TabBase, ITabSetup, ITabLayout, ILayoutControl
    {
        private static readonly object Layout = TabLayout.Create()
            .Row(r =>
            {
                r.Cell(0).WithTitle("Command");
                r.Cell(1).WithTitle("Key");
                r.Cell(2).WithTitle("Sent");
                r.Cell(3).WithTitle("Received");
                r.Cell(4).Suffix(" ms").WithTitle("Duration");
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
            var plugin = Plugin.Create("Command", "Key", "Sent", "Received", "Duration");

            var duplicatedKey = new HashSet<string>();

            foreach (var message in context.GetMessages<RedisTimelineMessage>())
            {
                var key = message.Command + ":" + message.Key;

                var columns = plugin.AddRow()
                    .Column(message.Command)
                    .Column(message.Key)
                    .Column((message.SendObject == null) ? null : JsonConvert.SerializeObject(message.SendObject))
                    .Column((message.ReceivedObject == null) ? null : JsonConvert.SerializeObject(message.ReceivedObject ?? ""))
                    .Column(message.Duration);
                columns.WarnIf(duplicatedKey.Contains(key));
                columns.ErrorIf(message.IsError);
                duplicatedKey.Add(key);
            }

            return plugin;
        }
    }
}