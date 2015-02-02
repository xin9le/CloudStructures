using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CloudStructures
{
    public class RedisList<T> : RedisStructure
    {
        protected override string CallType
        {
            get { return "RedisList"; }
        }

        public RedisList(RedisSettings settings, string listKey)
            : base(settings, listKey)
        {
        }

        public RedisList(RedisGroup connectionGroup, string listKey)
            : base(connectionGroup, listKey)
        {
        }

        /// <summary>
        /// LPUSH http://redis.io/commands/lpush
        /// </summary>
        public Task<long> LeftPush(T value, When when = When.Always, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                long sentSize;
                var v = Settings.ValueConverter.Serialize(value, out sentSize);
                var vr = await Command.ListLeftPushAsync(Key, v, when, commandFlags).ConfigureAwait(false);
                return Pair.CreatePair(new { value }, sentSize, vr, sizeof(long));
            });
        }

        /// <summary>
        /// LPUSH http://redis.io/commands/lpush
        /// </summary>
        public Task<long> LeftPush(T[] values, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var sentSize = 0L;
                var redisValues = values.Select(x =>
                {
                    long _size;
                    var rv = Settings.ValueConverter.Serialize(x, out _size);
                    sentSize += _size;
                    return rv;
                }).ToArray();

                var vr = await Command.ListLeftPushAsync(Key, redisValues, commandFlags).ConfigureAwait(false);
                return Pair.CreatePair(new { values }, sentSize, vr, sizeof(long));
            });
        }

        /// <summary>
        /// RPUSH http://redis.io/commands/rpush
        /// </summary>
        public Task<long> RightPush(T value, When when = When.Always, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                long sentSize;
                var v = Settings.ValueConverter.Serialize(value, out sentSize);
                var vr = await Command.ListRightPushAsync(Key, v, when, commandFlags).ConfigureAwait(false);
                return Pair.CreatePair(new { value }, sentSize, vr, sizeof(long));
            });
        }

        /// <summary>
        /// RPUSH http://redis.io/commands/rpush
        /// </summary>
        public Task<long> RightPush(T[] values, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var sentSize = 0L;
                var redisValues = values.Select(x =>
                {
                    long _size;
                    var rv = Settings.ValueConverter.Serialize(x, out _size);
                    sentSize += _size;
                    return rv;
                }).ToArray();

                var vr = await Command.ListRightPushAsync(Key, redisValues, commandFlags).ConfigureAwait(false);
                return Pair.CreatePair(new { values }, sentSize, vr, sizeof(long));
            });
        }

        ///// <summary>
        ///// LINDEX http://redis.io/commands/lindex
        ///// </summary>
        //public Task<Tuple<bool, T>> TryGet(int index, CommandFlags commandFlags = CommandFlags.None)
        //{
        //    return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
        //    {
        //        var value = await Command.Get(Settings.Db, Key, index, commandFlags).ConfigureAwait(false);
        //        var result = (value == null)
        //            ? Tuple.Create(false, default(T))
        //            : Tuple.Create(true, Settings.ValueConverter.Deserialize<T>(value));
        //        return Pair.Create(new { index }, result);
        //    });
        //}

        //public async Task<T> GetOrDefault(int index, T defaultValue = default(T), CommandFlags commandFlags = CommandFlags.None)
        //{
        //    var result = await TryGet(index, commandFlags).ConfigureAwait(false);
        //    return result.Item1 ? result.Item2 : defaultValue;
        //}

        ///// <summary>
        ///// LLEN http://redis.io/commands/llen
        ///// </summary>
        //public Task<long> GetLength(CommandFlags commandFlags = CommandFlags.None)
        //{
        //    return TraceHelper.RecordReceive(Settings, Key, CallType, () =>
        //    {
        //        return Command.GetLength(Settings.Db, Key, commandFlags);
        //    });
        //}

        ///// <summary>
        ///// LRANGE http://redis.io/commands/lrange
        ///// </summary>
        //public Task<T[]> Range(int start, int stop, CommandFlags commandFlags = CommandFlags.None)
        //{
        //    return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
        //    {
        //        var results = await Command.Range(Settings.Db, Key, start, stop, commandFlags).ConfigureAwait(false);
        //        var resultArray = results.Select(Settings.ValueConverter.Deserialize<T>).ToArray();
        //        return Pair.Create(new { start, stop }, resultArray);
        //    });
        //}

        ///// <summary>
        ///// LREM http://redis.io/commands/lrem
        ///// </summary>
        //public Task<long> Remove(T value, int count = 1, CommandFlags commandFlags = CommandFlags.None)
        //{
        //    return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
        //    {
        //        var v = Settings.ValueConverter.Serialize(value);
        //        var r = await Command.Remove(Settings.Db, Key, v, count, commandFlags).ConfigureAwait(false);

        //        return Pair.Create(new { value, count }, r);
        //    });
        //}

        ///// <summary>
        ///// LPOP http://redis.io/commands/lpop
        ///// </summary>
        //public Task<T> RemoveFirst(CommandFlags commandFlags = CommandFlags.None)
        //{
        //    return TraceHelper.RecordReceive(Settings, Key, CallType, async () =>
        //    {
        //        var result = await Command.RemoveFirst(Settings.Db, Key, commandFlags).ConfigureAwait(false);
        //        return Settings.ValueConverter.Deserialize<T>(result);
        //    });
        //}

        ///// <summary>
        ///// RPOP http://redis.io/commands/rpop
        ///// </summary>
        //public Task<T> RemoveLast(CommandFlags commandFlags = CommandFlags.None)
        //{
        //    return TraceHelper.RecordReceive(Settings, Key, CallType, async () =>
        //    {
        //        var result = await Command.RemoveLast(Settings.Db, Key, commandFlags).ConfigureAwait(false);
        //        return Settings.ValueConverter.Deserialize<T>(result);
        //    });
        //}

        ///// <summary>
        ///// LSET http://redis.io/commands/lset
        ///// </summary>
        //public Task Set(int index, T value, CommandFlags commandFlags = CommandFlags.None)
        //{
        //    return TraceHelper.RecordSend(Settings, Key, CallType, async () =>
        //    {
        //        var v = Settings.ValueConverter.Serialize(value);
        //        await Command.Set(Settings.Db, Key, index, v, commandFlags).ConfigureAwait(false);
        //        return new { index, value };
        //    });
        //}

        ///// <summary>
        ///// LTRIM http://redis.io/commands/ltrim
        ///// </summary>
        //public Task Trim(int count, CommandFlags commandFlags = CommandFlags.None)
        //{
        //    return TraceHelper.RecordSend(Settings, Key, CallType, async () =>
        //    {
        //        await Command.Trim(Settings.Db, Key, count, commandFlags);
        //        return new { count };
        //    });
        //}

        ///// <summary>
        ///// LTRIM http://redis.io/commands/ltrim
        ///// </summary>
        //public Task Trim(int start, int stop, CommandFlags commandFlags = CommandFlags.None)
        //{
        //    return TraceHelper.RecordSend(Settings, Key, CallType, async () =>
        //    {
        //        await Command.Trim(Settings.Db, Key, start, stop, commandFlags).ConfigureAwait(false);
        //        return new { start, stop };
        //    });
        //}

        //// additional commands

        //public Task<long> AddFirstAndFixLength(T value, int fixLength, CommandFlags commandFlags = CommandFlags.None)
        //{
        //    return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
        //    {
        //        var v = Settings.ValueConverter.Serialize(value);
        //        using (var tx = Settings.GetConnection().CreateTransaction())
        //        {
        //            var addResult = tx.Lists.AddFirst(Settings.Db, Key, v, createIfMissing: true, commandFlags);
        //            var trimResult = tx.Lists.Trim(Settings.Db, Key, count: fixLength, commandFlags);

        //            await tx.Execute(commandFlags).ConfigureAwait(false);
        //            var result = await addResult.ConfigureAwait(false);
        //            return Pair.Create(new { value, fixLength }, result);
        //        }
        //    });
        //}

        //public Task<T[]> ToArray(CommandFlags commandFlags = CommandFlags.None)
        //{
        //    return TraceHelper.RecordReceive(Settings, Key, CallType, () =>
        //    {
        //        return Range(0, -1, commandFlags);
        //    });
        //}

        ///// <summary>
        ///// Clear is alias of Delete.
        ///// </summary>
        //public Task<bool> Clear(CommandFlags commandFlags = CommandFlags.None)
        //{
        //    return Delete(commandFlags);
        //}
    }
}