using StackExchange.Redis;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace CloudStructures
{
    public class RedisSortedSet<T> : RedisStructure
    {
        protected override string CallType
        {
            get { return "RedisSortedSet"; }
        }

        public RedisSortedSet(RedisSettings settings, RedisKey listKey)
            : base(settings, listKey)
        {
        }

        public RedisSortedSet(RedisGroup connectionGroup, RedisKey listKey)
            : base(connectionGroup, listKey)
        {
        }

        /// <summary>
        /// ZADD http://redis.io/commands/zadd
        /// </summary>
        public Task<bool> Add(T value, double score, RedisExpiry expiry = null, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                long sentSize;
                var v = Settings.ValueConverter.Serialize(value, out sentSize);
                var vr = await this.ExecuteWithKeyExpire(x => x.SortedSetAddAsync(Key, v, score, commandFlags), Key, expiry, commandFlags).ForAwait();

                return Tracing.CreateSentAndReceived(new { value, score, expiry = expiry?.Value }, sentSize, vr, sizeof(bool));
            });
        }

        /// <summary>
        /// ZADD http://redis.io/commands/zadd
        /// </summary>
        public Task<long> Add(IEnumerable<KeyValuePair<T, double>> values, RedisExpiry expiry = null, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                // materialize
                if (!(values is ICollection))
                {
                    values = values.ToArray();
                }

                long sentSize = 0;
                var sendValues = values.Select(x =>
                {
                    long s;
                    var v = Settings.ValueConverter.Serialize(x, out s);
                    sentSize += s;
                    return new SortedSetEntry(v, x.Value);
                }).ToArray();

                var vr = await this.ExecuteWithKeyExpire(x => x.SortedSetAddAsync(Key, sendValues, commandFlags), Key, expiry, commandFlags).ForAwait();
                return Tracing.CreateSentAndReceived(new { values, expiry = expiry?.Value }, sentSize, vr, sizeof(long));
            });
        }

        /// <summary>
        /// ZCARD, ZCOUNT http://redis.io/commands/zcard http://redis.io/commands/zcount
        /// </summary>
        public Task<long> Length(double min = double.NegativeInfinity, double max = double.PositiveInfinity, Exclude exclude = Exclude.None, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
             {
                 var r = await Command.SortedSetLengthAsync(Key, min, max, exclude, commandFlags).ForAwait();
                 return Tracing.CreateSentAndReceived(new { min, max, exclude }, sizeof(double) * 2 + sizeof(int), r, sizeof(long));
             });
        }

        /// <summary>
        /// ZINCRBY http://redis.io/commands/zincrby
        /// </summary>
        public Task<double> Increment(T member, double value, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                long size;
                var v = Settings.ValueConverter.Serialize(member, out size);
                var r = await Command.SortedSetIncrementAsync(Key, v, value, commandFlags).ConfigureAwait(false);
                return Tracing.CreateSentAndReceived(new { member, value }, size, r, sizeof(double));
            });
        }

        /// <summary>
        /// ZINCRBY http://redis.io/commands/zincrby
        /// </summary>
        public Task<double> Decrement(T member, double value, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                long size;
                var v = Settings.ValueConverter.Serialize(member, out size);
                var r = await Command.SortedSetDecrementAsync(Key, v, value, commandFlags).ConfigureAwait(false);
                return Tracing.CreateSentAndReceived(new { member, value }, size, r, sizeof(double));
            });
        }


        /// <summary>
        /// ZRANGE, ZREVRANGE http://redis.io/commands/zrange http://redis.io/commands/zrevrange
        /// </summary>
        public Task<T[]> RangeByRank(long start = 0, long stop = -1, Order order = Order.Ascending, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var rs = await Command.SortedSetRangeByRankAsync(Key, start, stop, order, commandFlags).ForAwait();
                long size = 0;
                var result = rs.Select(x =>
                {
                    long s;
                    var v = Settings.ValueConverter.Deserialize<T>(x, out s);
                    size += s;
                    return v;
                }).ToArray();

                return Tracing.CreateSentAndReceived(new { start, stop, order }, sizeof(long) * 2 + sizeof(int), result, size);
            });
        }

        /// <summary>
        /// ZRANGE, ZREVRANGE http://redis.io/commands/zrange http://redis.io/commands/zrevrange
        /// </summary>
        public Task<SortedSetResult<T>[]> RangeByRankWithScores(long start = 0, long stop = -1, Order order = Order.Ascending, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var rs = await Command.SortedSetRangeByRankWithScoresAsync(Key, start, stop, order, commandFlags).ForAwait();
                long size = 0;
                var result = rs.Select((x, i) =>
                {
                    long s;
                    var v = Settings.ValueConverter.Deserialize<T>(x.Element, out s);
                    size += s;
                    return new SortedSetResult<T>(v, x.Score);
                }).ToArray();

                return Tracing.CreateSentAndReceived(new { start, stop, order }, sizeof(long) * 2 + sizeof(int), result, size);
            });
        }

        /// <summary>
        /// ZRANGE, ZREVRANGE http://redis.io/commands/zrange http://redis.io/commands/zrevrange
        /// </summary>
        public Task<SortedSetResultWithRank<T>[]> RangeByRankWithScoresAndRank(long start = 0, long stop = -1, Order order = Order.Ascending, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                SortedSetEntry[] rs;
                long startIndex = 0;
                if (start >= 0)
                {
                    rs = await Command.SortedSetRangeByRankWithScoresAsync(Key, start, stop, order, commandFlags).ForAwait();
                    startIndex = start;
                }
                else
                {
                    var tx = CreateTransaction();
                    var lengthFuture = tx.SortedSetLengthAsync(Key, flags: commandFlags);
                    var rsFuture = tx.SortedSetRangeByRankWithScoresAsync(Key, start, stop, order, commandFlags);
                    await tx.ExecuteAsync(commandFlags).ForAwait();
                    rs = await rsFuture.ForAwait();
                    var length = await lengthFuture.ForAwait();

                    startIndex = length + start;
                }

                long size = 0;
                var result = rs.Select((x, i) =>
                {
                    long s;
                    var v = Settings.ValueConverter.Deserialize<T>(x.Element, out s);
                    size += s;
                    var rank = startIndex + i;
                    return new SortedSetResultWithRank<T>(v, x.Score, rank);
                }).ToArray();

                return Tracing.CreateSentAndReceived(new { start, stop, order }, sizeof(long) * 2 + sizeof(int), result, size);
            });
        }

        /// <summary>
        /// ZRANGEBYSCORE, ZREVRANGEBYSCORE http://redis.io/commands/zrangebyscore http://redis.io/commands/zrevrangebyscore
        /// </summary>
        public Task<T[]> RangeByScore(double start = double.NegativeInfinity, double stop = double.PositiveInfinity, Exclude exclude = Exclude.None, Order order = Order.Ascending, long skip = 0, long take = -1, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var values = await Command.SortedSetRangeByScoreAsync(Key, start, stop, exclude, order, skip, take, commandFlags).ForAwait();

                long size = 0;
                var result = values.Select(x =>
                {
                    long s;
                    var v = Settings.ValueConverter.Deserialize<T>(x, out s);
                    size += s;
                    return v;
                }).ToArray();

                return Tracing.CreateSentAndReceived(new { start, stop, exclude, order, skip, take }, sizeof(long) * 2 + sizeof(int) * 2 + sizeof(double) * 2, result, size);
            });
        }

        /// <summary>
        /// ZRANGEBYSCORE, ZREVRANGEBYSCORE http://redis.io/commands/zrangebyscore http://redis.io/commands/zrevrangebyscore
        /// </summary>
        public Task<SortedSetResult<T>[]> RangeByScoreWithScores(double start = double.NegativeInfinity, double stop = double.PositiveInfinity, Exclude exclude = Exclude.None, Order order = Order.Ascending, long skip = 0, long take = -1, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                var values = await Command.SortedSetRangeByScoreWithScoresAsync(Key, start, stop, exclude, order, skip, take, commandFlags).ForAwait();

                long size = 0;
                var result = values.Select(x =>
                {
                    long s;
                    var v = Settings.ValueConverter.Deserialize<T>(x.Element, out s);
                    size += s;
                    return new SortedSetResult<T>(v, x.Score);
                }).ToArray();

                return Tracing.CreateSentAndReceived(new { start, stop, exclude, order, skip, take }, sizeof(long) * 2 + sizeof(int) * 2 + sizeof(double) * 2, result, size);
            });
        }

        /// <summary>
        /// ZRANGEBYLEX http://redis.io/commands/zrangebylex
        /// </summary>
        public Task<T[]> RangeByValue(T min = default(T), T max = default(T), Exclude exclude = Exclude.None, long skip = 0, long take = -1, CommandFlags commandFlags = CommandFlags.None)
        {
            return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
            {
                long minSize;
                var minValue = Settings.ValueConverter.Serialize(min, out minSize);
                long maxSize;
                var maxValue = Settings.ValueConverter.Serialize(max, out maxSize);

                var values = await Command.SortedSetRangeByValueAsync(Key, minValue, maxValue, exclude, skip, take, commandFlags).ForAwait();

                long size = 0;
                var result = values.Select(x =>
                {
                    long s;
                    var v = Settings.ValueConverter.Deserialize<T>(x, out s);
                    size += s;
                    return v;
                }).ToArray();

                return Tracing.CreateSentAndReceived(new { min, max, exclude, skip, take }, minSize + maxSize, result, size);
            });
        }


        ///// <summary>
        ///// ZRANK http://redis.io/commands/zrank
        ///// </summary>
        //public Task<long?> Rank(T member, bool ascending = true, CommandFlags commandFlags = CommandFlags.None)
        //{
        //    return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
        //    {
        //        var r = await Command.Rank(Settings.Db, Key, Settings.ValueConverter.Serialize(member), ascending, commandFlags).ConfigureAwait(false);
        //        return Pair.Create(new { member, ascending }, r);
        //    });
        //}

        ///// <summary>
        ///// ZREM http://redis.io/commands/zrem
        ///// </summary>
        //public Task<bool> Remove(T member, CommandFlags commandFlags = CommandFlags.None)
        //{
        //    return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
        //    {
        //        var r = await Command.Remove(Settings.Db, Key, Settings.ValueConverter.Serialize(member), commandFlags).ConfigureAwait(false);
        //        return Pair.Create(new { member }, r);
        //    });
        //}

        ///// <summary>
        ///// ZREM http://redis.io/commands/zrem
        ///// </summary>
        //public Task<long> Remove(T[] members, CommandFlags commandFlags = CommandFlags.None)
        //{
        //    return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
        //    {
        //        var v = members.Select(x => Settings.ValueConverter.Serialize(x)).ToArray();
        //        var r = await Command.Remove(Settings.Db, Key, v, commandFlags).ConfigureAwait(false);
        //        return Pair.Create(new { members }, r);
        //    });
        //}

        ///// <summary>
        ///// ZREMRANGEBYRANK http://redis.io/commands/zremrangebyrank
        ///// </summary>
        //public Task<long> RemoveRange(long start, long stop, CommandFlags commandFlags = CommandFlags.None)
        //{
        //    return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
        //    {
        //        var r = await Command.RemoveRange(Settings.Db, Key, start, stop, commandFlags).ConfigureAwait(false);
        //        return Pair.Create(new { start, stop }, r);
        //    });
        //}

        ///// <summary>
        ///// ZREMRANGEBYSCORE http://redis.io/commands/zremrangebyscore
        ///// </summary>
        //public Task<long> RemoveRange(double min, double max, bool minInclusive = true, bool maxInclusive = true, CommandFlags commandFlags = CommandFlags.None)
        //{
        //    return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
        //    {
        //        var r = await Command.RemoveRange(Settings.Db, Key, min, max, minInclusive, maxInclusive, commandFlags).ConfigureAwait(false);
        //        return Pair.Create(new { min, max, minInclusive, maxInclusive }, r);
        //    });
        //}

        ///// <summary>
        ///// ZSCORE http://redis.io/commands/zscore
        ///// </summary>
        //public Task<double?> Score(T member, CommandFlags commandFlags = CommandFlags.None)
        //{
        //    return TraceHelper.RecordSendAndReceive(Settings, Key, CallType, async () =>
        //    {
        //        var r = await Command.Score(Settings.Db, Key, Settings.ValueConverter.Serialize(member), commandFlags).ConfigureAwait(false);
        //        return Pair.Create(new { member }, r);
        //    });
        //}
    }
}