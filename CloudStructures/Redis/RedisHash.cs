using BookSleeve;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CloudStructures.Redis
{
    public class RedisDictionary<T> : IObservable<KeyValuePair<string, T>>
    {
        public string Key { get; private set; }
        // TODO:db
        readonly RedisSettings settings;

        // TODO:transaction
        public RedisDictionary(RedisSettings settings, string hashKey)
        {
            this.settings = settings;
            this.Key = hashKey;
        }

        public RedisDictionary(RedisGroup connectionGroup, string hashKey)
            : this(connectionGroup.GetSettings(hashKey), hashKey)
        {
        }

        protected RedisConnection Connection
        {
            get
            {
                return settings.GetConnection();
            }
        }

        protected IHashCommands Command
        {
            get
            {
                return Connection.Hashes;
            }
        }

        // TODO:Implemente all methods

        // ToDictionary, AsObservable


        public IDisposable Subscribe(IObserver<KeyValuePair<string, T>> observer)
        {
            throw new NotImplementedException();
        }
    }

    /*
    // : RedisDictionary<object> ?
    public class RedisHash : IObservable<KeyValuePair<string, object>>
    {
        public string Key { get; private set; }
        readonly RedisSettings settings;

        public RedisHash(RedisSettings settings, string hashKey)
        {
            this.settings = settings;
            this.Key = hashKey;
        }

        public RedisHash(RedisGroup connectionGroup, string hashKey)
            : this(connectionGroup.GetSettings(hashKey), hashKey)
        {
        }

        protected RedisConnection Connection
        {
            get
            {
                return settings.GetConnection();
            }
        }

        protected IHashCommands Command
        {
            get
            {
                return Connection.Hashes;
            }
        }

        // TODO:Implemente all methods

        public IDisposable Subscribe(IObserver<KeyValuePair<string, object>> observer)
        {
            throw new NotImplementedException();
        }
    }
    */

    /// <summary>
    /// Class mapped RedisHash
    /// </summary>
    public class RedisClass<T> where T : class, new()
    {
        public string Key { get; private set; }
        public int Db { get; private set; }
        readonly RedisSettings settings;
        readonly RedisTransaction transaction;
        readonly IRedisValueConverter valueConverter;
        readonly Func<T> valueFactory;
        readonly int? expirySeconds;

        public RedisClass(RedisSettings settings, string hashKey, Func<T> valueFactoryIfNotExists = null, int? expirySeconds = null)
        {
            this.settings = settings;
            this.Db = settings.Db;
            this.valueConverter = settings.ValueConverter;
            this.Key = hashKey;
            this.valueFactory = valueFactoryIfNotExists;
            this.expirySeconds = expirySeconds;
        }

        public RedisClass(RedisGroup connectionGroup, string hashKey, Func<T> valueFactoryIfNotExists = null, int? expirySeconds = null)
            : this(connectionGroup.GetSettings(hashKey), hashKey, valueFactoryIfNotExists, expirySeconds)
        {
        }

        public RedisClass(RedisTransaction transaction, int db, IRedisValueConverter valueConverter, string hashKey, Func<T> valueFactoryIfNotExists = null, int? expirySeconds = null)
        {
            this.transaction = transaction;
            this.Db = db;
            this.valueConverter = valueConverter;
            this.Key = hashKey;
            this.valueFactory = valueFactoryIfNotExists;
            this.expirySeconds = expirySeconds;
        }

        protected RedisConnection Connection
        {
            get
            {
                return (transaction == null) ? settings.GetConnection() : transaction;
            }
        }

        protected IHashCommands Command
        {
            get
            {
                return Connection.Hashes;
            }
        }

        public virtual async Task<T> GetValue(bool queueJump = false)
        {
            var data = await Command.GetAll(Db, Key, queueJump).ConfigureAwait(false);
            if (data == null)
            {
                if (valueFactory != null)
                {
                    var value = valueFactory();
                    if (expirySeconds != null)
                    {
                        var a = SetValue(value);
                        var b = SetExpire(expirySeconds.Value, queueJump);
                        await Task.WhenAll(a, b).ConfigureAwait(false);
                    }
                    return value;
                }
                return null;
            }

            var accessor = FastMember.TypeAccessor.Create(typeof(T), allowNonPublicAccessors: false);
            var result = (T)accessor.CreateNew();

            foreach (var member in accessor.GetMembers())
            {
                byte[] value;
                if (data.TryGetValue(member.Name, out value))
                {
                    accessor[result, member.Name] = valueConverter.Deserialize(member.Type, value);
                }
            }

            return result;
        }

        public virtual Task SetValue(T value, bool queueJump = false)
        {
            var accessor = FastMember.TypeAccessor.Create(typeof(T), allowNonPublicAccessors: false);
            var members = accessor.GetMembers();
            var values = new Dictionary<string, byte[]>(members.Count);
            foreach (var member in members)
            {
                values.Add(member.Name, valueConverter.Serialize(accessor[value, member.Name]));
            }

            return Command.Set(Db, Key, values, queueJump);
        }

        public virtual Task<bool> SetField(string field, object value, bool queueJump = false)
        {
            return Command.Set(Db, Key, field, valueConverter.Serialize(value), queueJump);
        }

        public virtual Task SetFields(Tuple<string, object>[] fields, bool queueJump = false)
        {
            var accessor = FastMember.TypeAccessor.Create(typeof(T), allowNonPublicAccessors: false);
            var values = new Dictionary<string, byte[]>(fields.Length);
            foreach (var field in fields)
            {
                values.Add(field.Item1, valueConverter.Serialize(accessor[field.Item2, field.Item1]));
            }

            return Command.Set(Db, Key, values, queueJump);
        }

        public virtual Task<long> Increment(string field, int value = 1, bool queueJump = false)
        {
            return Command.Increment(Db, Key, field, value, queueJump);
        }

        public virtual Task<double> Increment(string field, double value = 1, bool queueJump = false)
        {
            return Command.Increment(Db, Key, field, value, queueJump);
        }

        public virtual async Task<long[]> Increments(Tuple<string, int>[] fields, bool queueJump = false)
        {
            using (var tx = Connection.CreateTransaction())
            {
                var resultTask = new Task<long>[fields.Length];
                for (int i = 0; i < fields.Length; i++)
                {
                    var field = fields[i];
                    resultTask[i] = tx.Hashes.Increment(Db, Key, field.Item1, field.Item2, queueJump);
                }

                await tx.Execute(queueJump).ConfigureAwait(false);

                var result = new long[fields.Length];
                for (int i = 0; i < fields.Length; i++)
                {
                    result[i] = await resultTask[i].ConfigureAwait(false);
                }

                return result;
            }
        }

        public virtual async Task<double[]> Increments(Tuple<string, double>[] fields, bool queueJump = false)
        {
            using (var tx = Connection.CreateTransaction())
            {
                var resultTask = new Task<double>[fields.Length];
                for (int i = 0; i < fields.Length; i++)
                {
                    var field = fields[i];
                    resultTask[i] = tx.Hashes.Increment(Db, Key, field.Item1, field.Item2, queueJump);
                }

                await tx.Execute(queueJump).ConfigureAwait(false);

                var result = new double[fields.Length];
                for (int i = 0; i < fields.Length; i++)
                {
                    result[i] = await resultTask[i].ConfigureAwait(false);
                }

                return result;
            }
        }

        public virtual Task<bool> SetExpire(int seconds, bool queueJump = false)
        {
            return Connection.Keys.Expire(Db, Key, seconds, queueJump);
        }
    }

    /// <summary>
    /// Memory memoized class mapped RedisHash
    /// </summary>
    public class MemoizedRedisClass<T> : RedisClass<T>
        where T : class, new()
    {
        T cache = null;

        public MemoizedRedisClass(RedisSettings settings, string hashKey, Func<T> valueFactory, int? expirySeconds = null)
            : base(settings, hashKey, valueFactory, expirySeconds)
        {
        }

        public MemoizedRedisClass(RedisGroup connectionGroup, string hashKey, Func<T> valueFactory, int? expirySeconds = null)
            : base(connectionGroup, hashKey, valueFactory, expirySeconds)
        {
        }

        public override async Task<T> GetValue(bool queueJump = false)
        {
            if (cache != null) return cache;

            var value = await base.GetValue(queueJump).ConfigureAwait(false);
            cache = value;
            return value;
        }

        public override async Task SetValue(T value, bool queueJump = false)
        {
            await base.SetValue(value, queueJump).ConfigureAwait(false);
            if (cache != null)
            {
                cache = value;
            }
        }

        public override async Task<bool> SetField(string field, object value, bool queueJump = false)
        {
            var result = await base.SetField(field, value, queueJump).ConfigureAwait(false);
            if (cache != null)
            {
                FastMember.ObjectAccessor.Create(cache)[field] = value;
            }
            return result;
        }

        public override async Task SetFields(Tuple<string, object>[] fields, bool queueJump = false)
        {
            await base.SetFields(fields, queueJump).ConfigureAwait(false);
            if (cache != null)
            {
                var accessor = FastMember.TypeAccessor.Create(typeof(T), allowNonPublicAccessors: false);
                foreach (var field in fields)
                {
                    accessor[cache, field.Item1] = field.Item2;
                }
            }
        }

        public override async Task<long> Increment(string field, int value = 1, bool queueJump = false)
        {
            var v = await base.Increment(field, value, queueJump).ConfigureAwait(false);
            if (cache != null)
            {
                FastMember.ObjectAccessor.Create(cache)[field] = v;
            }
            return v;
        }

        public override async Task<double> Increment(string field, double value = 1, bool queueJump = false)
        {
            var v = await base.Increment(field, value, queueJump).ConfigureAwait(false);
            if (cache != null)
            {
                FastMember.ObjectAccessor.Create(cache)[field] = v;
            }
            return v;
        }

        public override async Task<long[]> Increments(Tuple<string, int>[] fields, bool queueJump = false)
        {
            var v = await base.Increments(fields, queueJump).ConfigureAwait(false);
            if (cache != null)
            {
                var accessor = FastMember.TypeAccessor.Create(typeof(T));
                for (int i = 0; i < fields.Length; i++)
                {
                    accessor[cache, fields[i].Item1] = v[i];
                }
            }
            return v;
        }

        public override async Task<double[]> Increments(Tuple<string, double>[] fields, bool queueJump = false)
        {
            var v = await base.Increments(fields, queueJump).ConfigureAwait(false);
            if (cache != null)
            {
                var accessor = FastMember.TypeAccessor.Create(typeof(T));
                for (int i = 0; i < fields.Length; i++)
                {
                    accessor[cache, fields[i].Item1] = v[i];
                }
            }
            return v;
        }
    }
}