using System;
using System.IO;
using System.Reactive;
using System.Reactive.Subjects;
using System.Text;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace CloudStructures.Redis
{
    public class RemoteNotificationException : Exception
    {
        public RemoteNotificationException(string errorMessage)
            : base(errorMessage)
        {

        }
    }

    public enum PubSubKeyType
    {
        Normal, Pattern
    }

    internal class RemotableNotification<T>
    {
        public NotificationKind Kind { get; private set; }
        public T Value { get; private set; }
        public string ErrorMessage { get; private set; }

        private RemotableNotification()
        {

        }

        public static RemotableNotification<T> OnNext(T value)
        {
            return new RemotableNotification<T>()
            {
                Kind = NotificationKind.OnNext,
                Value = value
            };
        }

        public static RemotableNotification<T> OnError(string errorMessage)
        {
            return new RemotableNotification<T>()
            {
                Kind = NotificationKind.OnError,
                ErrorMessage = errorMessage
            };
        }

        public static RemotableNotification<T> OnCompleted()
        {
            return new RemotableNotification<T>()
            {
                Kind = NotificationKind.OnCompleted
            };
        }

        public void Accept(IObserver<T> observer)
        {
            switch (Kind)
            {
                case NotificationKind.OnCompleted:
                    observer.OnCompleted();
                    break;
                case NotificationKind.OnError:
                    observer.OnError(new RemoteNotificationException(ErrorMessage));
                    break;
                case NotificationKind.OnNext:
                    observer.OnNext(Value);
                    break;
                default:
                    throw new InvalidOperationException("Invalid Kind");
            }
        }

        public static RemotableNotification<T> ReadFrom(Stream stream, IRedisValueConverter converter)
        {
            using (var br = new BinaryReader(stream, Encoding.UTF8, leaveOpen: false))
            {
                var value = new RemotableNotification<T>();
                value.Kind = (NotificationKind)br.ReadInt32();
                switch (value.Kind)
                {
                    case NotificationKind.OnCompleted:
                        break;
                    case NotificationKind.OnError:
                        value.ErrorMessage = br.ReadString();
                        break;
                    case NotificationKind.OnNext:
                        using (var restMemory = new MemoryStream())
                        {
                            stream.CopyTo(restMemory);
                            long dummy;
                            value.Value = converter.Deserialize<T>(restMemory.ToArray(), out dummy);
                        }
                        break;
                    default:
                        throw new InvalidOperationException("Invalid Kind");
                }

                return value;
            }
        }

        public void WriteTo(Stream stream, IRedisValueConverter converter)
        {
            using (var bw = new BinaryWriter(stream, Encoding.UTF8, leaveOpen: false))
            {
                bw.Write((int)Kind);
                switch (Kind)
                {
                    case NotificationKind.OnCompleted:
                        break;
                    case NotificationKind.OnError:
                        bw.Write(ErrorMessage);
                        break;
                    case NotificationKind.OnNext:
                        long dummy;
                        bw.Write((byte[])converter.Serialize(Value, out dummy));
                        break;
                    default:
                        throw new InvalidOperationException("Invalid Kind");
                }
            }
        }
    }

    public class RedisSubject<T> : ISubject<T>
    {
        public string Key { get; private set; }
        public int Db { get; private set; }
        readonly RedisSettings settings;
        readonly IRedisValueConverter valueConverter;
        readonly PubSubKeyType keyType;

        public RedisSubject(RedisSettings settings, string key, PubSubKeyType keyType = PubSubKeyType.Normal)
        {
            this.settings = settings;
            this.Db = settings.Db;
            this.valueConverter = settings.ValueConverter;
            this.Key = key;
            this.keyType = keyType;
        }

        public RedisSubject(RedisGroup connectionGroup, string key, PubSubKeyType keyType = PubSubKeyType.Normal)
            : this(connectionGroup.GetSettings(key), key, keyType)
        {
        }

        protected ConnectionMultiplexer Connection
        {
            get
            {
                return settings.GetConnection();
            }
        }

        public void OnNext(T value)
        {
            OnNext(value, CommandFlags.None).Wait();
        }

        public Task<long> OnNext(T value, CommandFlags commandFlags)
        {
            if (keyType != PubSubKeyType.Normal) throw new InvalidOperationException("OnNext is supported only PubSubKeyType.Normal");

            using (var ms = new MemoryStream())
            {
                RemotableNotification<T>.OnNext(value).WriteTo(ms, valueConverter);
                return Task.FromResult(Connection.GetSubscriber().Publish(Key, ms.ToArray(), commandFlags));
            }
        }

        public void OnError(Exception error)
        {
            OnError(error.ToString()).Wait();
        }

        public Task<long> OnError(string errorMessage, CommandFlags commandFlags = CommandFlags.None)
        {
            if (keyType != PubSubKeyType.Normal) throw new InvalidOperationException("OnError is supported only PubSubKeyType.Normal");

            using (var ms = new MemoryStream())
            {
                RemotableNotification<T>.OnError(errorMessage).WriteTo(ms, valueConverter);
                return Task.FromResult(Connection.GetSubscriber().Publish(Key, ms.ToArray(), commandFlags));
            }
        }

        public void OnCompleted()
        {
            OnCompleted(CommandFlags.None).Wait();
        }

        public Task<long> OnCompleted(CommandFlags commandFlags)
        {
            if (keyType != PubSubKeyType.Normal) throw new InvalidOperationException("OnCompleted is supported only PubSubKeyType.Normal");

            using (var ms = new MemoryStream())
            {
                RemotableNotification<T>.OnCompleted().WriteTo(ms, valueConverter);
                return Task.FromResult(Connection.GetSubscriber().Publish(Key, ms.ToArray(), commandFlags));
            }
        }

        public IDisposable Subscribe(IObserver<T> observer)
        {
            var disposable = System.Reactive.Disposables.Disposable.Create(() =>
            {
                if (keyType == PubSubKeyType.Normal)
                {
                    Connection.GetSubscriber().Unsubscribe(Key);
                }
                else
                {
                    Connection.GetSubscriber().Unsubscribe(new RedisChannel(Key, RedisChannel.PatternMode.Pattern));
                }
            });

            var subscribeAction = (Action<RedisChannel, RedisValue>)((_, xs) =>
            {
                using (var ms = new MemoryStream(xs))
                {
                    var value = RemotableNotification<T>.ReadFrom(ms, valueConverter);
                    value.Accept(observer);
                    if (value.Kind == NotificationKind.OnError || value.Kind == NotificationKind.OnCompleted)
                    {
                        disposable.Dispose();
                    }
                }
            });

            // when error, shutdown, close, reconnect? handling?
            if (keyType == PubSubKeyType.Normal)
            {
                Connection.GetSubscriber().Subscribe(Key, subscribeAction);
            }
            else
            {
                Connection.GetSubscriber().Subscribe(new RedisChannel(Key, RedisChannel.PatternMode.Pattern), subscribeAction);
            }

            return disposable;
        }
    }
}