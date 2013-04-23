using BookSleeve;
using System;
using System.IO;
using System.Reactive;
using System.Reactive.Subjects;
using System.Text;

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
                            value.Value = converter.Deserialize<T>(restMemory.ToArray());
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
                        bw.Write(converter.Serialize(Value));
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

        protected RedisConnection Connection
        {
            get
            {
                return settings.GetConnection();
            }
        }

        public void OnNext(T value)
        {
            OnNext(value, queueJump: false);
        }

        public void OnNext(T value, bool queueJump)
        {
            if (keyType != PubSubKeyType.Normal) throw new InvalidOperationException("OnNext is supported only PubSubKeyType.Normal");

            using (var ms = new MemoryStream())
            {
                RemotableNotification<T>.OnNext(value).WriteTo(ms, valueConverter);
                Connection.Publish(Key, ms.ToArray(), queueJump);
            }
        }

        public void OnError(Exception error)
        {
            OnError(error.ToString());
        }

        public void OnError(string errorMessage, bool queueJump = false)
        {
            if (keyType != PubSubKeyType.Normal) throw new InvalidOperationException("OnError is supported only PubSubKeyType.Normal");

            using (var ms = new MemoryStream())
            {
                RemotableNotification<T>.OnError(errorMessage).WriteTo(ms, valueConverter);
                Connection.Publish(Key, ms.ToArray(), queueJump);
            }
        }

        public void OnCompleted()
        {
            OnCompleted(queueJump: false);
        }

        public void OnCompleted(bool queueJump)
        {
            if (keyType != PubSubKeyType.Normal) throw new InvalidOperationException("OnCompleted is supported only PubSubKeyType.Normal");

            using (var ms = new MemoryStream())
            {
                RemotableNotification<T>.OnCompleted().WriteTo(ms, valueConverter);
                Connection.Publish(Key, ms.ToArray(), queueJump);
            }
        }

        public IDisposable Subscribe(IObserver<T> observer)
        {
            var disposable = System.Reactive.Disposables.Disposable.Create(() =>
            {
                if (keyType == PubSubKeyType.Normal)
                {
                    Connection.GetOpenSubscriberChannel().Unsubscribe(Key).Wait();
                }
                else
                {
                    Connection.GetOpenSubscriberChannel().PatternUnsubscribe(Key).Wait();
                }
            });

            var subsribeAction = (Action<string, byte[]>)((_, xs) =>
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
                Connection.GetOpenSubscriberChannel().Subscribe(Key, subsribeAction).Wait();
            }
            else
            {
                Connection.GetOpenSubscriberChannel().PatternSubscribe(Key, subsribeAction).Wait();
            }

            return disposable;
        }
    }
}