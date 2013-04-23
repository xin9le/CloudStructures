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
                            restMemory.CopyTo(stream);
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

        public RedisSubject(RedisSettings settings, string key)
        {
            this.settings = settings;
            this.Db = settings.Db;
            this.valueConverter = settings.ValueConverter;
            this.Key = key;
        }

        public RedisSubject(RedisGroup connectionGroup, string key)
            : this(connectionGroup.GetSettings(key), key)
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
            using (var ms = new MemoryStream())
            {
                RemotableNotification<T>.OnCompleted().WriteTo(ms, valueConverter);
                Connection.Publish(Key, ms.ToArray(), queueJump);
            }
        }

        public IDisposable Subscribe(IObserver<T> observer)
        {
            var channnel = Connection.GetOpenSubscriberChannel();

            // Error += OnError? reconnect?
            var disposable = System.Reactive.Disposables.Disposable.Create(() =>
            {
                channnel.Unsubscribe(Key);
            });

            channnel.Subscribe(Key, (_, xs) =>
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

            return disposable;
        }
    }
}