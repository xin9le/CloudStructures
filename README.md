CloudStructures
===============
Redis Client based on BookSleeve. Features: connection management, serialize/deserialize to object, key distribute and configuration. The concept is distributed collection inspired by Cloud Collection(System.Cloud.Collections.IAsyncList[T], etc...) of [ActorFx](http://actorfx.codeplex.com/).

Install
---
using with NuGet(Including PreRelease), [CloudStructures](https://nuget.org/packages/CloudStructures/)
```
PM> Install-Package CloudStructures
```

Example
---
```csharp
// Server of Redis
public static class RedisServer
{
    public static readonly RedisSettings Default = new RedisSettings("127.0.0.1");
}

// a class
public class Person
{
    public string Name { get; set; }
    public int Age { get; set; }
}


// Redis String
var redis = new RedisString<Person>(RedisServer.Default, "test-string-key");
await redis.Set(new Person { Name = "John", Age = 34 });

var copy = await redis.GetValueOrDefault();

// Redis list
var redis = new RedisList<Person>(RedisServer.Default, "test-list-key");
await redis.AddLast(new Person { Name = "Tom" });
await redis.AddLast(new Person { Name = "Mary" });

var persons = await redis.Range(0, 10);

// and others - Set, SortedSet, Hash, Dictionary(Generic Hash), Class(Object-Hash-Mapping)
```

ConnectionManagement
---
```csharp
// Represents of Redis settings
var settings = new RedisSettings(host: "127.0.0.1", port: 6379, db: 0);

// BookSleeve's threadsafe connection
// keep single connection and re-connect when disconnected
var conn = settings.GetConnection();

// multi group of connections
var group = new RedisGroup(groupName: "Cache", settings: new[]
{
    new RedisSettings(host: "100.0.0.1", port: 6379, db: 0),
    new RedisSettings(host: "105.0.0.1", port: 6379, db: 0),
});

// key hashing
var conn = group.GetSettings("hogehoge-100").GetConnection();

// customize serializer(default as JSON, and option includes protocol-buffers)
new RedisSettings("127.0.0.1", converter: new JsonRedisValueConverter());
new RedisSettings("127.0.0.1", converter: new ProtoBufRedisValueConverter());
```

PubSub -> Observable
---
Experimental feature, CloudStructures with Reactive Extensions. RedisSubject is ISubject = IObservable and IObserver. Observer publish message to Redis PubSub Channnel. Observable subscribe to Redis PubSub Channel.

using with NuGet(Including PreRelease), [CloudStructures-Rx](https://nuget.org/packages/CloudStructures-Rx/)
```
PM> Install-Package CloudStructures -Pre
```

```csharp
// RedisSubject as ISubject<T>
var subject = new RedisSubject<string>(RedisServer.Default, "PubSubTest");

// subject as IObservable<T> and Subscribe to Redis PubSub Channel
var a = subject
    .Select(x => DateTime.Now.Ticks + " " + x)
    .Subscribe(x => Console.WriteLine(x));

var b = subject
    .Where(x => !x.StartsWith("A"))
    .Subscribe(x => Console.WriteLine(x), () => Console.WriteLine("completed!"));

// subject as IObserver and OnNext/OnError/OnCompleted publish to Redis PubSub Channel
subject.OnNext("ABCDEFGHIJKLM");
subject.OnNext("hello");
subject.OnNext("world");

Thread.Sleep(200);

a.Dispose(); // Unsubscribe is Dispose
subject.OnCompleted(); // if receive OnError/OnCompleted then subscriber is unsubscribed
```

Configuration
---
load configuration from web.config or app.config

```xml
<configSections>
    <section name="cloudStructures" type="CloudStructures.Redis.CloudStructuresConfigurationSection, CloudStructures" />
</configSections>

<cloudStructures>
    <redis>
        <group name="cache">
            <add host="127.0.0.1" />
            <add host="127.0.0.2" port="1000" />
        </group>
        <group name="session">
            <add host="127.0.0.1" db="2" valueConverter="CloudStructures.Redis.ProtoBufRedisValueConverter, CloudStructures" />
        </group>
    </redis>
</cloudStructures>
```

```csharp
// load configuration from .config
var groups = CloudStructuresConfigurationSection.GetSection().ToRedisGroups();
```

group attributes are "host, port, ioTimeout, password, maxUnsent, allowAdmin, syncTimeout, db, valueConverter, commandTracer".  
It is same as RedisSettings except commandTracer.