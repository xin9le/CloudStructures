CloudStructures
===============
Redis Client based on BookSleeve. Features: connection management, serialize/deserialize to object, key distribute and configuration. And includes Redis Profiler for Glimpse. The concept is distributed collection inspired by Cloud Collection(System.Cloud.Collections.IAsyncList[T], etc...) of [ActorFx](http://actorfx.codeplex.com/).

Install
---
using with NuGet, [CloudStructures](https://nuget.org/packages/CloudStructures/)
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
PM> Install-Package CloudStructures-Rx -Pre
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

Glimpse.CloudStructures.Redis
---
CloudStructures has Redis Profiler for [Glimpse](http://getglimpse.com/). - [Glimpse.CloudStructures.Redis](https://nuget.org/packages/Glimpse.CloudStructures.Redis/)

```
PM> Install-Package Glimpse.CloudStructures.Redis
```

Setup Glimpse and add config with RedisProfiler for example

```xml
<cloudStructures>
<redis>
    <group name="Demo">
    <add host="127.0.0.1" db="0" commandTracer="Glimpse.CloudStructures.Redis.RedisProfiler, Glimpse.CloudStructures.Redis" />
    </group>
</redis>
</cloudStructures>
```

You can see Redis Tab on Glimpse.

![](http://i.imgur.com/QZ7hZu6.jpg)

Command, Key, Sent/Received Object as JSON, Duration. If duplicate command and key then show warn(Icon and Orange Text).

And Timeline, can visualise parallel access.

![](http://i.imgur.com/yqzAIzk.jpg)

Sample is avaliable on this Repositry, [CloudStructures.Demo.Mvc](https://github.com/neuecc/CloudStructures/tree/master/CloudStructures.Demo.Mvc).

Who is using this?
---
CloudStructures is in production use at [Grani](http://grani.jp/)  
Grani is top social game developer at Japan(and I'm CTO at Grani).  
The game use redis massively heavy, hundreds of thousands of message per second.

History
---
2013-11-15 ver 0.6.0
* add, Glimpse.CloudStructures.Redis
* improved, connection waitOpen use syncTimeout.
* improved, can monitor RedisConnection event(Open/Close/Error/Shutdown) on RedisSettings.
* fix bugs, List.AddFirstAndFixLength is always trimed right length -1.
* breaking changes, ICommandTracer receive many extra key(settings, sentObject, receivedObject).

License
---
under [MIT License](http://opensource.org/licenses/MIT)
