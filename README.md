CloudStructures
===============
Distributed Collections based on Redis and BookSleeve. This is inspired by Cloud Collection(System.Cloud.Collections.IAsyncList[T], etc...) of [ActorFx](http://actorfx.codeplex.com/).

Install
---
using with NuGet(Including PreRelease), [CloudStructures](https://nuget.org/packages/CloudStructures/)
```
PM> Install-Package CloudStructures -Pre
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
    public string Name { get; private set; }
    public RedisList<Person> Friends { get; private set; }

    public Person(string name)
    {
        Name = name;
        Friends = new RedisList<Person>(RedisServer.Default, "Person-" + Name);
    }
}

// local ? cloud ? object
var sato = new Person("Mike");

// add person
await sato.Friends.AddLast(new Person("John"));
await sato.Friends.AddLast(new Person("Mary"));

// count
var friendCount = await sato.Friends.GetLength();
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

// customize serializer(default as JSON)
new RedisSettings("127.0.0.1", converter: new JsonRedisValueConverter());
new RedisSettings("127.0.0.1", converter: new ProtoBufRedisValueConverter());
```