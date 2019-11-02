# What's CloudStructures
CloudStructures is the [Redis](https://redis.io/) client based on [StackExchange.Redis](https://github.com/StackExchange/StackExchange.Redis). **Now supports .NET Standard!!**

StackExchange.Redis is very pure and low level library. It's Redis driver like ADO.NET. It's very difficult to use it as raw. CloudStructures provides simple O/R (Object / Redis) mapper like [Dapper](https://github.com/StackExchange/Dapper) for ADO.NET.


[![Releases](https://img.shields.io/github/release/neuecc/CloudStructures.svg)](https://github.com/neuecc/CloudStructures/releases)



# Support framework
- .NET Standard 2.0



# Installation
```
PM> Install-Package CloudStructures
```



# Data structures of Redis
CloudStructures supports these Redis data types. All methods are async.

| Structure | Description |
| --- | --- |
| `RedisBit` | Bits API |
| `RedisDictionary<TKey, TValue>` | Hashes API with constrained value type |
| `RedisGeo<T>` | Geometries API |
| `RedisHashSet<T>` | like `RedisDictionary<T, bool>` |
| `RedisHyperLogLog<T>` | HyperLogLogs API |
| `RedisList<T>` | Lists API |
| `RedisLua` | Lua eval API |
| `RedisSet<T>` | Sets API |
| `RedisSortedSet<T>` | SortedSets API |
| `RedisString<T>` | Strings API |



# Getting started
Following code is simple sample.

```cs
// RedisConnection have to be held as static.
public static class RedisServer
{
    public static RedisConnection Connection { get; }
    public static RedisServer()
    {
        var config = new RedisConfig("name", "connectionString");
        Connection = new RedisConnection(config);
    }
}

// A certain data class
public class Person
{
    public string Name { get; set; }
    public int Age { get; set; }
}

// 1. Create redis structure
var key = "test-key";
var defaultExpiry = TimeSpan.FromDays(1);
var redis = new RedisString<Person>(RedisServer.Connection, key, defaultExpiry)

// 2. Call command
var neuecc = new Person("neuecc", 35);
await redis.SetAsync(neuecc);
var result = await redis.GetAsync();
```



# ValueConverter
If you use this library, you must implement `IValueConverter` to serialize your original class. However, we provides default implementations using [MessagePack for C#](https://github.com/neuecc/MessagePack-CSharp) and [Utf8Json](https://github.com/neuecc/Utf8Json). Unless you pass custom `IValueConverter` to `RedisConnection` ctor, fallback to `Utf8JsonConverter` automatically. If you wanna use MessagePack version, you should install following package.

```
PM> Install-Package CloudStructures.Converters.MessagePack
```



# Authors
- Yoshifumi Kawai (a.k.a [@neuecc](https://twitter.com/neuecc))
- Takaaki Suzuki (a.k.a [@xin9le](https://twitter.com/xin9le))

Yoshifumi Kawai is software developer in Tokyo, Japan. Awarded Microsoft MVP (C#) since April, 2011. He's the original owner of this project.

Takaaki Suzuki is software developer in Fukui, Japan. Awarded Microsoft MVP (C#) since July, 2012. He's a contributer who led the .NET Standard support.



# License
This library is under the MIT License.
