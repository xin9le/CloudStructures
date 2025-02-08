# CloudStructures
CloudStructures is the [Redis](https://redis.io/) client based on [StackExchange.Redis](https://github.com/StackExchange/StackExchange.Redis).

StackExchange.Redis is very pure and low level library. It's Redis driver like ADO.NET. It's difficult to use it as raw. CloudStructures provides simple O/R (Object / Redis) mapper like [Dapper](https://github.com/StackExchange/Dapper) for ADO.NET.


[![Releases](https://img.shields.io/github/release/neuecc/CloudStructures.svg)](https://github.com/neuecc/CloudStructures/releases)



# Support framework
- .NET 8+
- .NET Standard 2.0+
- .NET Framework 4.6.2+



# Installation
```
dotnet add package CloudStructures
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
If you use this library, you *should* implement `IValueConverter` to serialize your original class. Unless you pass custom `IValueConverter` to `RedisConnection` ctor, fallback to `SystemTextJsonConverter` automatically that is default converter we provide.


## How to implement custom `IValueConverter`

```cs
using CloudStructures.Converters;
using Utf8Json;
using Utf8Json.Resolvers;

namespace HowToImplement_CustomValueConverter
{
    public sealed class Utf8JsonConverter : IValueConverter
    {
        public byte[] Serialize<T>(T value)
            => JsonSerializer.Serialize(value, StandardResolver.AllowPrivate);

        public T Deserialize<T>(byte[] value)
            => JsonSerializer.Deserialize<T>(value, StandardResolver.AllowPrivate);
    }
}
```

```cs
using CloudStructures.Converters;
using MessagePack;
using MessagePack.Resolvers;

namespace HowToImplement_CustomValueConverter
{
    public sealed class MessagePackConverter : IValueConverter
    {
        private MessagePackSerializerOptions Options { get; }

        public MessagePackConverter(MessagePackSerializerOptions options)
            => this.Options = options;

        public byte[] Serialize<T>(T value)
            => MessagePackSerializer.Serialize(value, this.Options);

        public T Deserialize<T>(byte[] value)
            => MessagePackSerializer.Deserialize<T>(value, this.Options);
    }
}
```



# Authors
- Yoshifumi Kawai (a.k.a [@neuecc](https://twitter.com/neuecc))
- Takaaki Suzuki (a.k.a [@xin9le](https://twitter.com/xin9le))

Yoshifumi Kawai is software developer in Tokyo, Japan. Awarded Microsoft MVP (C#) since April, 2011. He's the original owner of this project.

Takaaki Suzuki is software developer in Fukui, Japan. Awarded Microsoft MVP (C#) since July, 2012. He's a contributer who led the .NET Standard support.



# License
This library is under the MIT License.
