using System;
using System.IO;
using System.Text;
using LZ4Stream = LZ4.LZ4Stream;

namespace CloudStructures
{
    public class LZ4JsonRedisValueConverter : ObjectRedisValueConverterBase
    {
        protected override object DeserializeCore(Type type, byte[] value)
        {
            using (var ms = new MemoryStream(value))
            using (var lz4 = new LZ4Stream(ms, System.IO.Compression.CompressionMode.Decompress))
            using (var sr = new StreamReader(lz4, Encoding.UTF8))
            {
                var result = Jil.JSON.Deserialize(sr, type);
                return result;
            }
        }

        protected override byte[] SerializeCore(object value, out long resultSize)
        {
            using (var ms = new MemoryStream())
            {
                using (var lz4 = new LZ4Stream(ms, System.IO.Compression.CompressionMode.Compress))
                using (var sw = new StreamWriter(lz4))
                {
                    Jil.JSON.Serialize(value, sw);
                }
                var result = ms.ToArray();
                resultSize = result.Length;
                return result;
            }
        }
    }
}