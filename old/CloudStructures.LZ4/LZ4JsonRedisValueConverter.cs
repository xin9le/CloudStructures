using System;
using System.IO;
using System.Text;
using LZ4Stream = LZ4.LZ4Stream;

namespace CloudStructures
{
    public class LZ4JsonRedisValueConverter : ObjectRedisValueConverterBase
    {
        readonly int blockSize;
        readonly bool highCompression;

        // default LZ4Stream buffer size is 1MB but it's too large on Redis storing usage.

        /// <summary>
        /// Create LZ4JsonRedisValueConverter by BufferSize = 64kb, StandardCompression Mode.
        /// </summary>
        public LZ4JsonRedisValueConverter()
            : this(1024 * 64, false)
        {

        }

        /// <summary>
        /// Create LZ4JsonRedisValueConverter by StandardCompression Mode.
        /// </summary>
        public LZ4JsonRedisValueConverter(int blockSize)
            : this(blockSize, false)
        {

        }

        /// <summary>
        /// Create LZ4JsonRedisValueConverter by BufferSize = 64kb.
        /// </summary>
        public LZ4JsonRedisValueConverter(bool highCompression)
            : this(1024 * 64, highCompression)
        {

        }

        /// <summary>
        /// Create LZ4JsonRedisValueConverter.
        /// </summary>
        public LZ4JsonRedisValueConverter(int blockSize, bool highCompression)
        {
            this.blockSize = blockSize;
            this.highCompression = highCompression;
        }


        protected override object DeserializeCore(Type type, byte[] value)
        {
            using (var ms = new MemoryStream(value))
            using (var lz4 = new LZ4Stream(ms, System.IO.Compression.CompressionMode.Decompress, blockSize: blockSize))
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
                using (var lz4 = new LZ4Stream(ms, System.IO.Compression.CompressionMode.Compress, highCompression, blockSize))
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