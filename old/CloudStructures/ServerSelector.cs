using StackExchange.Redis;
using System;

namespace CloudStructures
{
    public interface IServerSelector
    {
        RedisSettings Select(RedisSettings[] settings, RedisKey key);
    }

    public class SimpleHashingSelector : IServerSelector
    {
        public RedisSettings Select(RedisSettings[] settings, RedisKey key)
        {
            if (settings.Length == 0) throw new ArgumentException("settings length is 0");
            if (settings.Length == 1) return settings[0];

            using (var md5 = new System.Security.Cryptography.MD5CryptoServiceProvider())
            {
                var hashBytes = md5.ComputeHash((byte[])key);
                var preSeed = BitConverter.ToInt32(hashBytes, 0);
                if (preSeed == int.MinValue) preSeed++; // int.MinValue can't do Abs

                var seed = System.Math.Abs(preSeed);
                var index = seed % settings.Length;
                return settings[index];
            }
        }
    }
}
