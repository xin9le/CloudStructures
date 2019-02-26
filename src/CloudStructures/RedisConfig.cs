using System;
using StackExchange.Redis;



namespace CloudStructures
{
    /// <summary>
    /// Represents connection configuration.
    /// </summary>
    public class RedisConfig
    {
        #region Properties
        /// <summary>
        /// Gets name.
        /// </summary>
        public string Name { get; }


        /// <summary>
        /// Gets configuration options.
        /// </summary>
        /// <remarks>
        /// How to write configuration:
        /// https://stackexchange.github.io/StackExchange.Redis/Configuration.html
        /// </remarks>
        public ConfigurationOptions Options { get; }


        /// <summary>
        /// Gets logical database index.
        /// </summary>
        public int? Database { get; }
        #endregion


        #region Constructors
        /// <summary>
        /// Creates instance.
        /// </summary>
        /// <param name="name"></param>
        /// <param name="connectionString"></param>
        /// <param name="database"></param>
        public RedisConfig(string name, string connectionString, int? database = null)
        {
            if (connectionString == null)
                throw new ArgumentNullException(nameof(connectionString));

            this.Name = name ?? throw new ArgumentNullException(nameof(name));
            this.Options = ConfigurationOptions.Parse(connectionString);
            this.Database = database;
        }
        #endregion
    }
}
