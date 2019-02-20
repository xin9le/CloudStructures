using System;
using StackExchange.Redis;



namespace CloudStructures
{
    /// <summary>
    /// 構成を表します。
    /// </summary>
    public class RedisConfig
    {
        #region プロパティ
        /// <summary>
        /// 名称を取得します。
        /// </summary>
        public string Name { get; }


        /// <summary>
        /// 構成オプションを取得します。
        /// </summary>
        /// <remarks>
        /// 書き方のヒントはこちら
        /// https://stackexchange.github.io/StackExchange.Redis/Configuration.html
        /// </remarks>
        public ConfigurationOptions Options { get; }


        /// <summary>
        /// 論理的データベースのインデックスを取得します。
        /// </summary>
        public int? Database { get; }
        #endregion


        #region コンストラクタ
        /// <summary>
        /// インスタンスを生成します。
        /// </summary>
        /// <param name="name">名称</param>
        /// <param name="connectionString">接続文字列</param>
        /// <param name="database">論理データベース</param>
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
