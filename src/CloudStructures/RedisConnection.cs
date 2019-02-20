using System;
using System.IO;
using System.Linq;
using CloudStructures.Converters;
using StackExchange.Redis;



namespace CloudStructures
{
    /// <summary>
    /// Redis サーバーへの接続を提供します。
    /// </summary>
    /// <remarks>接続は破棄することなくアプリケーション上で使い回す必要があります。</remarks>
    public sealed class RedisConnection
    {
        #region プロパティ
        /// <summary>
        /// 構成を取得します。
        /// </summary>
        public RedisConfig Config { get; }


        /// <summary>
        /// 値変換機能を取得します。
        /// </summary>
        internal ValueConverter Converter { get; }


        /// <summary>
        /// データベースを取得します。
        /// </summary>
        internal IDatabaseAsync Database
            => this.Config.Database.HasValue
            ? this.InnerConnection.Value.GetDatabase(this.Config.Database.Value)
            : this.InnerConnection.Value.GetDatabase();


        /// <summary>
        /// トランザクションを取得します。
        /// </summary>
        internal ITransaction Transaction
            => ((IDatabase)this.Database).CreateTransaction();


        /// <summary>
        /// 接続先のサーバーを取得します。
        /// </summary>
        internal IServer[] Servers
            => this.Config.Options
            .EndPoints
            .Select(x => this.InnerConnection.Value.GetServer(x))
            .ToArray();


        /// <summary>
        /// 内部で保持するコネクションを取得します。
        /// </summary>
        private Lazy<ConnectionMultiplexer> InnerConnection { get; }
        #endregion


        #region コンストラクタ
        /// <summary>
        /// インスタンスを生成します。
        /// </summary>
        /// <param name="config">構成</param>
        /// <param name="converter">値変換機能</param>
        /// <param name="handler">イベントハンドラ</param>
        /// <param name="logger">ログ書き込み機能</param>
        public RedisConnection(RedisConfig config, IValueConverter converter, IConnectionEventHandler handler = null, TextWriter logger = null)
        {
            if (converter == null)
                throw new ArgumentNullException(nameof(converter));

            this.Config = config ?? throw new ArgumentNullException(nameof(config));
            this.Converter = new ValueConverter(converter);
            this.InnerConnection = new Lazy<ConnectionMultiplexer>(() =>
            {
                var connection = ConnectionMultiplexer.Connect(this.Config.Options, logger);

                //--- attach events
                connection.ConfigurationChanged += (_, e) => handler?.OnConfigurationChanged(this, e);
                connection.ConfigurationChangedBroadcast += (_, e) => handler?.OnConfigurationChangedBroadcast(this, e);
                connection.ConnectionFailed += (_, e) => handler?.OnConnectionFailed(this, e);
                connection.ConnectionRestored += (_, e) => handler?.OnConnectionRestored(this, e);
                connection.ErrorMessage += (_, e) => handler?.OnErrorMessage(this, e);
                connection.HashSlotMoved += (_, e) => handler?.OnHashSlotMoved(this, e);
                connection.InternalError += (_, e) => handler?.OnInternalError(this, e);

                return connection;
            });
        }
        #endregion
    }
}
