using System;
using CloudStructures.Converters;
using StackExchange.Redis;



namespace CloudStructures
{
    /// <summary>
    /// Generics 版の <see cref="RedisResult"/> を表します。
    /// </summary>
    /// <typeparam name="T">データ型</typeparam>
    public readonly struct RedisResult<T>
    {
        #region プロパティ
        /// <summary>
        /// 既定値を取得します。
        /// </summary>
        public static RedisResult<T> Default { get; } = default;


        /// <summary>
        /// 値があるかどうかを取得します。
        /// </summary>
        public bool HasValue { get; }


        /// <summary>
        /// 値を取得します。
        /// </summary>
        public T Value
            => this.HasValue
            ?  this.value
            :  throw new InvalidOperationException("has no value.");
        private readonly T value;
        #endregion


        #region コンストラクタ
        /// <summary>
        /// インスタンスを生成します。
        /// </summary>
        /// <param name="value">値</param>
        internal RedisResult(T value)
        {
            this.HasValue = true;
            this.value = value;
        }
        #endregion


        #region override
        /// <summary>
        /// 文字列に変換します。
        /// </summary>
        /// <returns></returns>
        public override string ToString()
            => this.HasValue ? this.Value.ToString() : null;
        #endregion


        #region 取得
        /// <summary>
        /// 値を取得します。値が存在しない場合は null を返します。
        /// </summary>
        /// <returns></returns>
        public object GetValueOrNull()
            => this.HasValue ? (object)this.Value : null;


        /// <summary>
        /// 値を取得します。値が存在しない場合は既定値を返します。
        /// </summary>
        /// <param name="default">既定値</param>
        /// <returns></returns>
        public T GetValueOrDefault(T @default = default)
            => this.HasValue ? this.Value : @default;


        /// <summary>
        /// 値を取得します。値が存在しない場合はデリゲートから取得された値を返します。
        /// </summary>
        /// <param name="valueFactory">既定値の生成処理</param>
        /// <returns></returns>
        public T GetValueOrDefault(Func<T> valueFactory)
        {
            if (valueFactory == null)
                throw new ArgumentNullException(nameof(valueFactory));
            return this.HasValue ? this.Value : valueFactory();
        }
        #endregion
    }



    /// <summary>
    /// Generics 版の <see cref="RedisResult"/> に有効期限情報を付与したものを表します。
    /// </summary>
    /// <typeparam name="T">データ型</typeparam>
    public readonly struct RedisResultWithExpiry<T>
    {
        #region プロパティ
        /// <summary>
        /// 既定値を取得します。
        /// </summary>
        public static RedisResultWithExpiry<T> Default { get; } = default;


        /// <summary>
        /// 値があるかどうかを取得します。
        /// </summary>
        public bool HasValue { get; }


        /// <summary>
        /// 値を取得します。
        /// </summary>
        public T Value
            => this.HasValue
            ?  this.value
            :  throw new InvalidOperationException("has no value.");
        private readonly T value;


        /// <summary>
        /// 有効期限を取得します。
        /// </summary>
        public TimeSpan? Expiry { get; }
        #endregion


        #region コンストラクタ
        /// <summary>
        /// インスタンスを生成します。
        /// </summary>
        /// <param name="value">値</param>
        /// <param name="expiry">有効期限</param>
        internal RedisResultWithExpiry(T value, TimeSpan? expiry)
        {
            this.HasValue = true;
            this.value = value;
            this.Expiry = expiry;
        }
        #endregion


        #region override
        /// <summary>
        /// 文字列に変換します。
        /// </summary>
        /// <returns></returns>
        public override string ToString()
            => this.HasValue ? this.Value.ToString() : null;
        #endregion


        #region 取得
        /// <summary>
        /// 値を取得します。値が存在しない場合は null を返します。
        /// </summary>
        /// <returns></returns>
        public object GetValueOrNull()
            => this.HasValue ? (object)this.Value : null;


        /// <summary>
        /// 値を取得します。値が存在しない場合は既定値を返します。
        /// </summary>
        /// <param name="default">既定値</param>
        /// <returns></returns>
        public T GetValueOrDefault(T @default = default)
            => this.HasValue ? this.Value : @default;


        /// <summary>
        /// 値を取得します。値が存在しない場合はデリゲートから取得された値を返します。
        /// </summary>
        /// <param name="valueFactory">既定値の生成処理</param>
        /// <returns></returns>
        public T GetValueOrDefault(Func<T> valueFactory)
        {
            if (valueFactory == null)
                throw new ArgumentNullException(nameof(valueFactory));
            return this.HasValue ? this.Value : valueFactory();
        }
        #endregion
    }



    /// <summary>
    /// <see cref="RedisResult{T}"/> および <seealso cref="RedisResultWithExpiry{T}"/> 関連の拡張機能を提供します。
    /// </summary>
    internal static class RedisResultExtensions
    {
        /// <summary>
        /// 指定された値を <see cref="RedisResult{T}"/> に変換します。
        /// </summary>
        /// <typeparam name="T">データ型</typeparam>
        /// <param name="value">値</param>
        /// <param name="converter">値変換機能</param>
        /// <returns></returns>
        public static RedisResult<T> ToResult<T>(this in RedisValue value, ValueConverter converter)
        {
            if (value.IsNull)
                return RedisResult<T>.Default;

            var converted = converter.Deserialize<T>(value);
            return new RedisResult<T>(converted);
        }


        /// <summary>
        /// 指定された値を <see cref="RedisResultWithExpiry{T}"/> に変換します。
        /// </summary>
        /// <typeparam name="T">データ型</typeparam>
        /// <param name="value">値</param>
        /// <param name="converter">値変換機能</param>
        /// <returns></returns>
        public static RedisResultWithExpiry<T> ToResult<T>(this in RedisValueWithExpiry value, ValueConverter converter)
        {
            if (value.Value.IsNull)
                return RedisResultWithExpiry<T>.Default;

            var converted = converter.Deserialize<T>(value.Value);
            return new RedisResultWithExpiry<T>(converted, value.Expiry);
        }
    }
}
