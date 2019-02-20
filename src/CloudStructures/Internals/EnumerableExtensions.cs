using System;
using System.Collections.Generic;
using System.Linq;



namespace CloudStructures.Internals
{
    /// <summary>
    /// <see cref="IEnumerable{T}"/> の拡張機能を提供します。
    /// </summary>
    internal static class EnumerableExtensions
    {
        /// <summary>
        /// source の中身が空かどうかを返します。
        /// </summary>
        /// <typeparam name="T">要素の型</typeparam>
        /// <param name="source">コレクション</param>
        /// <returns></returns>
        public static bool IsEmpty<T>(this IEnumerable<T> source)
        {
            if (source == null)
                throw new ArgumentNullException(nameof(source));
            return !source.Any();
        }


        /// <summary>
        /// 状態を与えつつ射影します。
        /// </summary>
        /// <typeparam name="T">要素の型</typeparam>
        /// <typeparam name="TState">状態の型</typeparam>
        /// <typeparam name="TResult">結果の型</typeparam>
        /// <param name="source">コレクション</param>
        /// <param name="state">状態</param>
        /// <param name="selector">セレクター</param>
        /// <returns></returns>
        public static IEnumerable<TResult> Select<T, TState, TResult>(this IEnumerable<T> source, TState state, Func<T, TState, TResult> selector)
        {
            if (source == null) throw new ArgumentNullException(nameof(source));
            if (selector == null) throw new ArgumentNullException(nameof(selector));

            foreach (var x in source)
                yield return selector(x, state);
        }


        /// <summary>
        /// source が遅延状態の場合は実体化して返し、既に実体化されている場合は何もせずそれ自身を返します。
        /// </summary>
        /// <typeparam name="T">要素の型</typeparam>
        /// <param name="source">コレクション</param>
        /// <param name="nullToEmpty">true の場合、source が null 時は空シーケンスを返します。false の場合は <see cref="ArgumentNullException"/> を吐きます。</param>
        /// <returns></returns>
        public static IEnumerable<T> Materialize<T>(this IEnumerable<T> source, bool nullToEmpty = true)
        {
            if (source == null)
            {
                if (nullToEmpty)
                    return Enumerable.Empty<T>();
                throw new ArgumentNullException("source が null です");
            }
            if (source is ICollection<T>) return source;
            if (source is IReadOnlyCollection<T>) return source;
            return source.ToArray();
        }
    }
}
