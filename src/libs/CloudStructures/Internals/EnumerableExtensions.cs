using System;
using System.Collections.Generic;
using System.Linq;

namespace CloudStructures.Internals;



/// <summary>
/// Provides extension methods for <see cref="IEnumerable{T}"/>.
/// </summary>
internal static class EnumerableExtensions
{
    extension<T>(IEnumerable<T> @this)
    {
        /// <summary>
        /// Returns if collection is empty.
        /// </summary>
        /// <returns></returns>
        public bool IsEmpty()
            => !@this.Any();


        /// <summary>
        /// Projects each element of a sequance into new form with state. 
        /// </summary>
        /// <typeparam name="TState">State type</typeparam>
        /// <typeparam name="TResult">Result type</typeparam>
        /// <param name="state"></param>
        /// <param name="selector"></param>
        /// <returns></returns>
        public IEnumerable<TResult> Select<TState, TResult>(TState state, Func<T, TState, TResult> selector)
        {
            foreach (var x in @this)
                yield return selector(x, state);
        }
    }


    extension<T>(IEnumerable<T>? @this)
    {
        /// <summary>
        /// If state of source is lazy, returns materialized collection. if materialized already, it does nothing and returns itself.
        /// </summary>
        /// <param name="nullToEmpty">If true, returns empty sequence when source collection is null. If false, throws <see cref="ArgumentNullException"/></param>
        /// <returns></returns>
        public IEnumerable<T> Materialize(bool nullToEmpty = true)
        {
            if (@this is null)
            {
                if (nullToEmpty)
                    return [];

#if NET6_0_OR_GREATER
                ArgumentNullException.ThrowIfNull(@this);
#else
                throw new ArgumentNullException(nameof(@this));
#endif
            }
            if (@this is ICollection<T>) return @this;
            if (@this is IReadOnlyCollection<T>) return @this;
            return [.. @this];
        }
    }
}
