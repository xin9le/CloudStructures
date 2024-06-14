using System;
using System.Collections.Generic;
using System.Linq;

namespace CloudStructures.Internals;



/// <summary>
/// Provides extension methods for <see cref="IEnumerable{T}"/>.
/// </summary>
internal static class EnumerableExtensions
{
    /// <summary>
    /// Returns if collection is empty.
    /// </summary>
    /// <typeparam name="T">Element type</typeparam>
    /// <param name="source"></param>
    /// <returns></returns>
    public static bool IsEmpty<T>(this IEnumerable<T> source)
        => !source.Any();


    /// <summary>
    /// Projects each element of a sequance into new form with state.
    /// </summary>
    /// <typeparam name="T">Element type</typeparam>
    /// <typeparam name="TState">State type</typeparam>
    /// <typeparam name="TResult">Result type</typeparam>
    /// <param name="source"></param>
    /// <param name="state"></param>
    /// <param name="selector"></param>
    /// <returns></returns>
    public static IEnumerable<TResult> Select<T, TState, TResult>(this IEnumerable<T> source, TState state, Func<T, TState, TResult> selector)
    {
        foreach (var x in source)
            yield return selector(x, state);
    }


    /// <summary>
    /// If state of source is lazy, returns materialized collection. if materialized already, it does nothing and returns itself.
    /// </summary>
    /// <typeparam name="T">Element type</typeparam>
    /// <param name="source"></param>
    /// <param name="nullToEmpty">If true, returns empty sequence when source collection is null. If false, throws <see cref="ArgumentNullException"/></param>
    /// <returns></returns>
    public static IEnumerable<T> Materialize<T>(this IEnumerable<T>? source, bool nullToEmpty = true)
    {
        if (source is null)
        {
            if (nullToEmpty)
                return new T[0];

#if NET6_0_OR_GREATER
            ArgumentNullException.ThrowIfNull(source);
#else
            throw new ArgumentNullException(nameof(source));
#endif
        }
        if (source is ICollection<T>) return source;
        if (source is IReadOnlyCollection<T>) return source;
        return source.ToArray();
    }
}
