using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace CloudStructures
{
    internal static class TaskExtensions
    {
        public static ConfiguredTaskAwaitable ForAwait(this Task task)
        {
            return task.ConfigureAwait(false);
        }

        public static ConfiguredTaskAwaitable<T> ForAwait<T>(this Task<T> task)
        {
            return task.ConfigureAwait(false);
        }
    }
}