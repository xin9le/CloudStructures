using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CloudStructures
{
    public class SortedSetResult<T>
    {
        public T Value { get; private set; }
        public double Score { get; private set; }

        public SortedSetResult(T value, double score)
        {
            Value = value;
            Score = score;
        }

        public override string ToString()
        {
            return $"Score:{Score}, Value:{Value}";
        }
    }

    public class SortedSetResultWithRank<T>
    {
        public T Value { get; private set; }
        public double Score { get; private set; }
        public double Rank { get; private set; }

        public SortedSetResultWithRank(T value, double score, double rank)
        {
            Value = value;
            Score = score;
            Rank = rank;
        }

        public override string ToString()
        {
            return $"Rank:{Rank}, Score:{Score}, Value:{Value}";
        }
    }
}
