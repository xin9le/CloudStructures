using System;

namespace CloudStructures;



/// <summary>
/// Contains information about a server connection establishment. 
/// </summary>
public sealed class ConnectionOpenedEventArgs : EventArgs
{
    /// <summary>
    /// Gets the elapsed time to establish connection.
    /// </summary>
    public TimeSpan Elapsed { get; }


    /// <summary>
    /// Creates instance.
    /// </summary>
    /// <param name="elapsed"></param>
    internal ConnectionOpenedEventArgs(TimeSpan elapsed)
        => this.Elapsed = elapsed;
}
