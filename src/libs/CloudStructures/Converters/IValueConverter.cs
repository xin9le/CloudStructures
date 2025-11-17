namespace CloudStructures.Converters;



/// <summary>
/// Provides data conversion function.
/// </summary>
public interface IValueConverter
{
    /// <summary>
    /// Serialize to byte array.
    /// </summary>
    /// <typeparam name="T">Data type</typeparam>
    /// <param name="value"></param>
    /// <returns></returns>
    byte[] Serialize<T>(T value);


    /// <summary>
    /// Deserialize from byte array.
    /// </summary>
    /// <typeparam name="T">Data type</typeparam>
    /// <param name="value"></param>
    /// <returns></returns>
    T Deserialize<T>(byte[] value);
}
