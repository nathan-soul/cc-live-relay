namespace CcLiveRelay.Util;

/// <summary>
/// Mutable byte buffer: append, in-place patch with zero-fill growth (the header PATCH
/// path), slice, and length. The body is append-only and the header is patched in place.
/// </summary>
public sealed class ByteBuffer
{
    private byte[] _buf = new byte[64 * 1024];

    public int Count { get; private set; }

    public byte this[int index] => _buf[index];

    public void Append(ReadOnlySpan<byte> data)
    {
        EnsureCapacity(Count + data.Length);
        data.CopyTo(_buf.AsSpan(Count));
        Count += data.Length;
    }

    /// <summary>
    /// In-place slice assignment `buf[offset..offset+len] = data`, growing with zero fill
    /// when the patch extends past the end.
    /// </summary>
    public void Patch(int offset, ReadOnlySpan<byte> data)
    {
        int needed = offset + data.Length;
        if (needed > Count)
        {
            EnsureCapacity(needed);
            _buf.AsSpan(Count, needed - Count).Clear();
            Count = needed;
        }
        data.CopyTo(_buf.AsSpan(offset));
    }

    public bool SliceEquals(int offset, ReadOnlySpan<byte> data) =>
        offset + data.Length <= Count && _buf.AsSpan(offset, data.Length).SequenceEqual(data);

    public byte[] ToArray() => _buf.AsSpan(0, Count).ToArray();

    public byte[] Slice(int start, int length) => _buf.AsSpan(start, length).ToArray();

    public void CopyTo(int start, Span<byte> destination) =>
        _buf.AsSpan(start, destination.Length).CopyTo(destination);

    private void EnsureCapacity(int needed)
    {
        if (needed <= _buf.Length)
            return;
        int newSize = Math.Max(_buf.Length * 2, needed);
        Array.Resize(ref _buf, newSize);
    }
}
