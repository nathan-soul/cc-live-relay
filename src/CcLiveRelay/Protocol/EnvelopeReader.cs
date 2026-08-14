using System.Buffers.Binary;

namespace CcLiveRelay.Protocol;

/// <summary>
/// Reassembles binary envelope frames from a stream of WebSocket chunks.
///
/// Kestrel hands the application arbitrary buffer-sized slices of a message (the default
/// receive buffer is 4 KB while source BODY frames can be 256 KB), so a frame can arrive
/// split across many ReceiveAsync results — and multiple frames can arrive inside one. This
/// reader appends every chunk and pulls complete frames off the front until none remain.
/// </summary>
public sealed class EnvelopeReader
{
    private byte[] _buf = new byte[64 * 1024];
    private int _count;

    public int BufferedBytes => _count;

    public void Append(ReadOnlyMemory<byte> data)
    {
        if (_count + data.Length > _buf.Length)
        {
            int newSize = Math.Max(_buf.Length * 2, _count + data.Length);
            Array.Resize(ref _buf, newSize);
        }
        data.Span.CopyTo(_buf.AsSpan(_count));
        _count += data.Length;
    }

    /// <summary>
    /// Pull one complete frame off the buffer. Returns false when the buffered bytes do not
    /// yet hold a full frame (the caller should wait for more data).
    /// </summary>
    public bool TryReadFrame(out byte msgType, out byte[] payload)
    {
        msgType = 0;
        payload = [];
        if (_count < 5)
            return false;
        int payloadLen = (int)BinaryPrimitives.ReadUInt32LittleEndian(_buf.AsSpan(1, 4));
        if (_count < 5 + payloadLen)
            return false;

        msgType = _buf[0];
        payload = _buf.AsSpan(5, payloadLen).ToArray();

        int consumed = 5 + payloadLen;
        int remaining = _count - consumed;
        if (remaining > 0)
            Array.Copy(_buf, consumed, _buf, 0, remaining);
        _count = remaining;
        return true;
    }
}
