using System.Buffers.Binary;

namespace CcLiveRelay.Protocol;

/// <summary>
/// Binary envelope protocol, aligned with the C++ LiveStreamer/LiveObserver clients and
/// one byte type + uint32 LE payload length + payload.
/// </summary>
public static class BinaryEnvelope
{
    // ── Binary message types (aligned with C++ client) ────────────────────────
    public const byte MsgRegister = 0;
    public const byte MsgHeader = 1;
    public const byte MsgPatch = 2;
    public const byte MsgBody = 3;
    public const byte MsgEnd = 4;
    public const byte MsgRole = 5;
    public const byte MsgError = 6;
    public const byte MsgChat = 7;
    public const byte MsgSpectatorChat = 8;
    // Frame heartbeat: the source's current logic
    // frame, [frame u32 LE]. The replay body only contains records for frames that have
    // input, so in quiet play it is silent apart from one CRC record every ~1.7 s; an
    // observer deriving the live edge from those records learns where the game is in 1.7 s
    // jumps and starves between them. The tick states the frame directly. It is opaque to
    // the relay, which forwards it and remembers the latest value for observers joining
    // later. Safety invariant: the source emits the tick for frame N right after flushing
    // that frame's records over the same ordered transport, so "tick N arrived" proves
    // "every record with frame <= N arrived".
    public const byte MsgTick = 9;

    public static byte[] Pack(byte msgType, ReadOnlySpan<byte> payload)
    {
        var frame = new byte[5 + payload.Length];
        frame[0] = msgType;
        BinaryPrimitives.WriteUInt32LittleEndian(frame.AsSpan(1), (uint)payload.Length);
        payload.CopyTo(frame.AsSpan(5));
        return frame;
    }

    public static byte[] Pack(byte msgType) => Pack(msgType, ReadOnlySpan<byte>.Empty);

    /// <summary>Unpack a complete frame. Returns false if fewer than 5 bytes, or when the
    /// declared payload length exceeds what is present.</summary>
    public static bool TryUnpackFrame(ReadOnlySpan<byte> data, out byte msgType, out byte[] payload)
    {
        msgType = 0;
        payload = [];
        if (data.Length < 5)
            return false;
        msgType = data[0];
        int payloadLen = (int)BinaryPrimitives.ReadUInt32LittleEndian(data.Slice(1, 4));
        if (data.Length < 5 + payloadLen)
            return false;
        payload = data.Slice(5, payloadLen).ToArray();
        return true;
    }

    public static byte[] PackU32(uint value)
    {
        var buf = new byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(buf, value);
        return buf;
    }

    public static byte[] PackU64(ulong value)
    {
        var buf = new byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(buf, value);
        return buf;
    }
}
