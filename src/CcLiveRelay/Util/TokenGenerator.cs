using System.Buffers.Text;
using System.Security.Cryptography;

namespace CcLiveRelay.Util;

/// <summary>n random bytes, base64url-encoded, for the single-use credentials.</summary>
public static class TokenGenerator
{
    public static string TokenUrlSafe(int byteCount)
    {
        byte[] bytes = RandomNumberGenerator.GetBytes(byteCount);
        return Base64Url.EncodeToString(bytes);
    }
}
