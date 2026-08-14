namespace CcLiveRelay.State;

/// <summary>
/// A single-use credential minted on GO's behalf via /internal/*.
/// Key -> {lobby_id, user_id, priority, expires_at}. Expiry is fixed and short
/// (WATCH_TICKET_TTL_SECONDS): the client flow is atomic (GO validates JWT -> mints -> sends
/// -> client connects immediately), so a short window keeps the stolen-ticket replay small.
/// </summary>
public sealed class Credential
{
    public required string LobbyId { get; init; }
    public long? UserId { get; init; }
    /// <summary>Priority watchers (admin / user_priority = Viewer, decided by GO at mint time)
    /// bypass the relay's byte-level delay hold. Stream tokens never carry it.</summary>
    public bool Priority { get; init; }
    /// <summary>Unix seconds.</summary>
    public double ExpiresAt { get; init; }
}
