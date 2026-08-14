using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using CcLiveRelay.Protocol;

using CcLiveRelay.Util;
using static CcLiveRelay.Protocol.BinaryEnvelope;
namespace CcLiveRelay.Session;

public sealed partial class GameSession
{
    // ── Chat ──
    // MSG_CHAT = player chat, frame-stamped by the streamer, frame-gated on the observer;
    // the relay stores a bounded history so late-joining watchers get the recent slice in
    // their catch-up. MSG_SPECTATOR_CHAT = live spectator meta-chat (Twitch-style): no
    // history by design ("you missed it"), rate-limited per connection.
    private readonly List<byte[]> _chatHistory = [];

    /// <summary>
    /// Player chat (MSG_CHAT): store + broadcast. Deduped by opaque payload. All-push model:
    /// every source's client executes the same NetChatCommandMsg at the same frame, so several
    /// sources forward byte-identical copies of each message — whole-payload dedupe against
    /// the history drops the duplicates.
    /// </summary>
    public async Task ApplyChatAsync(IClientSocket ws, byte[] payload)
    {
        if (_sourceDemoted(ws))
            return;
        if (payload.Length == 0)
            return;
        bool isNew;
        lock (_sync)
        {
            isNew = !_chatHistory.Any(p => p.AsSpan().SequenceEqual(payload));
            if (!isNew)
                return;
            _chatHistory.Add(payload);
            while (_chatHistory.Count > _options.ChatHistoryMax)
                _chatHistory.RemoveAt(0);
            _lastActive = TimeSource.Now();
        }
        if (_options.Debug)
            Console.WriteLine($"[LIVESTREAMER] [CHAT] Game {LobbyId}: player chat frame ({payload.Length}B)");
        BroadcastEnvelope(MsgChat, payload);
    }

    /// <summary>
    /// Fan a spectator chat frame out to observer-mode sources only. Streaming players must
    /// never see spectator chat. Plain sends with per-socket error handling; sources have no
    /// catch-up/lock machinery (spectator chat is live and unordered).
    /// </summary>
    private async Task SendToObserverSourcesAsync(byte[] payload)
    {
        byte[] frame = BinaryEnvelope.Pack(MsgSpectatorChat, payload);
        List<IClientSocket> dead = [];
        List<IClientSocket> sources;
        lock (_sync) sources = [.. _sources];
        foreach (var ws in sources)
        {
            if (!IsObserverSource(ws))
                continue;
            try
            {
                await ws.SendAsync(frame);
            }
            catch (Exception e)
            {
                Console.WriteLine($"[LIVESTREAMER] [WARN] spectator chat send to source failed " +
                                  $"({e.GetType().Name}: {e.Message}), marking dead");
                dead.Add(ws);
            }
        }
        if (dead.Count > 0)
        {
            lock (_sync)
            {
                foreach (var ws in dead)
                {
                    _sources.Remove(ws);
                    _sourceHealth.Remove(ws);
                    _sourceObserver.Remove(ws);
                }
            }
        }
    }

    /// <summary>
    /// Spectator chat (MSG_SPECTATOR_CHAT): live, rate-limited, no history. Senders: watchers
    /// (v1) and, defensively, observer-mode sources. Broadcast to all watchers plus
    /// observer-mode sources. Deliberately no history/catch-up.
    /// </summary>
    public async Task ApplySpectatorChatAsync(IClientSocket ws, byte[] payload)
    {
        if (payload.Length == 0)
            return;
        double now = TimeSource.Now();
        lock (_sync)
        {
            if (!_spectatorRate.TryGetValue(ws, out var stamps))
            {
                stamps = [];
                _spectatorRate[ws] = stamps;
            }
            stamps.RemoveAll(t => now - t >= _options.SpectatorRateWindow);
            if (stamps.Count >= _options.SpectatorRateMax)
            {
                Console.WriteLine($"[LIVESTREAMER] [CHAT] spectator chat rate-limited for {ws.GetHashCode():x}");
                return;
            }
            stamps.Add(now);
        }
        if (_options.Debug)
            Console.WriteLine($"[LIVESTREAMER] [CHAT] Game {LobbyId}: spectator chat ({payload.Length}B)");
        BroadcastEnvelope(MsgSpectatorChat, payload);
        await SendToObserverSourcesAsync(payload);
    }

    /// <summary>The last CHAT_CATCHUP_COUNT chat frames, for a joining observer.</summary>
    private List<byte[]> ChatCatchupSlice()
    {
        lock (_sync)
        {
            int start = Math.Max(0, _chatHistory.Count - _options.ChatCatchupCount);
            return _chatHistory.Skip(start).ToList();
        }
    }
}
