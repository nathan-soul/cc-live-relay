using System.Net.WebSockets;

namespace CcLiveRelay.Session;

/// <summary>
/// The send-side surface GameSession talks to. The production implementation wraps the
/// ASP.NET Core WebSocket; unit tests inject a fake that records frames, exactly as
/// test_session_unit.py's FakeWS does. Sends are always one complete binary envelope frame.
/// </summary>
public interface IClientSocket
{
    bool IsOpen { get; }
    Task SendAsync(byte[] frame);
    Task CloseAsync();
}

public sealed class WebSocketClientSocket : IClientSocket
{
    private readonly WebSocket _ws;

    public WebSocketClientSocket(WebSocket ws) => _ws = ws;

    public bool IsOpen => _ws.State == WebSocketState.Open;

    public Task SendAsync(byte[] frame) =>
        _ws.SendAsync(frame, WebSocketMessageType.Binary, true, CancellationToken.None);

    public Task CloseAsync() =>
        _ws.CloseAsync(WebSocketCloseStatus.NormalClosure, null, CancellationToken.None);
}
