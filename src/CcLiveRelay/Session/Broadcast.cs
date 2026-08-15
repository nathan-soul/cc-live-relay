using System.Text;
using CcLiveRelay.Protocol;

using static CcLiveRelay.Protocol.BinaryEnvelope;
namespace CcLiveRelay.Session;

public sealed partial class GameSession
{
    // ── Broadcast ───────────────────────────────────────────────────────────

    /// <summary>
    /// Queue a frame for every observer and return — no network I/O here, ever. Each observer
    /// has its own ObserverWriter (bounded channel + one writer task), so a slow observer only
    /// backs up its own queue and is dropped when it overflows (a rejoin re-serves catch-up).
    ///
    /// Must hold <c>_sync</c>: enqueues are serialized there, which makes per-observer order
    /// exact — a live chunk can never be delivered ahead of the catch-up that precedes it.
    ///
    /// Pass targets to pin the recipient list to a moment in the past — for body data that
    /// must be the instant the bytes were appended, so an observer that joined afterwards
    /// (and will receive them via catch-up) is not also sent them live.
    /// </summary>
    public void BroadcastEnvelope(byte msgType, byte[] payload, List<IClientSocket>? targets = null)
    {
        byte[] frame = BinaryEnvelope.Pack(msgType, payload);

        lock (_sync)
        {
            if (msgType == MsgEnd)
            {
                // Stream over: nothing is left to spoil. Held observers get the rest of the
                // body now (force flush), then the END frame — both enqueued together so the
                // writer delivers them in that order.
                foreach (var ws in targets ?? [.. _observerWsSet])
                {
                    if (IsHeld(ws))
                        EnqueueHeldFlushLocked(ws, force: true);
                    EnqueueObserverFrame(ws, frame);
                }
                return;
            }

            foreach (var ws in targets ?? [.. _observerWsSet])
            {
                // Held observers are served by the shared delayed edge, never a live chunk;
                // their pointer advances to the watermark at arrival + delay. MSG_TICK is
                // withheld from them for the same reason one step removed: it carries no
                // bytes, but it advertises the live edge, and the delay hold exists precisely
                // so a modified client cannot know — let alone reach — data younger than the
                // delay. MSG_STATS is the same argument again: the source's logic rate and ping
                // describe the match *now*, so a live one would be a liveness oracle for a
                // modified client. All three reach held observers from the delayed flush path.
                if (msgType is MsgBody or MsgTick or MsgStats && IsHeld(ws))
                    continue;
                EnqueueObserverFrame(ws, frame);
            }
        }
    }
}
