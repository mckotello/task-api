namespace TaskApi;

public sealed record TaskCompletedEvent(
    Guid TaskId,
    string Title,
    DateTimeOffset CompletedAt,
    Priority Priority);

public interface ITaskEventPublisher
{
    // Enqueues the event; does not wait for network I/O.
    bool TryPublish(TaskCompletedEvent message);
}
