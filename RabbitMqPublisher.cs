using System.Text.Json;
using System.Threading.Channels;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;

namespace TaskApi;

public sealed class RabbitMqPublisher : ITaskEventPublisher, IHostedService, IDisposable
{
    private const string ExchangeName = "task.events";
    private const string RoutingKey = "task.completed";
    private readonly ILogger<RabbitMqPublisher> _logger;
    private readonly IConfiguration _configuration;
    private readonly Channel<TaskCompletedEvent> _queue = Channel.CreateBounded<TaskCompletedEvent>(
        new BoundedChannelOptions(500) { FullMode = BoundedChannelFullMode.Wait, SingleReader = true, SingleWriter = false });
    private readonly CancellationTokenSource _stop = new();
    private Task? _worker;
    private IConnection? _connection;
    private IModel? _channel;

    public RabbitMqPublisher(ILogger<RabbitMqPublisher> logger, IConfiguration configuration)
    {
        _logger = logger;
        _configuration = configuration;
    }

    public bool TryPublish(TaskCompletedEvent message)
    {
        if (_queue.Writer.TryWrite(message)) return true;
        _logger.LogError("RabbitMQ queue is full or stopping. Event for task {TaskId} was not queued", message.TaskId);
        return false;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _worker = RunAsync(_stop.Token);
        return Task.CompletedTask;
    }

    private async Task RunAsync(CancellationToken cancellationToken)
    {
        try
        {
            try
            {
                var factory = new ConnectionFactory
                {
                    Uri = new Uri(_configuration["RabbitMq:Uri"] ?? "amqp://guest:guest@localhost:5672/"),
                    DispatchConsumersAsync = true,
                    AutomaticRecoveryEnabled = false,
                    RequestedConnectionTimeout = TimeSpan.FromSeconds(2)
                };
                _connection = factory.CreateConnection();
                _channel = _connection.CreateModel();
                _channel.ExchangeDeclare(ExchangeName, ExchangeType.Topic, durable: true, autoDelete: false);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "RabbitMQ is unavailable; completion events will be logged and dropped");
            }

            await foreach (var message in _queue.Reader.ReadAllAsync(cancellationToken))
            {
                if (_channel is null)
                {
                    _logger.LogError("Cannot publish completion event for task {TaskId}: RabbitMQ is unavailable", message.TaskId);
                    continue;
                }

                try
                {
                    var body = JsonSerializer.SerializeToUtf8Bytes(new
                    {
                        taskId = message.TaskId,
                        title = message.Title,
                        completedAt = message.CompletedAt,
                        priority = message.Priority.ToString()
                    });
                    var properties = _channel.CreateBasicProperties();
                    properties.ContentType = "application/json";
                    properties.DeliveryMode = 2;
                    _channel.BasicPublish(ExchangeName, RoutingKey, mandatory: false, basicProperties: properties, body: body);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to publish completion event for task {TaskId}", message.TaskId);
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Expected after the graceful-shutdown deadline.
        }
        finally
        {
            try { _channel?.Close(); } catch (Exception ex) { _logger.LogDebug(ex, "Error closing RabbitMQ channel"); }
            try { _connection?.Close(); } catch (Exception ex) { _logger.LogDebug(ex, "Error closing RabbitMQ connection"); }
            _channel?.Dispose();
            _connection?.Dispose();
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _queue.Writer.TryComplete();
        if (_worker is null) return;

        // Allow queued publications to drain for up to four seconds.
        var completed = await Task.WhenAny(_worker, Task.Delay(TimeSpan.FromSeconds(4), cancellationToken));
        if (completed != _worker)
        {
            _stop.Cancel();
            try { await _worker; } catch (OperationCanceledException) { }
        }
        else
        {
            await _worker;
        }
    }

    public void Dispose()
    {
        _stop.Cancel();
        _stop.Dispose();
    }
}
