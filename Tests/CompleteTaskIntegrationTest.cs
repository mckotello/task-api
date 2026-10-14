using System.Collections.Concurrent;
using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using TaskApi;
using Xunit;

namespace TaskApi.Tests;

public sealed class CompleteTaskIntegrationTest : IClassFixture<TaskApiFactory>
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web)
    { Converters = { new JsonStringEnumConverter() } };
    private readonly HttpClient _client;
    private readonly TaskApiFactory _factory;

    public CompleteTaskIntegrationTest(TaskApiFactory factory)
    {
        _factory = factory;
        _client = factory.CreateClient();
        using var scope = factory.Services.CreateScope();
        scope.ServiceProvider.GetRequiredService<TaskDbContext>().Database.EnsureCreated();
    }

    [Fact]
    public async Task Create_Then_Complete_Queues_Rabbit_Event()
    {
        var createResponse = await _client.PostAsJsonAsync("/tasks", new { title = "Buy milk", priority = "High" });
        Assert.Equal(HttpStatusCode.Created, createResponse.StatusCode);
        var created = await createResponse.Content.ReadFromJsonAsync<TaskItem>(JsonOptions);
        Assert.NotNull(created);
        Assert.False(created.IsCompleted);
        Assert.Null(created.CompletedAt);

        var completeResponse = await _client.PutAsync($"/tasks/{created.Id}/complete", null);
        Assert.Equal(HttpStatusCode.OK, completeResponse.StatusCode);
        var completed = await completeResponse.Content.ReadFromJsonAsync<TaskItem>(JsonOptions);
        Assert.NotNull(completed);
        Assert.True(completed.IsCompleted);
        Assert.NotNull(completed.CompletedAt);

        var publisher = _factory.Publisher;
        Assert.True(publisher.Messages.TryDequeue(out var message));
        Assert.Equal(created.Id, message!.TaskId);
        Assert.Equal("Buy milk", message.Title);
        Assert.Equal(Priority.High, message.Priority);
        Assert.Equal(completed.CompletedAt, message.CompletedAt);
    }
}

public sealed class TaskApiFactory : WebApplicationFactory<Program>
{
    private readonly Microsoft.Data.Sqlite.SqliteConnection _connection = new("Data Source=:memory:");
    public RecordingPublisher Publisher { get; } = new();

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.UseEnvironment("Testing");
        _connection.Open();
        builder.ConfigureServices(services =>
        {
            services.RemoveAll<DbContextOptions<TaskDbContext>>();
            services.RemoveAll<Microsoft.EntityFrameworkCore.Infrastructure.IDbContextOptionsConfiguration<TaskDbContext>>();
            services.RemoveAll<TaskDbContext>();
            services.AddDbContext<TaskDbContext>(options => options.UseSqlite(_connection));
            services.RemoveAll<ITaskEventPublisher>();
            services.AddSingleton<ITaskEventPublisher>(Publisher);
        });
    }

    protected override void Dispose(bool disposing)
    {
        base.Dispose(disposing);
        if (disposing) _connection.Dispose();
    }

    public override async ValueTask DisposeAsync()
    {
        await base.DisposeAsync();
        await _connection.DisposeAsync();
    }
}

public sealed class RecordingPublisher : ITaskEventPublisher
{
    public ConcurrentQueue<TaskCompletedEvent> Messages { get; } = new();
    public bool TryPublish(TaskCompletedEvent message)
    {
        Messages.Enqueue(message);
        return true;
    }
}
