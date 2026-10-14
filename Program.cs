using System.Text.Json.Serialization;
using Microsoft.EntityFrameworkCore;

namespace TaskApi;

public sealed record CreateTaskRequest(string? Title, Priority Priority = Priority.Medium);

public static class Program
{
    public static void Main(string[] args) => CreateApp(args).Run();

    public static WebApplication CreateApp(string[] args)
    {
        var builder = WebApplication.CreateBuilder(args);
        builder.Services.ConfigureHttpJsonOptions(options =>
            options.SerializerOptions.Converters.Add(new JsonStringEnumConverter()));

        builder.Services.AddDbContext<TaskDbContext>(options =>
            options.UseNpgsql(builder.Configuration.GetConnectionString("Tasks") ??
                "Host=localhost;Port=5432;Database=taskapi;Username=postgres;Password=postgres"));

        builder.Services.AddSingleton<RabbitMqPublisher>();
        builder.Services.AddSingleton<ITaskEventPublisher>(sp => sp.GetRequiredService<RabbitMqPublisher>());
        builder.Services.AddHostedService(sp => sp.GetRequiredService<RabbitMqPublisher>());

        var app = builder.Build();

        // Keep the sample runnable without a generated migration. Replace with MigrateAsync once migrations are added.
        if (!app.Environment.IsEnvironment("Testing"))
        {
            using var scope = app.Services.CreateScope();
            scope.ServiceProvider.GetRequiredService<TaskDbContext>().Database.EnsureCreated();
        }

        app.MapPost("/tasks", async (CreateTaskRequest request, TaskDbContext db, CancellationToken ct) =>
        {
            if (string.IsNullOrWhiteSpace(request.Title) || request.Title.Length > 200)
                return Results.BadRequest(new { error = "Title must contain 1–200 non-whitespace characters." });
            if (!Enum.IsDefined(request.Priority))
                return Results.BadRequest(new { error = "Priority must be Low, Medium or High." });

            var task = new TaskItem
            {
                Id = Guid.NewGuid(),
                Title = request.Title.Trim(),
                IsCompleted = false,
                CreatedAt = DateTimeOffset.UtcNow,
                CompletedAt = null,
                Priority = request.Priority
            };
            db.Tasks.Add(task);
            await db.SaveChangesAsync(ct);
            return Results.Created($"/tasks/{task.Id}", task);
        });

        app.MapGet("/tasks", async (TaskDbContext db, CancellationToken ct) =>
            Results.Ok(await db.Tasks.AsNoTracking().OrderByDescending(x => x.CreatedAt).ToListAsync(ct)));

        app.MapPut("/tasks/{id:guid}/complete", async (Guid id, TaskDbContext db, ITaskEventPublisher publisher, ILogger<Program> logger, CancellationToken ct) =>
        {
            var task = await db.Tasks.SingleOrDefaultAsync(x => x.Id == id, ct);
            if (task is null) return Results.NotFound();
            if (task.IsCompleted) return Results.Conflict(new { error = "Task is already completed." });

            task.IsCompleted = true;
            task.CompletedAt = DateTimeOffset.UtcNow;
            try
            {
                await db.SaveChangesAsync(ct);
            }
            catch (DbUpdateConcurrencyException)
            {
                // PostgreSQL xmin detects another request updating this row between read and save.
                logger.LogInformation("Concurrent completion attempt for task {TaskId}", id);
                return Results.Conflict(new { error = "Task was completed by another request." });
            }

            var published = publisher.TryPublish(new TaskCompletedEvent(task.Id, task.Title, task.CompletedAt.Value, task.Priority));
            if (!published) logger.LogError("Completion event for task {TaskId} could not be queued", task.Id);
            return Results.Ok(task);
        });

        app.MapDelete("/tasks/{id:guid}", async (Guid id, TaskDbContext db, CancellationToken ct) =>
        {
            var task = await db.Tasks.SingleOrDefaultAsync(x => x.Id == id, ct);
            if (task is null) return Results.NotFound();
            db.Tasks.Remove(task);
            await db.SaveChangesAsync(ct);
            return Results.NoContent();
        });

        return app;
    }
}
