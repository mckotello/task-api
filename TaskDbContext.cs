using Microsoft.EntityFrameworkCore;

namespace TaskApi;

public sealed class TaskDbContext(DbContextOptions<TaskDbContext> options) : DbContext(options)
{
    public DbSet<TaskItem> Tasks => Set<TaskItem>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        var task = modelBuilder.Entity<TaskItem>();
        task.ToTable("tasks");
        task.HasKey(x => x.Id);
        task.Property(x => x.Title).HasMaxLength(200).IsRequired();
        task.Property(x => x.Priority).HasConversion<string>().HasMaxLength(16).IsRequired();
        task.Property(x => x.CreatedAt).IsRequired();
        task.Property(x => x.IsCompleted).IsRequired();

        // PostgreSQL xmin is a system-maintained row version. SQLite test provider skips this mapping.
        if (Database.ProviderName?.Contains("Npgsql", StringComparison.Ordinal) == true)
            task.Property<uint>("xmin").IsRowVersion();
    }
}
