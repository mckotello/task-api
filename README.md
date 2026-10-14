# TaskApi (.NET 9)

Minimal API для задач: PostgreSQL + EF Core, публикация событий в RabbitMQ, интеграционный тест с SQLite и подменой publisher.

## Запуск зависимостей

```bash
docker compose up -d
```

Нужен установленный .NET 9 SDK.

## Запуск API

Из каталога `TaskApi`:

```bash
dotnet restore
dotnet run --urls http://localhost:8080
```

При старте приложение создаёт таблицы через `EnsureCreated`, поэтому миграции для первого запуска не требуются. Для реального проекта лучше добавить EF migrations и заменить `EnsureCreated()` на `Migrate()`.

Endpoints:

- `POST /tasks` — JSON `{"title":"Buy milk","priority":"High"}`; `priority` необязателен и по умолчанию `Medium`.
- `GET /tasks` — все задачи.
- `PUT /tasks/{id}/complete` — завершить задачу; повторное/конкурентное завершение возвращает `409`.
- `DELETE /tasks/{id}` — физически удалить задачу.

Пример:

```bash
curl -i -X POST http://localhost:8080/tasks -H "Content-Type: application/json" -d '{"title":"Buy milk","priority":"High"}'
curl -i http://localhost:8080/tasks
```

RabbitMQ Management UI: http://localhost:15672 (guest / guest). Exchange `task.events` типа `topic` создаётся приложением; routing key события — `task.completed`.

## Тесты

```bash
dotnet test Tests/TaskApi.Tests.csproj
```

Тест поднимает приложение через `WebApplicationFactory`, использует SQLite in-memory для БД и `RecordingPublisher` вместо реального RabbitMQ. Он проверяет создание задачи, завершение и содержимое опубликованного события.

## Примечания по реализации

- PostgreSQL не имеет SQL Server-style `rowversion`; для optimistic concurrency используется системная колонка `xmin` как EF Core row-version token. `DbUpdateConcurrencyException` преобразуется в `409 Conflict`.
- Событие ставится в ограниченную очередь только после успешного `SaveChangesAsync`. HTTP-запрос не ждёт RabbitMQ publish. Ошибки публикации логируются, но не откатывают завершение задачи.
- При shutdown очередь закрывается, worker получает до 4 секунд на отправку накопленных событий, затем канал и соединение RabbitMQ закрываются.
- Это не transactional outbox: аварийное завершение процесса между сохранением в БД и публикацией может потерять событие. Для production стоит добавить outbox и ретраи.
