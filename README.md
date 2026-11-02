# Payway

Платёжный шлюз на Go, Kafka и PostgreSQL с тестовым HTTP-провайдером.
Создание, обработка, отмена платежей и история событий. Реальных списаний нет.

## Запуск

Нужны Docker и Docker Compose.

Создайте в корне `.env` с двумя разными случайными значениями из цифр и букв `a-f`:

```dotenv
POSTGRES_PASSWORD=<пароль БД>
CALLBACK_SECRET=<секрет длиной не менее 32 символов>
```

Запустите из корня проекта:

```bash
docker compose up --build
```

API: `http://localhost:8080`. Миграция существующей БД применяется сервисом `migrate`.
Если уже запускали предыдущую версию, добавьте `CALLBACK_SECRET` в свой `.env`.

## Платежи

```bash
curl -X POST http://localhost:8080/payments \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: order-001' \
  -d '{"merchant_id":"demo-shop","amount":12500,"currency":"KZT"}'
```

Сумма в минимальных единицах валюты: `12500` = `125.00 KZT`.
Для нового платежа нужен новый `Idempotency-Key`, для повтора — прежний ключ и те же данные.

Подставьте `payment_id` из ответа:

```bash
curl 'http://localhost:8080/payments/<payment_id>'
curl 'http://localhost:8080/payments/<payment_id>/events'
curl -X POST 'http://localhost:8080/payments/<payment_id>/cancel'
```

Обработка асинхронная: после создания возможен короткий `404`, затем `NEW` и `PROCESSING`.
Тестовый провайдер завершает платёж примерно через 8 секунд:

- `12500` — успешный платёж (`COMPLETED`).
- `12513` — отказ (`FAILED`): любые суммы, заканчивающиеся на `13`.
- `12599` — остаётся в `PROCESSING` для проверки отмены: суммы, заканчивающиеся на `99`.

Отмена возвращает `202`, результат нужно читать через GET. Уже завершённый платёж
не отменяется. При гонке с завершением итог определяет провайдер.
Старые платежи в `NEW` можно запустить повтором исходного POST с прежним ключом.
История поддерживает `?after_version=2&limit=50` (максимум 100 событий).

Callbacks отправляются провайдером автоматически на `POST /callbacks/mock`.
Подпись — HMAC-SHA256 от `timestamp + "." + body` с ключом `CALLBACK_SECRET`;
заголовки `X-Callback-Timestamp` и `X-Callback-Signature`, допуск времени — 5 минут.
Один `event_id` нельзя повторять с другим содержимым.

Примеры запросов: [examples/payments.http](examples/payments.http).

Пока нет авторизации пользователей, реальных PSP, Jaeger, Prometheus/Grafana,
DLQ и автоматического восстановления потерянных публикаций. Непригодное для обработки
сообщение задерживает consumer до устранения причины. Это локальная версия для разработки.

## Остановка

```bash
docker compose down
```
