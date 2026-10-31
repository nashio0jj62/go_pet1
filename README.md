# Payway

Платёжный шлюз на Go, Kafka и PostgreSQL. Пока реализованы создание платежа в статусе `NEW` и получение его состояния.

## Запуск

Нужны Docker и Docker Compose.

1. Создайте в корне файл `.env`:

   ```dotenv
   POSTGRES_PASSWORD=<ваш пароль>
   ```

   Вместо `<ваш пароль>` укажите случайный пароль из цифр и букв `a-f`. Файл `.env` не добавляется в Git.

2. Запустите из корня проекта:

   ```bash
   docker compose up --build
   ```

API будет доступен на `http://localhost:8080`.

## Создать платёж

```bash
curl -X POST http://localhost:8080/payments \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: order-001' \
  -d '{"merchant_id":"demo-shop","amount":12500,"currency":"KZT"}'
```

Сумма указана в минимальных единицах валюты: `12500` = `125.00 KZT`.
Для нового платежа используйте новый `Idempotency-Key`.

Подставьте `payment_id` из ответа:

```bash
curl 'http://localhost:8080/payments/<payment_id>'
```

Обработка асинхронная: если сразу получили `404`, повторите запрос чуть позже.
Другие примеры — в [examples/payments.http](examples/payments.http).

## Остановка

```bash
docker compose down
```
