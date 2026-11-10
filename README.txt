Лабораторная работа № 5 — микросервисная бонусная программа

Структура:
api       — приём операций и чтение результатов;
worker    — расчёт бонусов, запись в PostgreSQL и публикация событий;
notifier  — обработка событий и хранение уведомлений в памяти;
analytics — статистика по магазинам в памяти;
postgres — база данных;
rabbitmq — брокер сообщений.

Запуск из папки lab5:
    docker compose up --build

Проверка контейнеров:
    docker compose ps

Документация API:
    http://localhost:8000/docs

Документация Notifier:
    http://localhost:8001/docs

Документация Analytics:
    http://localhost:8002/docs

RabbitMQ Management:
    http://localhost:15672
    логин: loyalty
    пароль: loyalty_pass

Тестовая покупка:
{
  "operation_id": "S12-K03-000154",
  "type": "purchase",
  "customer_id": "C1042",
  "store_id": "S12",
  "check_number": "S12-000154",
  "amount": "2490.00",
  "occurred_at": "2026-11-10T14:32:05+07:00"
}

После обработки:
GET http://localhost:8000/operations/S12-K03-000154
GET http://localhost:8000/accounts/C1042
GET http://localhost:8001/notifications?customer_id=C1042
GET http://localhost:8001/stats
GET http://localhost:8002/stores
GET http://localhost:8002/stats

Важно:
- Таблицы PostgreSQL создаёт worker автоматически.
- Уведомления и статистика хранятся в памяти и теряются после перезапуска соответствующего сервиса.
- Перед сдачей необходимо запустить проект и проверить все сценарии.
