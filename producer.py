from __future__ import annotations

import os
import asyncio
from typing import Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from dotenv import load_dotenv
import aio_pika
from aio_pika import connect_robust, Message, DeliveryMode


# =========================
# Общие модели/настройки
# =========================

class PublishRequest(BaseModel):
    """Модель входных данных для producer-сервиса."""
    message: str


# =========================
# Producer (FastAPI)
# =========================

producer_app = FastAPI(title="Producer API")


def load_config() -> dict:
    """
    Загрузить конфигурацию приложения из переменных окружения.
    """
    load_dotenv()

    rabbitmq_url = os.getenv("RABBITMQ_URL")
    queue_name = os.getenv("QUEUE_NAME")
    ru_queue_name = os.getenv("RU_QUEUE_NAME")

    if not queue_name:
        raise ValueError("QUEUE_NAME must be set in environment variables")

    return {
        "rabbitmq_url": rabbitmq_url,
        "queue_name": queue_name,
        "ru_queue_name": ru_queue_name,
    }


async def connect_rabbitmq(rabbitmq_url: str):
    """
    Установить асинхронное подключение к RabbitMQ.
    """
    try:
        connection = await connect_robust(rabbitmq_url)
        return connection
    except Exception as e:
        print(f"Failed to connect to RabbitMQ: {e}")
        raise


async def ensure_queue(connection, queue_name: str):
    """
    Создать канал и объявить очередь.
    """
    channel = await connection.channel()
    queue = await channel.declare_queue(queue_name, durable=True)
    return channel, queue


async def publish_message(channel, queue_name: str, payload: str) -> None:
    """
    Отправить сообщение в очередь RabbitMQ.
    """
    message = Message(
        body=payload.encode("utf-8"),
        delivery_mode=DeliveryMode.PERSISTENT
    )
    await channel.default_exchange.publish(message, routing_key=queue_name)


@producer_app.on_event("startup")
async def on_startup() -> None:
    """
    Подготовить подключение к RabbitMQ при старте FastAPI-приложения.
    """
    config = load_config()
    connection = await connect_rabbitmq(config["rabbitmq_url"])
    producer_app.state.connection = connection

    # Обеспечиваем существование обеих очередей
    channel1, _ = await ensure_queue(connection, config["queue_name"])
    channel2, _ = await ensure_queue(connection, config["ru_queue_name"])

    producer_app.state.channel = channel1  # сохраняем один канал для публикации
    producer_app.state.config = config

    print("Producer started and connected to RabbitMQ")


@producer_app.on_event("shutdown")
async def on_shutdown() -> None:
    """
    Корректно закрыть подключение к RabbitMQ при остановке приложения.
    """
    connection = getattr(producer_app.state, "connection", None)
    if connection:
        await connection.close()
        print("RabbitMQ connection closed")


def contains_cyrillic(text: str) -> bool:
    """Проверяет, содержит ли строка кириллицу."""
    cyrillic_letters = "АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯабвгдеёжзийклмнопрстуфхцчшщъыьэюя"
    return any(char in cyrillic_letters for char in text)


@producer_app.post("/publish")
async def publish_endpoint(req: PublishRequest):
    """
    HTTP-эндпоинт, который отправляет сообщение в RabbitMQ.
    """
    if not req.message or not req.message.strip():
        raise HTTPException(status_code=400, detail="Message cannot be empty")

    config = producer_app.state.config
    channel = producer_app.state.channel

    # Определяем целевую очередь
    if contains_cyrillic(req.message):
        target_queue = config["ru_queue_name"]
    else:
        target_queue = config["queue_name"]

    await publish_message(channel, target_queue, req.message)

    return {
        "status": "ok",
        "sent": req.message,
        "queue": target_queue
    }


# Для запуска сервера:
# uvicorn producer:producer_app --reload
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(producer_app, host="0.0.0.0", port=8000)