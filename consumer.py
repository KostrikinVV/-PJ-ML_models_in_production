import asyncio
import os
import signal
import sys

from dotenv import load_dotenv
import aio_pika
from aio_pika import connect_robust


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


async def run_consumer(rabbitmq_url: str, queue_name: str, consumer_name: str = "Consumer") -> None:
    """
    Запустить consumer, который читает сообщения из очереди RabbitMQ.
    """
    connection = await connect_robust(rabbitmq_url)
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=1)

    queue = await channel.declare_queue(queue_name, durable=True)

    print(f"[{consumer_name}] Waiting for messages in queue '{queue_name}'. To exit press CTRL+C")

    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            async with message.process():
                body = message.body.decode("utf-8")
                print(f"[{consumer_name}] Received: {body}")


def main_consumer(queue_name: str, consumer_name: str = "Consumer") -> None:
    """
    Точка входа для consumer-скрипта.
    """
    config = load_config()
    rabbitmq_url = config["rabbitmq_url"]

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        loop.run_until_complete(run_consumer(rabbitmq_url, queue_name, consumer_name))
    except KeyboardInterrupt:
        print(f"\n[{consumer_name}] Stopped by user")
    finally:
        loop.close()


if __name__ == "__main__":
    config = load_config()

    # Пример запуска двух consumer'ов (для разных очередей)
    if len(sys.argv) > 1 and sys.argv[1] == "ru":
        main_consumer(config["ru_queue_name"], "RU Consumer")
    else:
        main_consumer(config["queue_name"], "Default Consumer")