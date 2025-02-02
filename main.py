from confluent_kafka import Consumer, KafkaException, KafkaError
import threading
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, CallbackContext
import asyncio
import redis
import json
import configparser

config = configparser.ConfigParser()
config.read("config.ini")

TELEGRAM_TOKEN = config["TELEGRAM"]["TOKEN"]

kafka_host = config["KAFKA"]["HOST"]
kafka_port = int(config["KAFKA"]["PORT"])
kafka_topic = config["KAFKA"]["TOPIC"]

redis_host = config["REDIS"]["HOST"]
redis_port = int(config["REDIS"]["PORT"])
redis_number_database = int(config["REDIS"]["NUMBER_DATABASE"])

r = redis.Redis(host=redis_host, port=redis_port, db=redis_number_database, decode_responses=True)
kafka_conf = {
    'bootstrap.servers': f'{kafka_host}:{kafka_port}',
    'group.id': 'my_group',
    'auto.offset.reset': 'earliest'  # если не можем найти смещение
}
consumer = Consumer(kafka_conf)
consumer.subscribe([kafka_topic])


async def start(update: Update, context: CallbackContext) -> None:
    user_id = update.message.from_user.id
    user_name = update.message.from_user.username
    if user_name is None:
        user_name=""
    r.hset("user_storage", user_name, user_id)
    await update.message.reply_text(f"Привет, {user_name}! Ты добавлен в рассылку. .... ")

# Функция для обработки любых других сообщений
async def other_messages(update: Update, context: CallbackContext) -> None:
    await update.message.reply_text("Этот бот не умеет отвечать на сообщения :( Нажми /start чтобы узнать что он умеет.")


async def send_message(context: CallbackContext,user_name, msg) -> None:
    user_id = r.hget("user_storage", user_name)
    s = r.hgetall("user_storage")
    print(s)
    if user_id is None:
        print(f"User {user_id} not found")
        return
    try:
        await context.bot.send_message(chat_id=user_id, text=msg,parse_mode='Markdown')
    except Exception as e:
        print(f"Error sending user {user_id}: {e}")


def get_prepared_message(parsed_json):
    def getTransefrs(transfers):
        if transfers==0:
            return "Без пересадок"
        else:
            return "С пересадками"
    prepared_message = (f"Текущая информация по маршруту "
                        f"**{parsed_json["ticketData"]["origin"]}** -> **{parsed_json["ticketData"]["destination"]}** \n"
                        f"Дата/время отправления: **{parsed_json["ticketData"]["departure_at"]}**"
                        f"\n\n\n"
                        f"Цена: **{parsed_json["ticketData"]["price"]}** руб;\n"
                        f"{getTransefrs(parsed_json["ticketData"]["transfers"])}; \n"
                        f"Билет: {parsed_json["ticketData"]["link"]}")
    return prepared_message
def get_userName_and_prepared_message(msg:str) ->tuple:
    parsed_json = json.loads(msg)
    try:
        parsed_json = json.loads(msg)
        user_name = parsed_json["telegram"]
        prepared_message = get_prepared_message(parsed_json)
        return user_name,prepared_message
    except json.JSONDecodeError as e:
        print(f"Error parsing message to JSON: {e}")
        return "", ""

def listen_kafka(app: Application):
    """Поток, слушающий нажатие Enter для отправки рассылки."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        while True:
            msg = consumer.poll(timeout=2.0)  # читаем сообщение
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f'End of section {msg.topic()} [{msg.partition()}]')
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                msg_str = msg.value().decode("utf-8")
                print("Recieved message")
                user_name,prepared_message = get_userName_and_prepared_message(msg_str)
                loop.run_until_complete(send_message(app, user_name, prepared_message))
    finally:
        consumer.close()


app = Application.builder().token(TELEGRAM_TOKEN).build()
app.add_handler(CommandHandler("start", start))
app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, other_messages))
# Запуск потока для прослушивания нажатий Enter
thread = threading.Thread(target=listen_kafka, args=(app,), daemon=True)
thread.start()

app.run_polling()
