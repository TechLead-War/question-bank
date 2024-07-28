import os
from datetime import datetime

import nsq
import json

import psycopg2
import tornado.ioloop
from bson import ObjectId
from pymongo import MongoClient

from config import Config

MONGO_DB_URI = os.getenv("MONGO_DB_URI", "mongodb://localhost:27017")
mongo_client = MongoClient(
    MONGO_DB_URI,
    maxPoolSize=200,
    minPoolSize=10
)

db = mongo_client["questionBank"]
question_collection = db["questions"]
answered_collection = db["answered_questions"]
feedback_collection = db["feedback_questions"]


def handler(message):
    data = json.loads(message.body)
    res = response_validator(data)
    if not res:  # todo: we are returning true always, is this correct ?
        return True
    print(res)
    # todo: check if the option chose is also correct.

    iter = os.getenv("RETRY_COUNT", 2)
    for iterator in range(iter):
        try:
            query_interface(f"""update mcq.exam_user set marks = marks + 1 where username ='{data['username']}'""")
            return True

        except Exception:
            pass


    # todo: calc the score based on accuracy, if correct then give marks of speed.
    # todo: make a event table, make there success, failed, reason
    # todo: maybe that student was reset so their logs before time should not be entertained.
    # todo: don't capture marks for same question again for that user.

    return True


def get_db_connection():
    conn = psycopg2.connect(
        dbname=Config.POSTGRES['db'],
        user=Config.POSTGRES['user'],
        password=Config.POSTGRES['pw'],
        host=Config.POSTGRES['host'],
        port=Config.POSTGRES['port']
    )
    cursor = conn.cursor()
    cursor.execute(f"SET search_path TO {Config.POSTGRES['schema']};")
    cursor.close()
    return conn


def query_interface(query: str):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(query)

    if query.strip().lower().startswith('select'):
        result = cursor.fetchall()
    else:
        conn.commit()
        result = None

    cursor.close()
    conn.close()
    return result


def response_validator(data: dict):
    """
        Check for valid
        "username",
        "question_id",
        "option_id", and check the
        "time delta"
        if the response is in between the allowed limit.
    """

    username = data.get("username")
    question_id = data["data"].get("question_id")
    option_id = data["data"].get("option")
    answered_timestamps = data.get("timestamp")
    answered_response = answered_collection.find_one({
        "username": username
    })

    # check if valid question id
    question = question_collection.find_one(
        {
            "_id": ObjectId(question_id)
        }
    )

    # handle for invalid answer given
    answer_id = question.get("answer_id")
    if answer_id is not None or answer_id != option_id:
        return True

    if question is None or answered_response is None:
        return True

    else:
        # check for correct option_id
        flag = False
        for options in question["options"]:
            if options["option_id"] == option_id:
                flag = True
                break
        if flag is False:
            return True

    # check for valid time stamp
    for answers in answered_response["answered_ids"]:
        if answers["question_id"] == question_id:
            timedelta = datetime.fromisoformat(answered_timestamps) - answers["timestamp"]
            time_limit = 100

            try:
                time_limit = query_interface('SELECT time_per_question FROM mcq.exam_exam LIMIT 1;')

            except Exception:
                pass

            if timedelta.total_seconds() > time_limit[0][0]:
                return True


reader = nsq.Reader(
    message_handler=handler,
    topic=os.getenv('NSQ_TOPIC'),
    channel=os.getenv('NSQ_CHANNEL'),
    nsqd_tcp_addresses=os.getenv('NSQ_TCP_ADDRESS', '').split(','),
    requeue_delay=0,  # messages that are failed to be re-queued
    max_in_flight=50,  # how many messages can be picked at once
    max_backoff_duration=100,  # if there are many consecutive failures then how much it should wait before
    # again starting processing
    # lookupd_poll_interval=15  # checks for new topics, or producers
)

# Start the IOLoop to handle messages
tornado.ioloop.IOLoop.instance().start()
