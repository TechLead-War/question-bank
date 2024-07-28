import json
import threading
from datetime import datetime

import nsq
from bson import ObjectId
from flask import Flask, jsonify, request
from pymongo import MongoClient, ASCENDING
import os
from redis import Redis
import tornado.ioloop
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError, BulkWriteError, WriteError, \
    WriteConcernError, DuplicateKeyError


app = Flask(__name__)

# MongoDB setup
MONGO_DB_URI = os.getenv("MONGO_DB_URI", "mongodb://localhost:27017")
mongo_client = MongoClient(
    MONGO_DB_URI,
    maxPoolSize=200,
    minPoolSize=10
)

db = mongo_client["questionBank"]
collection = db["questions"]
answered_collection = db["answered_questions"]
feedback_collection = db["feedback_questions"]
collection.create_index([("question_id", ASCENDING)], unique=True)


# Redis setup
redis_client = Redis(
    host=os.getenv("REDIS_HOST"),
    port=os.getenv("REDIS_PORT"),
    password=os.getenv("REDIS_PASSWORD"),
    db=0
)

# NSQ setup
NSQ_HOST = os.getenv(
    'NSQ_HOSTS',
    '127.0.0.1:4150,'
    '127.0.0.1:4152,'
    '127.0.0.1:4154'
)  # this should be TCP port

NSQ_TOPIC = os.getenv('NSQ_TOPIC', '')
nsq_hosts_list = NSQ_HOST.split(',')
nsq_writer = nsq.Writer(nsq_hosts_list)


# Create a new instance of Tornado's IOLoop
ioloop = tornado.ioloop.IOLoop.instance()
ioloop_thread = threading.Thread(target=lambda: ioloop.start())
ioloop_thread.daemon = True
ioloop_thread.start()


@app.route('/question', methods=['GET'])
def get_question():
    # fetch question
    auth_header = request.headers.get('Authorization')

    if auth_header and auth_header.split(" ")[1] == os.environ.get("ADMIN_TOKEN"):
        try:
            username = request.args.get('username')
            question_limit = int(request.args.get('question_limit'))

        except Exception as e:
            return jsonify({"error": str(e)})

        # Check if username is provided
        if not username:
            return jsonify({'error': 'username not provided'}), 400

        # Fetch a question that the university_id hasn't answered
        question = fetch_unanswered_question(username, question_limit)

        if question:
            if question.get("error"):
                return jsonify(question), 409

            # Record that this university_id has answered this question
            record_answered_question(
                username,
                question['question_id']
            )
            return jsonify(question)

        else:
            return jsonify({'error': 'No unanswered questions available at the moment'}), 404

    else:
        return jsonify({'error': 'Unauthorized access'}), 401


def fetch_unanswered_question(username: str, question_limit: int):
    # Fetch a question that the university_id hasn't answered
    try:
        answered_questions = answered_collection.find_one({'username': username})
        answered_ids = answered_questions['answered_ids'] if answered_questions else []

        if len(answered_ids) >= question_limit:
            return {
                "error": "Questions limit reached",
                "status": 409
            }
        # Get a random question that hasn't been answered by this university_id
        answered_ids = [ObjectId(answered_id.get("question_id")) for answered_id in answered_ids]
        question = collection.find_one(
            {
                '_id': {'$nin': answered_ids},
                'test_id': username.split('_')[0] + '_'
            }
        )

        if question:
            try:
                return {
                    'question_id': str(question['_id']),
                    'text': question['question_text'],
                    'options': question['options']
                }

            except Exception:
                pass

    except KeyError:
        return {}

    return None


def record_answered_question(username, question_id):
    # Record that this university_id has answered this question
    answered_questions = answered_collection.find_one({'username': username})
    if answered_questions:
        answered_ids = answered_questions['answered_ids']
        if not any(answer['question_id'] == question_id for answer in answered_ids):
            answered_ids.append({
                'question_id': question_id,
                'timestamp': datetime.utcnow()
            })
            answered_collection.update_one(
                {'username': username},
                {'$set': {'answered_ids': answered_ids}}
            )
    else:
        answered_collection.insert_one({
            'username': username,
            'answered_ids': [{
                'question_id': question_id,
                'timestamp': datetime.utcnow()
            }]
        })


def validate_question(question):
    if "question_text" not in question or not question["question_text"]:
        return False, "Question text is missing or empty."
    if "options" not in question or len(question["options"]) != 4:
        return False, "Options should have exactly 4 elements."
    if "test_id" not in question or not question["test_id"]:
        return False, "Test_id is missing or empty."
    if "question_id" not in question or not question["question_id"]:
        return False, "Test_id is missing or empty."

    return True, ""


@app.route('/question/add', methods=['POST'])
def add_question():
    auth_header = request.headers.get('Authorization')
    if auth_header and auth_header.split(" ")[1] == os.environ.get("ADMIN_TOKEN"):
        request_data = request.get_json()
        successful_inserts = []
        failed_inserts = []

        for question in request_data:
            is_valid, error_message = validate_question(question)
            if is_valid:
                try:
                    result = collection.insert_one(question)
                    question["_id"] = str(question["_id"])
                    successful_inserts.append({
                        "question_id": str(result.inserted_id),  # Convert ObjectId to string
                        "question": question
                    })

                except DuplicateKeyError as e:
                    failed_inserts.append({
                        "error": "Question already exists",
                        "status_code": 400
                    })

            else:
                failed_inserts.append({
                    "question": question,
                    "error": error_message
                })

        if failed_inserts:
            response = {
                "message": "Some questions were not added successfully",
                "successful_inserts": successful_inserts,
                "failed_inserts": failed_inserts
            }
            return jsonify(response), 207

        else:
            return jsonify(
                {
                    'message': 'All questions added successfully'}
            ), 201

    else:
        return jsonify({'error': 'Unauthorized access'}), 401


@app.route('/answer/submit', methods=['POST'])
def capture_response_question():
    # don't process if timestamp says it older than 30 sec.
    request_data = request.get_json()
    username = request_data.get('username')

    data = {
        "question_id": request_data.get("question_id"),
        "option": request_data.get("option")
    }
    timestamp = datetime.utcnow().isoformat()

    if not username or not data:
        return jsonify(
            {'error': 'Missing required fields'}
        ), 400

    message = {
        'username': username,
        'data': data,
        'timestamp': timestamp
    }

    # Publish the message to NSQ
    try:
        # Publish the message to NSQ
        ioloop.add_callback(
            nsq_writer.pub,
            NSQ_TOPIC,
            json.dumps(message).encode('utf-8'),
            pub_callback
        )
        return jsonify(
            {
                'message': 'Answer submitted successfully',
                'status': 'success'
            }
        ), 200

    except Exception:
        return jsonify(
            {
                'error': 'Failed to submit answer',
                'status': 'failure'
            }
        ), 500


def pub_callback(conn, data):
    pass


@app.route('/submit/feedback', methods=['POST'])
def capture_feedback_question():
    auth_header = request.headers.get('Authorization')
    if auth_header and auth_header.split(" ")[1] == os.environ.get("ADMIN_TOKEN"):
        try:
            feedback_collection.insert_one(request.get_json())
            return {
                "status": "success",
                "message": "Feedback submitted successfully"
            }

        except (
                ConnectionFailure,
                ServerSelectionTimeoutError,
                BulkWriteError,
                WriteError,
                WriteConcernError
        ) as e:
            return {
                "status": "failed",
                "message": "Invalid",
                "error": str(e)
            }

        except (TypeError, ValueError, AttributeError) as e:
            return {
                "status": "failed",
                "message": "Invalid",
                "error": str(e)
            }

    else:
        return {
            "status": "failed",
            "message": "Invalid",
        }


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5012, debug=True)
