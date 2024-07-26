import os

import nsq
import json
import tornado.ioloop


def handler(message):
    data = json.loads(message.body)
    # maybe that student was reset so their logs before time should not be entertained.

    # check if the student answer between the answer limit of ex. 30sec.
    # fetch the correct answer of this question

    # exam->valid_till don't process score if that was after that.

    # calc the score based on accuracy, if correct then give marks of speed.
    # add this +marks to the student table

    # if student doesn't exist, or question id is incorrect, or time limit exceed, put the data in a mongo collection
    # and continue

    # if the db, code is not able to process the entry then retry.
    return False


reader = nsq.Reader(
    message_handler=handler,
    topic=os.getenv('NSQ_TOPIC'),
    channel=os.getenv('NSQ_CHANNEL'),
    nsqd_tcp_addresses=[os.getenv('NSQ_TCP_ADDRESS')],
    requeue_delay=0,  # messages that are failed to be re-queued
    max_in_flight=50,  # how many messages can be picked at once
    max_backoff_duration=100,  # if there are many consecutive failures then how much it should wait before
    # again starting processing
    # lookupd_poll_interval=15  # checks for new topics, or producers
)

# Start the IOLoop to handle messages
tornado.ioloop.IOLoop.instance().start()
