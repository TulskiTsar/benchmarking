import requests
import time
import multiprocessing
import threading
from confluent_kafka import Consumer, KafkaException, KafkaError
import sys
import queue
import json
import string
import random
import concurrent.futures

def id_generator(size=10, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.SystemRandom().choice(chars) for _ in range(size))

# def requester(url, data, headers):
#     r = requests.post(url, json = data, headers=headers)
#     return r

def worker(qq, thread_number):
    data = {
    'node_id':'00000000-0000-0000-0000-000000002977',
    'session_id':session_id,
    'author': 'sender',
    'thread_number': thread_number,
    'sys_ts': None,
    'seq_number': None
    }
    while True:
        item = qq.get()

        if item is None:
            break

        data['seq_number'] = item
        data['sys_ts'] = time.time()
        requests.post(url, json = data)

def sender(number_req, q, send_q):
    # """
    # Posts specified number of requests on "separate" threads
    # """

    qq = queue.Queue()
    threads = []

    thread_no = 200
    for i in range(thread_no):
        t = threading.Thread(target=worker, args=(qq,i))
        t.start()
        threads.append(t)

    for i in range(number_req):
        qq.put(i)

    for _ in range(thread_no):
        qq.put(None)

    for t in threads:
        t.join()

def receiver(q):

    """
    Kafka listener for incoming requests
    """

    conf = {
    'bootstrap.servers':BROKER,
    'group.id':GROUP,
    'session.timeout.ms':6000,
    'default.topic.config':{'auto.offset.reset':'smallest'}
    }
    c = Consumer(conf)

    try:
        c.subscribe([TOPIC])
        tm_out = 5
        tm_cur = time.time()
        tm_tot = tm_cur + tm_out
        messages_received = 0
        while True:
            msg = c.poll(timeout=1.0)

            if msg is None:
                tm_none = time.time()

                if tm_none > tm_tot:
                    print(
                    '%% No message recieved for {} seconds.'.format(tm_out)
                    )
                    break

                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write(
                    '%% Reached end of topic {0} [{1}] at offset {2}\n'.format(
                                    msg.topic(), msg.partition(), msg.offset())
                                    )
                    continue
                else:
                    raise KafkaException(msg.error())
                    break

            msg_dict = json.loads(msg.value())
            if not msg_dict.get('session_id') == session_id:
                print("Session ID mismatch")
                continue

            msg_dict['author'] = "receiver"
            tm_msg = msg_dict['sys_ts']
            tm_tot = tm_msg + tm_out
            messages_received += 1
            q.put(msg.value())
        print("Messages received: {}".format(messages_received))

    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')

    finally:
        print("%% Closing consumer \n")
        c.close()

# def reader(q):
#     """
#     Reads queue and divide information
#     """
#     sender_list = []
#     receiver_list = []
#     while not q.empty():
#         get_dict = q.get()
#
#         if 'sender' == get_dict.get('author'):
#             sender_list.append(get_dict)
#         else:
#             receiver_list.append(get_dict)
#
#     return sender_list, receiver_list

if __name__ == "__main__":
    lock = multiprocessing.Lock()
    TOPIC = "bar"
    BROKER = "kafka:9092"
    GROUP = "foo"
    session_id = id_generator(5)
    url = "http://localhost:8080/data/{}".format(TOPIC)
    no_requests = 100

    q = multiprocessing.Queue()
    send_q = multiprocessing.Queue()

    p_receiver = multiprocessing.Process(
                                target=receiver,
                                args=(q,)
                                )

    p_sender = multiprocessing.Process(
                                target=sender,
                                args=(no_requests, q, send_q)
                                )
    p_receiver.start()
    p_sender.start()
    p_receiver.join()
    p_sender.join()
