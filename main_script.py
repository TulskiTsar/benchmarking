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

def worker(qq, thread_number, w_q):
    """
    Sends POST requests
    """
    data = {
    'node_id':'00000000-0000-0000-0000-000000002977',
    'session_id':session_id,
    'thread_number': thread_number,
    'sys_ts': None,
    'seq_number': None,
    'status_code': None,
    'res_ts': None,
    'author': 'sender',
    }

    while True:
        item = qq.get()

        if item is None:
            break

        data['seq_number'] = item
        data['sys_ts'] = time.time()
        r = requests.post(url, json = data)

        data['res_ts'] = time.time()
        response_time = data.get('res_ts') - data.get('sys_ts')
        data['response_time'] = response_time
        status_code = r.status_code
        data['status_code'] = status_code

        # msg_auth_s = (0, data)
        # w_q.put(msg_auth_s)
        w_q.put(data)

def sender(number_req, q, w_q):
    """
    Starts specified number of threads for POST request
    """
    qq = queue.Queue()
    threads = []
    messages_sent = 0
    thread_no = 5

    for i in range(thread_no):
        t = threading.Thread(target=worker, args=(qq,i,w_q))
        t.start()
        threads.append(t)

    for i in range(number_req):
        qq.put(i)
        messages_sent += 1

    print("%% Messages sent: {}".format(messages_sent))

    for _ in range(thread_no):
        qq.put(None)

    for t in threads:
        t.join()

def receiver(q, l, no_requests, w_q):

    """
    Kafka listener for incoming requests
    """
    l.acquire()
    print("%% Lock acquired")
    conf = {
    'bootstrap.servers':BROKER,
    'group.id':GROUP,
    'session.timeout.ms':6000,
    'default.topic.config':{'auto.offset.reset':'smallest'},
    }
    c = Consumer(conf)

    try:
        c.subscribe([TOPIC])
        tm_out = 5
        tm_cur = time.time()
        tm_tot = tm_cur + tm_out
        messages_received = 0
        l.release()
        print("%% Lock released")
        while True:

            msg = c.poll(timeout=1.0)

            if msg is None:
                tm_none = time.time()

                if tm_none > tm_tot:
                    print("%% Messages received: {}".format(messages_received))
                    print(
                    '%% No message recieved for {} seconds.'.format(tm_out)
                    )
                    break
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(
                    '%% Reached end of topic {} [{}] at offset {}\n'.format(
                                    msg.topic(), msg.partition(), msg.offset())
                                    )
                    continue
                else:
                    raise KafkaException(msg.error())
                    break
            msg_load = json.loads(msg.value())

            if not msg_load.get('session_id') == session_id:
                print("Session ID mismatch")
                continue

            tm_msg = msg_load['sys_ts']
            tm_tot = tm_msg + tm_out
            messages_received += 1

            if messages_received == no_requests:
                print("%% Messages received: {}".format(messages_received))
                # c_q.put(('reciever', messages_received))
                break
            msg_load['author'] = 'receiver'
            # msg_auth_r = (1, msg_load)
            # w_q.put(msg_auth_r)
            w_q.put(msg_load)

    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')

    finally:
        print("%% Closing consumer \n")
        c.close()

def reader(w_q):
    """
    Reads queue and divide information
    """
    f_send = open("Sent Requests.txt", "w+")
    f_rec = open("Received Requests.txt", "w+")
    # while not w_q.empty():
    #     get_dict = w_q.get()
    #
    #     if get_dict[0] == 0:
    #         f_send.write("\n" + str(get_dict[1]) + "\n")
    #     else:
    #         f_rec.write("\n" + str(get_dict[1]) + "\n")
    while not w_q.empty():
        get_dict = w_q.get()

        if get_dict['author'] == 'sender':
            f_send.write("\n" + str(get_dict) + "\n")
        else:
            f_rec.write("\n" + str(get_dict) + "\n")

if __name__ == "__main__":
    TOPIC = "bar"
    BROKER = "kafka:9092"
    GROUP = "foo"
    session_id = id_generator(5)
    url = "http://localhost:8080/data/{}".format(TOPIC)
    no_requests = 20
    l = multiprocessing.Lock()
    q = multiprocessing.Queue()
    write_q = multiprocessing.Queue()

    p_receiver = multiprocessing.Process(
                                target=receiver,
                                args=(q, l, no_requests, write_q),
                                )

    p_sender = multiprocessing.Process(
                                target=sender,
                                args=(no_requests, q, write_q),
                                )
    p_receiver.start()
    p_sender.start()

    p_receiver.join()
    p_sender.join()
    reader(write_q)
