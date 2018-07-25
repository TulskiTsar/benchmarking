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
from collections import defaultdict
import numpy as np
import pandas as pd
# from matplotlib import pyplot as pet


def id_generator(size=10, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.SystemRandom().choice(chars) for _ in range(size))

def worker(qq, thread_number, write_queue):
    """
    Sends POST requests
    """
    timeout = 0.01


    while True:
        seq_number = qq.get()
        data = {
        'node_id':'00000000-0000-0000-0000-000000002977',
        'session_id':session_id,
        'thread_number': thread_number,
        }

        if seq_number == None:
            break

        data['seq_number'] = seq_number
        data['sys_ts'] = int(round(time.time() * 1000))
        r = requests.post(url, json = data)
        data['res_ts'] = int(round(time.time() * 1000))
        data['elapsed_time'] = r.elapsed.total_seconds()
        # print(data)
        write_queue.put(data)


def sender(number_req, q, write_queue):
    """
    Starts specified number of threads for POST request
    """
    qq = queue.Queue()
    threads = []
    messages_sent = 0
    thread_no = 5

    for i in range(number_req):
        qq.put(i)
        messages_sent += 1

    for i in range(thread_no):
        # Starts worker thread to send POST requests
        t = threading.Thread(target=worker, args=(qq,i,write_queue))
        t.start()
        threads.append(t)


    print("%% Messages sent: {}".format(messages_sent))

    for _ in range(thread_no):
        qq.put(None)

    for t in threads:
        t.join()

def receiver(q, l, no_requests, write_queue):

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

            elif msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(
                    '%% Reached end of topic {} [{}] at offset {}\n'.format(
                                    msg.topic(), msg.partition(), msg.offset())
                                    )
                    continue
                else:
                    raise KafkaException(msg.error())
                    break
            kafka_test = int(round(time.time() * 1000))
            kafka_ts = msg.timestamp()[1]
            msg_load = json.loads(msg.value())

            if not msg_load.get('session_id') == session_id:
                print("Session ID mismatch")
                continue

            tm_msg = msg_load['sys_ts']
            tm_tot = tm_msg + tm_out
            messages_received += 1
            msg_load['kafka_ts'] = kafka_ts
            msg_load['kafka_test'] = kafka_test
            # print(msg_load)
            write_queue.put(msg_load)

            if messages_received == no_requests:
                print("%% Messages received: {}".format(messages_received))
                write_queue.put(None)
                print("%% Stop message sent")
                break

    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')

    finally:
        print("%% Closing consumer \n")
        c.close()
        print("%% Consumer closed \n")

def reader(write_queue):
    """
    Reads queue and divide information
    """
    print("%% Reader initialised")
    context = defaultdict(dict)
    with open("Sent Requests.txt", "w+") as f_send, \
        open("Received Requests.txt", "w+") as f_rec:
        while True:
            item = write_queue.get()
            print("%% Listening")
            if item == None:
                print("%% Reader terminated")
                break
            # print(item)
            thread_number = item.get('thread_number')
            seq_number = item.get('seq_number')
            request_id = "{}[{}]".format(thread_number, seq_number)

            if request_id not in context:
                context[request_id] = item
            else:
                context[request_id].update(item)


    return context
    # for key in send_d:
    #     if key in recv_d:
    #         print(key, recv_d[key])
if __name__ == "__main__":
    TOPIC = "bar"
    BROKER = "kafka:9092"
    GROUP = "foo"
    session_id = id_generator(5)
    url = "http://localhost:8080/data/{}".format(TOPIC)
    no_requests = 148
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

    p_sender.join()
    print("%%%%%%%%%%%%%%%%% Sender process terminated   %%%%%%%%%%%%%%%%%%%")
    p_receiver.join()
    print("%%%%%%%%%%%%%%%%% Receiver process terminated %%%%%%%%%%%%%%%%%%%")

    context = reader(write_q)

    sys_ts_list = []
    res_ts_list = []
    cloud_ts_list = []
    request_list = []
    kafka_ts_list = []
    kafka_test_list = []
    elapsed_list = []

    # print(context)

    for key, dicto in context.items():
        request_list.append(key)
        sys_ts_list.append(dicto['sys_ts'])
        res_ts_list.append(dicto['res_ts'])
        cloud_ts_list.append(dicto['cloud_ts'])
        kafka_ts_list.append(dicto['kafka_ts'])
        kafka_test_list.append(dicto['kafka_test'])
        elapsed_list.append(dicto['elapsed_time'])

        # print(type(context[key]))
        # for key, value in context[key].items():
        #     print(key)
        #     print(value)
        # print(dicto['kafka_ts'])
        # print(response_time)


    # for key, value in context.items():
        # print(key)
        # print(value)

    # print(str(res_ts_list) + "\n")
    # print(str(sys_ts_list) + "\n")
    # print(str(cloud_ts_list) + "\n")
    # print(str(kafka_ts_list) + "\n")


    request_array = np.array(request_list)

    sys_ts_array = np.array(sys_ts_list)
    res_ts_array = np.array(res_ts_list)
    cloud_ts_array = np.array(cloud_ts_list)
    kafka_ts_array = np.array(kafka_ts_list)
    kafka_test_array = np.array(kafka_test_list)
    elapsed_array = np.array(elapsed_list)

    # print(cloud_ts_array)
    # print(sys_ts_array)

    elapsed_array = elapsed_array * 1000

    response_time_array = res_ts_array - sys_ts_array
    gobbler_time_array = cloud_ts_array - sys_ts_array
    kafka_time_array = kafka_ts_array - cloud_ts_array
    kafka_send_array = kafka_ts_array - sys_ts_array
    kafka_send_test_array = kafka_test_array - sys_ts_array

    response_time_median = np.median(response_time_array)
    gobbler_time_median = np.median(gobbler_time_array)
    kafka_time_median = np.median(kafka_time_array)
    kafka_send_median = np.median(kafka_send_array)
    kafka_send_test_median = np.median(kafka_send_test_array)
    elapsed_median = np.median(elapsed_array)

    with open("Metrics.txt", "w+") as fopen:
        fopen.write("Median response time: {}\n".format(response_time_median))
        fopen.write("Median elapsed time: {}\n".format(elapsed_median))
        fopen.write("Median script -> gobbler time: {}\n".format(gobbler_time_median))
        fopen.write("Median gobbler -> kafka time: {}\n".format(kafka_time_median))
        fopen.write("Median script -> kafka time: {}\n".format(kafka_send_median))
        fopen.write("Median kafka send test time: {}\n".format(kafka_send_test_median))



    # plt.scatter(request_array, response_array)
        # print(key)
        # print(value)
