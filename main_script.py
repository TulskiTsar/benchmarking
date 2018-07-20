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

def worker(qq, thread_number, f, w_q):
    """
    Sends POST requests
    """
    data = {
    'node_id':'00000000-0000-0000-0000-000000002977',
    'session_id':session_id,
    'author': 'sender',
    'thread_number': thread_number,
    'sys_ts': None,
    'seq_number': None,
    'status_code': None,
    'res_ts': None,
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
        status = json.dumps(json.loads(r.content))
        status_code = r.status_code
        data['status_code'] = status_code
        # # print(status_code)
        #
        # # Log the status codes of the requests
        # # f.write(status + ' ' + str(status_code) + "\n")
        # f.write(
        # "Request: " + "[" + str(thread_number) + "]" + str(item)+" "+ str(r) + "\n"
        # )
        w_q.put(data)


def sender(number_req, q, w_q):
    """
    Starts specified number of threads for POST request
    """
    qq = queue.Queue()
    threads = []
    messages_sent = 0
    thread_no = 5
    f = open("Status Logs.txt", "w+")
    for i in range(thread_no):
        t = threading.Thread(target=worker, args=(qq,i,f,w_q))
        t.start()
        threads.append(t)

    for i in range(number_req):
        qq.put(i)
        messages_sent += 1

    print("%% Messages sent: {}".format(messages_sent))
    # c_q.put(('sender',messages_sent))

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
    running = True
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


            if messages_received == no_requests:
                print("%% Messages received: {}".format(messages_received))
                # c_q.put(('reciever', messages_received))
                break
            # q.put(msg.value())
            w_q.put(msg_dict)
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
    while not w_q.empty():
        get_dict = w_q.get()

        if 'sender' == get_dict.get('author'):
            f_send.write(str(get_dict))
        else:
            f_rec.write(str(get_dict))

# Basic killer function
# def killer(c_q, k_q):
#     check_dict = dict()
#     while True:
#         item = c_q.get()
#         check_dict[item[0]] = item[1]
#
#         if len(check_dict) == 2:
#             break
#
#     if check_dict.get('sender') == check_dict.get('receiver'):
#         print("%% All items sent and recieved")
#         k_q.put("Kill")


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

    # p_killer = multiprocessing.Process(
    #                             target=killer,
    #                             args=()
    #                             )


    # p_killer.start()
    p_receiver.start()
    p_sender.start()

    p_receiver.join()
    p_sender.join()
    # p_killer.join()
    reader(write_q)
