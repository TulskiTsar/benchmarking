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


def id_generator(
        size=9, chars=string.ascii_uppercase + string.ascii_lowercase +
        string.digits):
    return ''.join(random.SystemRandom().choice(chars) for _ in range(size))


def requester(url, data, headers):
    r = requests.post(url, json = data, headers=headers)
    return r.content


def sender(url, data, headers, number_req, q):
    """
    Posts specified number of requests on "separate" threads
    """
    data['author'] = "sender"
    messages_sent = 0
    for idx, number in enumerate(range(number_req),1):
        data['sys_ts'] = time.time()
        data['uniq_id'] = id_generator()
        t = threading.Thread(target=requester, args=(url,data,headers))
        t.start()
        # print("Sent message: {}\n".format(data))
        messages_sent += 1
        q.put(data)
    print("Messages sent: {}".format(messages_sent))
    return messages_sent   


def receiver(q,l):
    """
    Sets up the Kafka listener to handle requests
    """
    l.acquire()
    conf = {'bootstrap.servers': BROKER, 'group.id': GROUP, 'session.timeout.ms': 6000,
            'default.topic.config': {'auto.offset.reset': 'largest'}}
    c = Consumer(conf)
    running = True
    l.release()

    try:
        c.subscribe([TOPIC])
        tm_out = 0.5
        tm_cur = time.time()
        tm_tot = tm_cur + tm_out
        messages_received = 0
        while running:
            msg = c.poll(timeout=1.0)

            if msg is None:
                tm_none = time.time()

                if tm_none > tm_tot:
                    print("%% No message received for {} seconds".format(
                        str(tm_out)))
                    break
                else:continue


            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write(
                            '%% Reached end of topic {} [{}] at offset {}\n'.format(
                                    msg.topic(), msg.partition(), msg.offset())
                            )
                    continue
                else:
                    raise KafkaException(msg.error())
                    break
            msg_dict = json.loads(msg.value())
            # Don't add new entry to dictionary, create tuple
            if not msg_dict.get('session_id') == session_id:
                # session_id == msg_dict.get('session_id'):
                sys.stderr.write('%% Session ID mismatch: ignored')
                continue
            msg_dict['author'] = "receiver"
            tm_msg = msg_dict['sys_ts']
            tm_tot = tm_msg + tm_out
            messages_received += 1
            q.put(msg_dict)

    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')

    finally:
        print("Messages received: {}".format(messages_received))
        print("%% Closing consumer\n")
        c.close()


def reader(q):
    """
    Reads the queue and sends information to relevant list according to author
    of information
    """

    sender_list = []
    receiver_list = []
    while not q.empty():
        get_dict = q.get()

        if 'sender' == get_dict.get('author'):
            sender_list.append(get_dict)
        else:
            receiver_list.append(get_dict)

    return sender_list, receiver_list 

def validator(list1, list2):
    """ 
    Validates that all sent messages have been received
    """

    for item in list1:
        if 'uniq_id' in item.keys():
           uniq_id = item['uniq_id']
           if (item['uniq_id'] == uniq_id for item in list2): 
               print("Message ID:{} sent - received".format(uniq_id))
           else:
               print("Message ID:{} sent - not received".format(uniq_id))
        


if __name__ == "__main__":
    print("")
    TOPIC = "bar"
    BROKER = "kafka:9092"
    GROUP = "foo"
    session_id = id_generator(5)


    url = "http://localhost:8080/data/{}".format(TOPIC)
    data = {
           'node_id':'00000000-0000-0000-0000-000000002977',
           'session_id':session_id
           }
    headers = {'Content-type': 'application/json'}
    quantity = 2

    lock = multiprocessing.Lock()
    q = multiprocessing.Queue()
    p_receiver = multiprocessing.Process(target=receiver, args=(q,lock))
    p_sender = multiprocessing.Process(
            target=sender, args=(url, data, headers, quantity, q)
            )
    print("Session ID: {}\n".format(session_id))
    p_receiver.start()
    p_sender.start()
    p_receiver.join()
    p_sender.join()
    # p_receiver.join()
    # p_sender.join()
    sender_list, receiver_list = reader(q)
    validator(sender_list, receiver_list)
