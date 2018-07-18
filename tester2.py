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

    for idx, number in enumerate(range(number_req),1):
        data['sys_ts'] = time.time()
        data['uniq_id'] = id_generator()
        t = threading.Thread(target=requester, args=(url,data,headers))
        t.start()
        # print("Sent message: {}\n".format(data))
        q.put(data)


def receiver(q):
    """
    Sets up the Kafka listener to handle requests
    """

    conf = {'bootstrap.servers': BROKER, 'group.id': GROUP, 'session.timeout.ms': 6000,
            'default.topic.config': {'auto.offset.reset': 'largest'}}
    c = Consumer(conf)
    running = True


    try:
        c.subscribe([TOPIC])
        tm_out = 0.5
        tm_cur = time.time()
        tm_tot = tm_cur + tm_out
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
                    sys.stderr.write('%% Reached end of topic {0} [{1}] at offset {2}\n'.format(
                                    msg.topic(), msg.partition(), msg.offset()))
                    continue
                else:
                    raise KafkaException(msg.error())
                    break
            res_ts = time.time()
            msg_decode = msg.value().decode('utf-8')
            msg_dict = json.loads(msg.value())
            msg_dict['author'] = "receiver"
            msg_dict['res_ts'] = res_ts
            # print('Received message: {}\n'.format(msg_dict))
            tm_msg = msg_dict['sys_ts']
            tm_tot = tm_msg + tm_out
            q.put(msg_dict)

    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')

    finally:
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

        if 'sender' in get_dict.values():
            sender_list.append(get_dict)
        if 'receiver' in get_dict.values():
            receiver_list.append(get_dict)
        else:
            print("%% No message author detected")

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
    url = "http://localhost:8080/data/{}".format(TOPIC)
    data = {'node_id':'00000000-0000-0000-0000-000000002977'}
    headers = {'Content-type': 'application/json'}
    q = multiprocessing.Queue()
    p_receiver = multiprocessing.Process(target=receiver, args=(q,))
    p_sender = multiprocessing.Process(
            target=sender, args=(url, data, headers, 2, q)
            )

    p_receiver.start()
    p_sender.start()
    p_receiver.join()
    p_sender.join()
    # p_receiver.join()
    # p_sender.join()
    sender_list, receiver_list = reader(q)
    validator(sender_list, receiver_list)
