import requests
import time
import multiprocessing
import threading
from confluent_kafka import Consumer, KafkaException, KafkaError
import sys
import queue
from multiprocessing.dummy import Pool




def sender(url, data_data, headers_headers, number_req, q):
    """
    Posts a request to a specified URL a number of times
    """
    requests_sent = []
    for _ in range(number_req):
        data_data['sys_ts'] = time.time()
        t = threading.Thread(target=requests.post, args=(url,data_data,headers_headers))
        t.start()

    # requests_sent = []
    # for number in range(number_req):
    #     data_data['sys_ts'] = time.time()
    #     request = requests.post(url, json = data_data, headers = headers_headers)
    #     requests_sent.append(request.json())
    #
    # return requests_sent

def receiver(q):
    """
    Sets up the Kafka listener to handle requests
    """
    conf = {'bootstrap.servers': BROKER, 'group.id': GROUP, 'session.timeout.ms': 6000,
            'default.topic.config': {'auto.offset.reset': 'smallest'}}
    c = Consumer(conf)
    running = True

    try:
        c.subscribe([TOPIC])
        while running:
            msg = c.poll(timeout=1.0)

            if msg is None: continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% Reached end of topic {0} [{1}] at offset {2}\n'.format(
                                    msg.topic(), msg.partition(), msg.offset()))
                    break
                else:
                    raise KafkaException(msg.error())

            msgv = msg.value()
            print(msgv)
            q.put(msgv)

    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')

    finally:
        print("%% Exiting")
        c.close()

def reader(q):
    reader_list = []
    while not q.empty():
        test_get = q.get()
        print("Received: {}.".format(test_get))
        reader_list.append(test_get)
    return reader_list

if __name__ == "__main__":
    TOPIC = "bar"
    BROKER = "kafka:9092"
    GROUP = "foo"
    url = "http://localhost:8080/data/{}".format(TOPIC)
    data = {'node_id':'00000000-0000-0000-0000-000000002977'}
    headers = {'Content-type': 'application/json'}
    q = multiprocessing.Queue()

    p_receiver = multiprocessing.Process(target=receiver, args=(q,))
    p_sender = multiprocessing.Process(target=sender, args=(url, data, headers, 10, q))

    p_receiver.start()
    p_sender.start()
    p_receiver.join()
    p_sender.join()

    items_received = len(reader(q))
    print("No. items received from Kafka: {}.".format(items_received))
