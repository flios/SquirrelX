import tweepy
import Queue
import datetime
import json
import numpy as np
import pandas as pd
import threading
import time
from tweepy import Status
import logging

class DataWriter:
    def __init__(self):
        self.buff_q = Queue.Queue()
        self.is_running = False
    def save_data(self, limit_num = 100):
        while self.is_running:
            if self.buff_q.qsize() > (limit_num + 10):
                filename = datetime.datetime.now().strftime('%Y%m%d%H%M%S') + ".json"
                with open(filename,'w') as output_file:
                    for i in range(limit_num):
                        raw_data = self.buff_q.get()
                        output_file.write(raw_data)

    def running(self, limit_num):
        if not self.is_running:
            self.is_running = True
            self.writting_thread = threading.Thread(target=self.save_data, args=(limit_num,))
            self.writting_thread.start()
        else:
            print "Writer is running!"

    def stop(self):
        self.is_running = False
        # while self.writting_thread.is_alive():
        #     self.writting_thread.join()

class SquirrelStreamListener(tweepy.StreamListener):
    def __init__(self, buff_queue = None):
        if buff_queue == None:
            self.data_writer = DataWriter()
            self.buff_q = self.data_writer.buff_q
        else:
            self.buff_q = buff_queue
        super(SquirrelStreamListener, self).__init__()
    def on_data(self, raw_data):
        """Called when raw data is received from connection.

        Override this method if you wish to manually handle
        the stream data. Return False to stop stream and close connection.
        """
        data = json.loads(raw_data)

        if 'in_reply_to_status_id' in data:
            status = Status.parse(self.api, data)
            if self.on_status(status) is False:
                return False
            self.buff_q.put(raw_data)
            # print self.buff_q.qsize()
            # print data
        elif 'delete' in data:
            delete = data['delete']['status']
            if self.on_delete(delete['id'], delete['user_id']) is False:
                return False
        elif 'event' in data:
            status = Status.parse(self.api, data)
            if self.on_event(status) is False:
                return False
        elif 'direct_message' in data:
            status = Status.parse(self.api, data)
            if self.on_direct_message(status) is False:
                return False
        elif 'friends' in data:
            if self.on_friends(data['friends']) is False:
                return False
        elif 'limit' in data:
            if self.on_limit(data['limit']['track']) is False:
                return False
        elif 'disconnect' in data:
            if self.on_disconnect(data['disconnect']) is False:
                return False
        elif 'warning' in data:
            if self.on_warning(data['warning']) is False:
                return False
        else:
            logging.error("Unknown message type: " + str(raw_data))
    def on_status(self,status):
        # print status.text
        pass

if __name__ == '__main__':
    import twitter_setup
    test_que = Queue.Queue()
    my_api = twitter_setup.twitter_setup()
    my_listener = SquirrelStreamListener(test_que)
    my_stream = tweepy.Stream(auth=my_api.auth, listener=my_listener)
    my_stream.filter(track=['bitcoin', 'btc'], languages=['en'], async=True)
    # my_listener.data_writer.running(100)

    time.sleep(10)
    tweet_num = test_que.qsize()
    for i in range(tweet_num):
        raw_data = test_que.get()
        temp_df = pd.read_json(raw_data)
        print temp_df


    my_stream.disconnect()
    # my_listener.data_writer.stop()
    print test_que.qsize()
