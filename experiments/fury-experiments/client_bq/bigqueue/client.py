from log import Logger
import logging
import tornado.escape
import tornado.ioloop
import tornado.web
import requests
import json

log = Logger('BigQueueClient', logging.INFO)


class BigQueueClient():


    def __init__(self, bq_endpoint, topic_name, message):
        '''
            Initialize BigQueueClient

                bq_endpoint: endpoint used for bigqueue for make operations.
                topic_name: topic name for bigqueue.
                message: message for storage in bigqueue.
        '''        
        self.bq_endpoint = bq_endpoint
        self.topic_name = topic_name
        self.message = message


    def publish(self):
        '''
            Publish a message in BigQueue

                message: message for publish in BigQueue.
                topic_name: name of topic for publish message.
        '''
        log.stdout("Publish")

        content = {
            "msg": self.message, 
            "topics": [self.topic_name]
        }

        endpoint = self._make_url_endpoint(self.bq_endpoint)
        try:
            response = requests.post(endpoint, data=json.dumps(content), headers={'content-type': 'application/json'})
            if response.status_code != 201:
                raise Exception('Could not publish messagem in BigQueue: %s' % response.content, response.status_code)

        except Exception as error:
            log.stdout('Publish \n {}'.format(error))            
            raise Exception('Could not publish message %s in BigQueue: %s' % (response.content), response.status_code)

    def _make_url_endpoint(self, endpoint):

        return endpoint + '/messages'

