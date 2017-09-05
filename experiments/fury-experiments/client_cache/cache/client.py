
from log import Logger
import logging
import tornado.escape
import tornado.ioloop
import tornado.web
import requests
import json

log = Logger('CacheClient', logging.INFO)


class CacheClient(object):


    def __init__(self, endpoint):
        '''
            Initialize CacheClient

                endpoint: endpoint used for cache for make operations getting a enviroment variable in Fury.

        '''
        self.endpoint = endpoint


    def get(self, key):

        '''
            Get data from Cache

                key: key for search a data in KVS Cache

        '''        
        log.stdout("Get")

        try:
            url = self._make_url_endpoint(self.endpoint)
            response = requests.get(url + "/{}".format(key))
            if response.status_code == 200:
                return 'document \n {}{}'.format(key, response.text)
            elif response.status_code == 404:
                return None
            else:
                raise Exception('Could not get data %s from Cache: %s' % (key, response.content), response.status_code)

        except Exception as error:
            log.stdout('Get \n {}'.format(error))            
            raise Exception('Could not get data %s from Cache %s' % (key, response.content), response.status_code)


    def save(self, key, value, headers=None):

        '''
            Save key and value in Cache

                key: key for save a value in Cache.
                value : value for save in Cache.
                headers: max-age, content type, etc.
        '''
        log.stdout("Save")

        data = {
            'key': key,
            'value': json.dumps(value)
        }

        try:
            url = self._make_url_endpoint(self.endpoint)
            response = requests.post(url, data=json.dumps(data), headers=headers)
            if response.status_code != 201:
                raise Exception('Could not save data %s to Cache: %s' % (key, response.content), response.status_code)

        except Exception as error:
            log.stdout('Save \n {}'.format(error))            
            raise Exception('Could not save data %s to Cache: %s' % (key, response.content), response.status_code)


    def update(self, key, value, headers=None):
        '''
            Update key and value in Cache

                key: key for update a value in Cache.
                value : value for update in Cache.
                headers: max-age, content type, etc.
        '''
        log.stdout("Update")

        data = {
            'key': key,
            'value': json.dumps(value)
        }

        try:
            url = self._make_url_endpoint(self.endpoint)
            response = requests.put(url, data=json.dumps(data), headers=headers)
            if response.status_code != 200 and response.status_code != 204:
                raise Exception('Could not update data %s to Cache: %s' % (key, response.content), response.status_code)

        except Exception as error:
            log.stdout('Update \n {}'.format(error))            
            raise Exception('Could not update data %s to Cache: %s' % (key, response.content), response.status_code)


    def delete(self, key):
        '''
            Delete data from Cache

                key: key for search a data in Cache

        '''        
        log.stdout("Delete")

        try:
            url = self._make_url_endpoint(self.endpoint)
            response = requests.delete(url + "/{}".format(key))
            if response.status_code != 204 and response.status_code != 404:
                raise Exception('Could not delete data %s from Cache: %s' % (key, response.content), response.status_code)

        except Exception as error:
            log.stdout('Delete \n {}'.format(error))            
            raise Exception('Could not delete data %s from Cahce: %s' % (key, response.content), response.status_code)


    def _make_url_endpoint(self, endpoint):

        return "http://" + endpoint
