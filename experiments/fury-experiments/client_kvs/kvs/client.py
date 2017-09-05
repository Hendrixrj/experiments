
from log import Logger
import logging
import tornado.escape
import tornado.ioloop
import tornado.web
import requests
import json

log = Logger('KVSClient', logging.INFO)


class KVSClient(object):


    def __init__(self, endpoint_write, endpoint_read, container_name, application_name):
        '''
            Initialize KVSClient

                endpoint_write: endpoint used for kvs for write operations.
                endpoint_read: endpoint used for kvs for read operations.
                container_name: name used for kvs build uri for endpoints
                application_name: name used for kvs build uri for endpoints

        '''
        self.container_name = container_name
        self.application_name = application_name
        self.endpoint_write = endpoint_write
        self.endpoint_read = endpoint_read


    def get(self, key):

        '''
            Get document from KVS

                key: key for search a document in KVS

        '''        
        log.stdout("Get")

        try:
            url = self._make_url_endpoint(self.endpoint_read)
            response = requests.get(url + "/{}".format(key))
            if response.status_code == 200:
                return 'document \n {}{}'.format(key, response.text)
            elif response.status_code == 404:
                return None
            else:
                raise Exception('Could not get document %s from KVS: %s' % (key, response.content), response.status_code)

        except Exception as error:
            log.stdout('Get \n {}'.format(error))            
            raise Exception('Could not get document %s from KVS: %s' % (key, response.content), response.status_code)


    def save(self, key, value):

        '''
            Save key and document in KVS

                key: key for save a document in KVS.
                value : value for document in KVS.
        '''
        log.stdout("Save")

        document = {
            'key': key,
            'value': json.dumps(value)
        }

        try:
            url = self._make_url_endpoint(self.endpoint_write)
            response = requests.post(url, data=json.dumps(document), headers={'content-type': 'application/json'})
            if response.status_code != 201:
                raise Exception('Could not save document %s to KVS: %s' % (key, response.content), response.status_code)

        except Exception as error:
            log.stdout('Save \n {}'.format(error))            
            raise Exception('Could not save document %s to KVS: %s' % (key, response.content), response.status_code)


    def update(self, key, value):
        '''
            Update a document in KVS for a key.

                key: key for update a document in KVS.
                value : new value for document in KVS.
        '''
        log.stdout("Update")

        document = {
            'key': key,
            'value': json.dumps(value)
        }

        try:
            url = self._make_url_endpoint(self.endpoint_write)
            response = requests.put(url, data=json.dumps(document), headers={'content-type': 'application/json'})
            if response.status_code != 200 and response.status_code != 204:
                raise Exception('Could not update document %s to KVS: %s' % (key, response.content), response.status_code)

        except Exception as error:
            log.stdout('Update \n {}'.format(error))            
            raise Exception('Could not save document %s to KVS: %s' % (key, response.content), response.status_code)


    def delete(self, key):
        '''
            Delete a document in KVS for a key.

                key: key for delete a document in KVS.
        '''        
        log.stdout("Delete")

        try:
            url = self._make_url_endpoint(self.endpoint_write)
            response = requests.delete(url + "/{}".format(key))
            if response.status_code != 204 and response.status_code != 404:
                raise Exception('Could not delete document %s from KVS: %s' % (key, response.content), response.status_code)

        except Exception as error:
            log.stdout('Delete \n {}'.format(error))            
            raise Exception('Could not delete document %s from KVS API: %s' % (key, response.content), response.status_code)


    def batchGet(self, keys=[]):
        '''
            Make a batch get document in KVS for a key list.

                keys: key list for get a documents in KVS.
        '''
        log.stdout("BatchGet")
        documents = []
        url = self._make_url_endpoint(self.endpoint_write) + '?keys='

        for key in keys:
            url += str(key) + ','

        url = url[:len(url) - 1]
        url += ('&operationType=BATCH')

        try:
            response = requests.get(url)
            if response.status_code == 200:
                data = json.loads(response.content)
                for document in data:
                    documents.append(document)

                return documents
            else:
                raise Exception('Could not get documents %s from KVS : %s' % (key, response.content), response.status_code)

        except Exception as error:
            log.stdout('BatchGet \n {}'.format(error))            
            raise Exception('Could not get documents %s from KVS: %s' % (key, response.content), response.status_code)


    def batch_save(self, documents=[]):
        '''
            Make a batch save documents in KVS for a document list.

                documents: documents list for save a documents in KVS.
        '''        
        if not documents:
            raise Exception('Documents should not be null')

        url = url = self._make_url_endpoint(self.endpoint_write) + '?operationType=BATCH'
        payloadDocuments = []

        for document in documents:
            payload = {'key': document['key'], 'value': document['value']}
            payloadDocuments.append(payload)

        try:
            response = requests.post(url, data=json.dumps(payloadDocuments), headers={'content-type': 'application/json'})

            if response.status_code != 201:
                raise Exception('Could not save documents to KVS: %s' % response.content, response.status_code)

        except Exception as error:
            log.stdout('BatchSave \n {}'.format(error))            
            raise Exception('Could not save documents %s from KVS: %s' % (key, response.content), response.status_code)


    def batch_update(self, documents=[]):
        '''
            Make a batch update documents in KVS for a document list.

                documents: documents list for update a documents in KVS.
        '''                
        if not documents:
            raise Exception('Documents should not be null')

        url = url = self._make_url_endpoint(self.endpoint_write) + '?operationType=BATCH'
        payloadDocuments = []

        for document in documents:
            payload = {'key': document['key'], 'value': document['value']}
            payloadDocuments.append(payload)

        try:
            response = requests.put(url, data=json.dumps(payloadDocuments), headers={'content-type': 'application/json'})

            if response.status_code != 201:
                raise Exception('Could not update documents to KVS: %s' % response.content, response.status_code)

        except Exception as error:
            log.stdout('Batch Update \n {}'.format(error))            
            raise Exception('Could not update documents %s from KVS: %s' % (key, response.content), response.status_code)

    def batch_delete(self, keys=[]):
        '''
            Make a batch delete documents in KVS for a key list.

                keys: key list for delete a documents in KVS.
        '''                
        documents = []
        url = self._make_url_endpoint(self.endpoint_write) + '?keys='

        for key in keys:
            url += str(key) + ','

        url = url[:len(url) - 1]
        url += ('&operationType=BATCH')

        try:
            response = requests.delete(url)
            if response.status_code == 200:
                data = json.loads(response.content)
                for document in data:
                    documents.append(document)

                return documents
            else:
                raise Exception('Could not delete documents %s from KVS: %s' % (key, response.content), response.status_code)

        except Exception as error:
            log.stdout('Batch Delete \n {}'.format(error))            
            raise Exception('Could not delete documents %s from KVS: %s' % (key, response.content), response.status_code)

    def _make_url_endpoint(self, endpoint):

        return endpoint + '/containers/' + self.container_name + "__" + self.application_name
