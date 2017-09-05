from log import Logger
from flask import Flask
from flask import request
import logging
import requests
import json
import os
import sys


VERSION = '0.0.1'


log = Logger('Playground', logging.INFO)

app = Flask(__name__)

@app.route('/bq/publish/<message>')
def bq_publish(message):
    topic_name = os.environ['BIGQUEUE_TOPIC_TOPIC_PLAYGROUND_TOPIC_NAME']
    endpoint = os.environ['BIGQUEUE_TOPIC_TOPIC_PLAYGROUND_ENDPOINT']

    message = {
        "msg": message, 
        "topics": [topic_name]
    }

    headers = {'content-type': 'application/json'}

    try:
        response = requests.post("http://" + endpoint + "/messages", data=json.dumps(json_data), headers=headers)
        return 'Publish - OK!'
    except Exception as error:
        raise Exception('Could not publish data %s in BigQueue %s' % (key, response.content), response.status_code)


@app.route('/bq/consume')
def bq_consume():
    info = None
    if request.json:
        info = json.loads(request.json)

    log.stdout(' Consume Queue - Info: ' + str(info))
    return 'Consume - OK'


@app.route('/kvs/get/<key>')
def kvs_get(key):
    container_uri = '/containers/tests__playground'
    endpoint_kvs_read = os.environ['KEY_VALUE_STORE_TESTS_END_POINT_READ'] + container_uri + "/" + key
    headers = {'Cntent-Type': 'application/json'}

    return key
    try:
        response = requests.get(endpoint_kvs_read, headers=headers)
        return "KVS - Get"
    except Exception as error:
        log.stdout(error)
        raise Exception('Could not get document %s in KVS %s' % (key, response.content), response.status_code)


@app.route('/kvs/get/<key>/<message>')
def kvs_send(key, message):
    container_uri = '/containers/tests__playground'
    endpoint_kvs_read = os.environ['KEY_VALUE_STORE_TESTS_END_POINT_WRITE'] + container_uri + "/" + key
    headers = {'content-Type': 'application/json'}

    document = {
        'key': key,
        'value': message,
    }

    try:
        response = requests.post(endpoint_kvs_write, json=json_data, headers=headers)
        return "KVS - Send"
    except Exception as error:
        log.stdout(error)
        raise Exception('Could not send document %s in KVS %s' % (key, response.content), response.status_code)

@app.route('/ping')
def ping():
    return 'pong'