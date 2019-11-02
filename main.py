#!/usr/bin/env python

import socket
import logging
import redis
import json
from pymongo import MongoClient

cache = redis.Redis(host='redis', port=6379)

db_client = MongoClient('mongodb://root:password@mongo')
db = db_client.server_storage
storage = db.storage

sock = socket.socket()
sock.bind(('', 65432))
sock.listen()
conn, addr = sock.accept()

while True:
    data = conn.recv(1024)

    if not data:
        break

    response = {}
    request = {}
    logging.info(data)
    try:
        request = json.loads(data)
    except ValueError as e:
        response["Status"] = "Bad Request"
    logging.info(request)
    if request["action"] == "get":
        if request.get("no-cache"):
            storage_ans = storage.find_one({"key": request["key"]})
            if storage_ans:
                response["message"] = storage_ans['value']
                response["Status"] = "OK"
            else:
                response["Status"] = "Not found"
        else:
            cached_ans = cache.get(request["key"])

            if not cached_ans:
                storage_ans = storage.find_one({"key": request["key"]})
                if storage_ans:
                    response["message"] = storage_ans['value']
                    response["Status"] = "OK"
                    cache.set(request["key"], storage_ans["value"])
                else:
                    response["Status"] = "Not found"

            else:
                if type(cached_ans) is bytes:
                    response["message"] = cached_ans.decode('utf-8')
                else:
                    response["message"] = cached_ans
                    response["Status"] = "OK"

    if request["action"] == "put":
        storage_data = storage.find_one_and_update(
            {"key": request["key"]}, {"$set": {"key": request["key"], "value": request["message"]}}, upsert=True)
        response["Status"] = "OK"

    if request["action"] == "delete":
        deleted_count = storage.delete_one({"key": request["key"]}).deleted_count
        if deleted_count:
            response["Status"] = "OK"
        else:
            response["Status"] = "Not found"

    response_string = json.dumps(response)
    response_string += '\n'
    conn.send(response_string.encode('utf-8'))

conn.close()
