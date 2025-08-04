import grpc
import os
import time
from collections import OrderedDict
from flask import Flask, jsonify, request
import PropertyLookup_pb2 as pb2
import PropertyLookup_pb2_grpc as pb2_grpc

app = Flask(__name__)

class CacheLRU:
    def __init__(self, max_size):
        self.storage = OrderedDict()
        self.max_size = max_size

    def retrieve(self, key):
        if key not in self.storage:
            return None
        self.storage.move_to_end(key)
        return self.storage[key]

    def store(self, key, value):
        if key in self.storage:
            self.storage.move_to_end(key)
        elif len(self.storage) >= self.max_size:
            self.storage.popitem(last=False)
        self.storage[key] = value

class PropertyClient:
    def __init__(self):
        project_name = os.getenv('PROJECT', 'p2')
        self.nodes = [f"{project_name}-dataset-1:5000", f"{project_name}-dataset-2:5000"]
        self.active_node = 0
        self.cache = CacheLRU(3)

    def fetch_addresses(self, zip_code, count):
        max_cache = 8
        if count <= max_cache:
            cached_result = self.cache.retrieve(zip_code)
            if cached_result:
                return cached_result[:count], "cache", None

        for _ in range(5):
            try:
                channel = grpc.insecure_channel(self.nodes[self.active_node])
                stub = pb2_grpc.PropertyLookupStub(channel)
                adjusted_count = max(count, max_cache)
                reply = stub.LookupByZip(pb2.ZipRequest(zip=zip_code, limit=adjusted_count))
                
                if reply.addresses:
                    self.cache.store(zip_code, reply.addresses[:max_cache])
                return reply.addresses[:count], str(self.active_node + 1), None
            except grpc.RpcError as err:
                error_msg = str(err)
                time.sleep(0.1)
            finally:
                self.active_node = (self.active_node + 1) % len(self.nodes)

        if count <= max_cache:
            cached_result = self.cache.retrieve(zip_code)
            if cached_result:
                return cached_result[:count], "cache", None

        return [], None, f"Data retrieval failed after multiple attempts: {error_msg}"

property_client = PropertyClient()

@app.route("/lookup/<zip_code>")
def lookup(zip_code):
    zip_code = int(zip_code)
    count = request.args.get("limit", default=4, type=int)
    results, source, error = property_client.fetch_addresses(zip_code, count)
    return jsonify({"addrs": results, "source": source, "error": error})

def launch():
    app.run(host="0.0.0.0", port=8080, debug=False, threaded=False)

if __name__ == "__main__":
    launch()