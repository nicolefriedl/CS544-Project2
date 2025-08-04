import grpc
import gzip
import csv
from concurrent import futures
import PropertyLookup_pb2 as pb2
import PropertyLookup_pb2_grpc as pb2_grpc

addresses_by_zip = {}

def load_addresses(filename="addresses.csv.gz"):
    global addresses_by_zip
    with gzip.open(filename, "rt") as f:
        reader = csv.reader(f)
        header = next(reader)
        address_idx = header.index("Address")
        zipcode_idx = header.index("ZipCode")
        for row in reader:
            address = row[address_idx]
            zipcode = row[zipcode_idx]
            if zipcode not in addresses_by_zip:
                addresses_by_zip[zipcode] = []
            addresses_by_zip[zipcode].append(address)
    for zipcode in addresses_by_zip:
        addresses_by_zip[zipcode].sort()

class PropertyLookupServicer(pb2_grpc.PropertyLookupServicer):
    def LookupByZip(self, request, context):
        zipcode = str(request.zip)
        limit = request.limit
        print(f"Received request: ZIP={zipcode}, Limit={limit}")
        if zipcode in addresses_by_zip:
            addresses = addresses_by_zip[zipcode][:limit]
            return pb2.AddressList(addresses=addresses)
        else:
            return pb2.AddressList(addresses=[])

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1), options=[("grpc.so_reuseport", 0)])
    pb2_grpc.add_PropertyLookupServicer_to_server(PropertyLookupServicer(), server)
    server.add_insecure_port("0.0.0.0:5000")
    load_addresses()
    print("gRPC server started on port 5000")
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    serve()