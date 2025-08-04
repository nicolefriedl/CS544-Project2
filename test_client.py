import grpc
import PropertyLookup_pb2 as pb2
import PropertyLookup_pb2_grpc as pb2_grpc

def run():
    """Client to test the gRPC PropertyLookup service."""
    channel = grpc.insecure_channel("localhost:5000")
    stub = pb2_grpc.PropertyLookupStub(channel)
    test_cases = [
        {"zip": 53703, "limit": 5},
        {"zip": 53711, "limit": 3},
        {"zip": 99999, "limit": 2}
    ]
    for case in test_cases:
        request = pb2.ZipRequest(zip=case["zip"], limit=case["limit"])
        try:
            response = stub.LookupByZip(request)
            print(f"ZIP: {case['zip']}, Limit: {case['limit']}, Addresses: {response.addresses}")
        except grpc.RpcError as e:
            print(f"Failed to fetch addresses for ZIP {case['zip']}: {e}")

if __name__ == "__main__":
    run()