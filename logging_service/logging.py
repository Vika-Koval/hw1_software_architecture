import os
import json
import grpc
import hazelcast
from contextlib import asynccontextmanager
from fastapi import FastAPI

HZ_CLUSTER = os.getenv("HZ_CLUSTER_NAME", "lab3-cluster")
HZ_MEMBERS = os.getenv("HAZELCAST_MEMBERS", "hazelcast1:5701").split(",")
GRPC_PORT = os.getenv("GRPC_PORT", "50051")
SERVER_NAME = os.getenv("LOGGING_INSTANCE_ID", "logging-unknown")

hz_client = None
hz_map = None
grpc_server = None


async def handle_save_log(request: bytes, context: grpc.aio.ServicerContext) -> bytes:
    data = json.loads(request.decode("utf-8"))
    print(f"{SERVER_NAME} Received save log request: {data}", flush=True)

    tx_id = data.get("transaction_Id")
    
    hz_map.put(tx_id, json.dumps(data))
    print(f"{SERVER_NAME} Wrote to Hazelcast transaction: {tx_id}", flush=True)

    response_data = {"status": "success", "processed_by": SERVER_NAME, "transaction_Id": tx_id}
    return json.dumps(response_data).encode("utf-8")

async def handle_get_logs(request: bytes, context: grpc.aio.ServicerContext) -> bytes:
    print(f"{SERVER_NAME} Received get logs request", flush=True)
    entries = hz_map.entry_set()
    
    parsed_logs = [json.loads(value) for key, value in entries]

    print(f"{SERVER_NAME} Received request to fetch all logs", flush=True)
    return json.dumps(parsed_logs).encode("utf-8")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global hz_client, hz_map, grpc_server
    print(f"{SERVER_NAME} Starting Hazelcast connection...", flush=True)
    try:
        hz_client = hazelcast.HazelcastClient(
            cluster_name=HZ_CLUSTER,
            cluster_members=HZ_MEMBERS
        )
        hz_map = hz_client.get_map("transactions-map").blocking()
        print(f"{SERVER_NAME} Successfully connected to Hazelcast cluster {HZ_CLUSTER}", flush=True)
    except Exception as e:
        print(f"{SERVER_NAME} Error connecting to Hazelcast: {e}", flush=True)

    grpc_server = grpc.aio.server()
    
    
    rpc_methods = {
        "SaveLog": grpc.unary_unary_rpc_method_handler(handle_save_log),
        "FetchLogs": grpc.unary_unary_rpc_method_handler(handle_get_logs),
    }
    handler = grpc.method_handlers_generic_handler("LoggingService", rpc_methods)
    grpc_server.add_generic_rpc_handlers((handler,))
    
    grpc_server.add_insecure_port(f"[::]:{GRPC_PORT}")
    await grpc_server.start()

    print(f"{SERVER_NAME} gRPC server listening on {GRPC_PORT}", flush=True)

    yield 

    print(f"{SERVER_NAME} Shutting down...", flush=True)
    if grpc_server:
        await grpc_server.stop(2)
    if hz_client:
        hz_client.shutdown()

app = FastAPI(lifespan=lifespan)