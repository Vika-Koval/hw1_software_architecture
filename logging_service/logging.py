import os
import json
import asyncio
import grpc
import hazelcast
from contextlib import asynccontextmanager
from fastapi import FastAPI

SERVER_NAME = os.getenv("HOSTNAME", "logging-pod")
GRPC_PORT = os.getenv("GRPC_PORT", "50051")

hz_client = None
hz_map = None
grpc_server = None

async def handle_save_log(request: bytes, context: grpc.aio.ServicerContext) -> bytes:
    if hz_map is None:
        print(f"{SERVER_NAME}: Rejecting SaveLog - Hazelcast not ready.", flush=True)
        await context.abort(grpc.StatusCode.UNAVAILABLE, "Hazelcast map is offline")

    data = json.loads(request.decode("utf-8"))
    
    tx_id = data.get("transaction_id") or data.get("transaction_Id")
    if not tx_id or "user_id" not in data or "amount" not in data:
        print(f"{SERVER_NAME}: Rejecting SaveLog - Missing fields in {data}", flush=True)
        await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Missing transaction_id, user_id, or amount")

    hz_map.put(tx_id, json.dumps(data))
    print(f"{SERVER_NAME}: Successfully saved tx: {tx_id}", flush=True)
    
    return json.dumps({"status": "success", "processed_by": SERVER_NAME}).encode("utf-8")

async def handle_get_logs(request: bytes, context: grpc.aio.ServicerContext) -> bytes:
    if hz_map is None:
        print(f"{SERVER_NAME}: Rejecting FetchLogs - Hazelcast not ready.", flush=True)
        await context.abort(grpc.StatusCode.UNAVAILABLE, "Hazelcast map is offline")

    entries = hz_map.entry_set()
    parsed_logs = [json.loads(value) for _, value in entries]
    print(f"{SERVER_NAME}: Fetched {len(parsed_logs)} logs", flush=True)
    
    return json.dumps(parsed_logs).encode("utf-8")

@asynccontextmanager
async def lifespan(app: FastAPI):
    global hz_client, hz_map, grpc_server
    
    hz_cluster = os.getenv("HAZELCAST_CLUSTER_NAME", "lab-cluster")
    hz_members = os.getenv("HAZELCAST_MEMBERS", "hazelcast:5701").split(",")

    print(f"{SERVER_NAME}: Connecting to Hazelcast {hz_cluster} at {hz_members}", flush=True)
    
    for attempt in range(1, 21):
        try:
            hz_client = hazelcast.HazelcastClient(
                cluster_name=hz_cluster, 
                cluster_members=hz_members,
                cluster_connect_timeout=3.0
            )
            hz_map = hz_client.get_map(os.getenv("HAZELCAST_MAP_NAME", "transactions-map")).blocking()
            print(f"{SERVER_NAME}: Hazelcast connected successfully!", flush=True)
            break
        except Exception as e:
            print(f"Attempt {attempt}: Hazelcast not ready. {e}", flush=True)
            await asyncio.sleep(3)

    grpc_server = grpc.aio.server(options=[
        ('grpc.max_send_message_length', 32 * 1024 * 1024),
        ('grpc.max_receive_message_length', 32 * 1024 * 1024)
    ])
    
    rpc_handlers = grpc.method_handlers_generic_handler(
        "LoggingService",
        {
            "SaveLog": grpc.unary_unary_rpc_method_handler(handle_save_log),
            "FetchLogs": grpc.unary_unary_rpc_method_handler(handle_get_logs),
        }
    )
    
    grpc_server.add_generic_rpc_handlers((rpc_handlers,))
    grpc_server.add_insecure_port(f"[::]:{GRPC_PORT}")
    await grpc_server.start()
    print(f"{SERVER_NAME}: gRPC server running on port {GRPC_PORT}", flush=True)

    yield

    print(f"{SERVER_NAME}: Shutting down...", flush=True)
    if grpc_server:
        await grpc_server.stop(2)
    if hz_client:
        hz_client.shutdown()

app = FastAPI(lifespan=lifespan)

@app.get("/health")
async def health(): 
    return {"status": "ok", "service": "logging-service", "instance": SERVER_NAME}