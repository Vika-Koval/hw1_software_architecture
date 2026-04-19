import os
import json
import asyncio
import grpc
import hazelcast
import httpx
from contextlib import asynccontextmanager
from fastapi import FastAPI

CONFIG_SERVER_URL = os.getenv("CONFIG_SERVER_URL", "http://config-server:8500")
SERVER_NAME = os.getenv("LOGGING_INSTANCE_ID", "logging-unknown")
GRPC_PORT = os.getenv("GRPC_PORT", "50051")
SELF_ADDRESS = os.getenv("SELF_ADDRESS", f"{SERVER_NAME}:50051")

hz_client = None
hz_map = None
grpc_server = None

async def fetch_config_value(key: str) -> str:
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{CONFIG_SERVER_URL}/config/{key}")
        response.raise_for_status()
        return response.json()["value"]

async def register_in_config_server():
    try:
        async with httpx.AsyncClient() as client:
            await client.post(
                f"{CONFIG_SERVER_URL}/register",
                json={
                    "service_name": "logging-service",
                    "instance_id": SERVER_NAME,
                    "address": SELF_ADDRESS
                }
            )
        print(f"{SERVER_NAME} Successfully registered in Config Server at {SELF_ADDRESS}", flush=True)
    except Exception as e:
        print(f"{SERVER_NAME} Failed to register in Config Server: {e}", flush=True)

async def handle_save_log(request: bytes, context: grpc.aio.ServicerContext) -> bytes:
    data = json.loads(request.decode("utf-8"))
    print(f"{SERVER_NAME} Received save log request: {data}", flush=True)

    tx_id = data.get("transaction_id") or data.get("transaction_Id", "unknown")
    
    hz_map.put(tx_id, json.dumps(data))
    print(f"{SERVER_NAME} Wrote to Hazelcast transaction: {tx_id}", flush=True)

    response_data = {"status": "success", "processed_by": SERVER_NAME, "transaction_id": tx_id}
    return json.dumps(response_data).encode("utf-8")

async def handle_get_logs(request: bytes, context: grpc.aio.ServicerContext) -> bytes:
    print(f"{SERVER_NAME} Received get logs request", flush=True)
    entries = hz_map.entry_set()
    
    parsed_logs = [json.loads(value) for key, value in entries]

    print(f"{SERVER_NAME} Received request to fetch all logs", flush=True)
    return json.dumps(parsed_logs).encode("utf-8")

class LoggingServiceHandler(grpc.GenericRpcHandler):
    def service_name(self):
        return "LoggingService"

    def service(self, handler_call_details):
        methods = {
            "/LoggingService/SaveLog": grpc.unary_unary_rpc_method_handler(handle_save_log),
            "/LoggingService/FetchLogs": grpc.unary_unary_rpc_method_handler(handle_get_logs),
        }
        return methods.get(handler_call_details.method)
    
@asynccontextmanager
async def lifespan(app: FastAPI):
    global hz_client, hz_map, grpc_server
    

    try:
        hz_cluster = await fetch_config_value("hazelcast.cluster_name")
        hz_members_str = await fetch_config_value("hazelcast.members")
        hz_map_name = await fetch_config_value("hazelcast.map_name")
        hz_members = hz_members_str.split(",")
    except Exception as e:
        print(f"{SERVER_NAME} Failed to fetch config: {e}. Using defaults.", flush=True)
        hz_cluster = "lab3-cluster"
        hz_members = ["hazelcast1:5701"]
        hz_map_name = "transactions-map"

    print(f"{SERVER_NAME} Starting Hazelcast connection to {hz_cluster}", flush=True)
    for _ in range(15):
        try:
            hz_client = hazelcast.HazelcastClient(
                cluster_name=hz_cluster,
                cluster_members=hz_members,
                cluster_connect_timeout=2.0
            )
            hz_map = hz_client.get_map(hz_map_name).blocking()
            print(f"{SERVER_NAME} Successfully connected to Hazelcast cluster {hz_cluster}", flush=True)
            break
        except Exception:
            print("Waiting for Hazelcast to wake up...", flush=True)
            await asyncio.sleep(3)

    grpc_server = grpc.aio.server()
    rpc_methods = {
        "SaveLog": grpc.unary_unary_rpc_method_handler(handle_save_log),
        "FetchLogs": grpc.unary_unary_rpc_method_handler(handle_get_logs),
    }
    grpc_server.add_generic_rpc_handlers((LoggingServiceHandler(),))
    
    grpc_server.add_insecure_port(f"[::]:{GRPC_PORT}")
    await grpc_server.start()

    print(f"{SERVER_NAME} gRPC server listening on {GRPC_PORT}", flush=True)

    for _ in range(10):
        try:
            await register_in_config_server()
            break
        except Exception:
            print("Waiting for Config Server...", flush=True)
            await asyncio.sleep(2)

    yield 

    print(f"{SERVER_NAME} Shutting down...", flush=True)
    
    try:
        async with httpx.AsyncClient() as client:
            await client.post(
                f"{CONFIG_SERVER_URL}/unregister",
                json={"service_name": "logging-service", "instance_id": SERVER_NAME}
            )
    except:
        pass

    if grpc_server:
        await grpc_server.stop(2)
    if hz_client:
        hz_client.shutdown()

app = FastAPI(lifespan=lifespan)