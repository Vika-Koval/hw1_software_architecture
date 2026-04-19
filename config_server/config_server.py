import os
from threading import Lock
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

active_services: dict[str, dict[str, str]] = {}
mutex = Lock()

app_settings = {
    "kafka.bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
    "kafka.balance_updates.topic": os.getenv("KAFKA_BALANCE_UPDATES_TOPIC", "balance-updates"),
    "kafka.balance_updates.group_id": os.getenv("KAFKA_BALANCE_UPDATES_GROUP", "counter-service"),
    "hazelcast.cluster_name": os.getenv("HAZELCAST_CLUSTER_NAME", "lab3-cluster"),
    "hazelcast.members": os.getenv("HAZELCAST_MEMBERS", "hazelcast1:5701,hazelcast2:5701,hazelcast3:5701"),
    "hazelcast.map_name": os.getenv("HAZELCAST_MAP_NAME", "transactions-map"),
}

@asynccontextmanager
async def server_lifespan(app: FastAPI):
    print(f"Discovery Server initialized. Loaded settings: {app_settings}", flush=True)
    yield
    print("Discovery Server is shutting down.", flush=True)

app = FastAPI(lifespan=server_lifespan, title="Microservices Discovery")

class NodeRegistration(BaseModel):
    service_name: str
    instance_id: str
    address: str

class NodeRemoval(BaseModel):
    service_name: str
    instance_id: str

@app.post("/register")
async def add_service_node(payload: NodeRegistration):
    with mutex:
        if payload.service_name not in active_services:
            active_services[payload.service_name] = {}
            
        active_services[payload.service_name][payload.instance_id] = payload.address
        current_state = dict(active_services[payload.service_name])
        
    print(f"Node '{payload.instance_id}' added to '{payload.service_name}' at {payload.address}", flush=True)
    return {"status": "ok", "service_name": payload.service_name, "instances": current_state}

@app.post("/unregister")
async def remove_service_node(payload: NodeRemoval):
    with mutex:
        target_group = active_services.get(payload.service_name)
        
        if not target_group or payload.instance_id not in target_group:
            raise HTTPException(status_code=404, detail="Service instance not found")
            
        del target_group[payload.instance_id]
        
        if len(target_group) == 0:
            del active_services[payload.service_name]
            current_state = {}
        else:
            current_state = dict(target_group)
            
    print(f"Node '{payload.instance_id}' removed from '{payload.service_name}'", flush=True)
    return {"status": "ok", "service_name": payload.service_name, "instances": current_state}

@app.get("/services/{service_name}")
async def fetch_specific_service(service_name: str):
    with mutex:
        service_nodes = dict(active_services.get(service_name, {}))
    return {"service_name": service_name, "instances": service_nodes}

@app.get("/services")
async def fetch_all_registered():
    with mutex:
        full_registry = {srv: dict(nodes) for srv, nodes in active_services.items()}
    return {"services": full_registry}

@app.get("/config/{param_key:path}")
async def fetch_config_parameter(param_key: str):
    value = app_settings.get(param_key)
    if value is None:
        raise HTTPException(status_code=404, detail="Configuration key not present")
    return {"key": param_key, "value": value}

@app.get("/config")
async def fetch_entire_config():
    return {"config": dict(app_settings)}