import asyncio
import time
import httpx
from tqdm.asyncio import tqdm

FACADE_URL = "http://localhost:8080"

async def client_worker(client: httpx.AsyncClient, client_id: int, num_requests: int, shared_account: bool, progress_bar: tqdm):
    user_id = "shared_user" if shared_account else f"user_{client_id}"
    payload = {"user_Id": user_id, "amount": 1.0}
    
    for _ in range(num_requests):
        response = await client.post(f"{FACADE_URL}/transaction", json=payload)
        response.raise_for_status()
        progress_bar.update(1)

async def run_scenario(scenario_name: str, num_clients: int, requests_per_client: int, shared_account: bool):
    total_requests = num_clients * requests_per_client
    
    limits = httpx.Limits(max_connections=100, max_keepalive_connections=20)
    
    async with httpx.AsyncClient(limits=limits, timeout=30.0) as client:
        await client.post(f"{FACADE_URL}/metrics/reset")
        
        progress_bar = tqdm(total=total_requests, desc="Request progress", unit="req")
        
        start_time = time.perf_counter()
        
        tasks = [
            client_worker(client, i, requests_per_client, shared_account, progress_bar)
            for i in range(num_clients)
        ]
        
        await asyncio.gather(*tasks)
        
        total_time = time.perf_counter() - start_time
        progress_bar.close()
        
        rps = total_requests / total_time
        
        metrics_resp = await client.get(f"{FACADE_URL}/metrics")
        metrics = metrics_resp.json()
        
        if shared_account:
            bal_resp = await client.get(f"{FACADE_URL}/user/shared_user")
            print(f"Shared account balance: {bal_resp.json().get('balance')} (Expected: {total_requests})")
        else:
            accounts_resp = await client.get(f"{FACADE_URL}/accounts")
            accounts_data = accounts_resp.json()
            print(f"Number of created accounts: {len(accounts_data)} (Expected: {num_clients})")
            print(f"Balance of user_0: {accounts_data.get('user_0')} (Expected: {requests_per_client})")
        
        print(f"\nTotal execution time: {total_time:.4f} sec")
        print(f"Throughput (RPS): {rps:.2f} requests/sec")
        print(f"Logging Service time: {metrics.get('logging_time_sec', 0):.4f} sec")
        print(f"Counter Service time: {metrics.get('counter_time_sec', 0):.4f} sec")

async def main():
    num_clients = 10
    requests_per_client = 10000 
    await run_scenario("Scenario 1: Different accounts", num_clients, requests_per_client, shared_account=False)
    await asyncio.sleep(1)
    await run_scenario("Scenario 2: Single shared account", num_clients, requests_per_client, shared_account=True)

if __name__ == "__main__":
    asyncio.run(main())