import asyncio
import time
import argparse
import httpx
from tqdm.asyncio import tqdm

FACADE_URL = "http://localhost:8080"

async def client_worker(client: httpx.AsyncClient, user_id: str, num_requests: int, amount: float, progress_bar: tqdm):
    payload = {"user_Id": user_id, "amount": amount}
    
    for _ in range(num_requests):
        response = await client.post(f"{FACADE_URL}/transaction", json=payload)
        response.raise_for_status()
        progress_bar.update(1)

async def main():
    parser = argparse.ArgumentParser(description="Load Test for Microservices")
    parser.add_argument("--clients", type=int, default=10, help="Number of concurrent clients")
    parser.add_argument("--requests-per-client", type=int, default=10000, help="Requests per client")
    parser.add_argument("--amount", type=float, default=1.0, help="Amount to add")
    parser.add_argument("--same-user", action="store_true", help="Send all requests to one shared account")
    parser.add_argument("--verify", action="store_true", help="Check balances after test")
    
    args = parser.parse_args()
    total_requests = args.clients * args.requests_per_client
    
    limits = httpx.Limits(max_connections=100, max_keepalive_connections=20)
    
    async with httpx.AsyncClient(limits=limits, timeout=30.0) as client:
        await client.post(f"{FACADE_URL}/metrics/reset")
        
        progress_bar = tqdm(total=total_requests, desc="Request progress", unit="req")
        start_time = time.perf_counter()
        
        tasks = []
        for i in range(args.clients):
            user_id = "shared_user" if args.same_user else f"user_{i}"
            tasks.append(client_worker(client, user_id, args.requests_per_client, args.amount, progress_bar))
        
        await asyncio.gather(*tasks)
        
        total_time = time.perf_counter() - start_time
        progress_bar.close()
        
        rps = total_requests / total_time
        
        print("Test Results")
        print(f"Total requests: {total_requests}")
        print(f"Elapsed seconds: {total_time:.4f} sec")
        print(f"Requests per second: {rps:.2f}")
        
        metrics_resp = await client.get(f"{FACADE_URL}/metrics")
        metrics = metrics_resp.json()
        
        print(f"Logging total seconds: {metrics.get('logging_time_sec', 0):.4f} sec")
        print(f"Counter total seconds: {metrics.get('counter_time_sec', 0):.4f} sec")
        
        if args.verify:
            print("\nWaiting 3 seconds for Kafka to finish processing queue...")
            await asyncio.sleep(3)
            
            if args.same_user:
                bal_resp = await client.get(f"{FACADE_URL}/user/shared_user")
                expected = total_requests * args.amount
                print(f"User balance: {bal_resp.json().get('balance')} (Expected: {expected})")
            else:
                accounts_resp = await client.get(f"{FACADE_URL}/accounts")
                accounts_data = accounts_resp.json()
                print(f"Number of created accounts: {len(accounts_data)} (Expected: {args.clients})")
                
                sample_user = "user_0"
                expected = args.requests_per_client * args.amount
                print(f"Balance of {sample_user}: {accounts_data.get(sample_user)} (Expected: {expected})")

if __name__ == "__main__":
    asyncio.run(main())