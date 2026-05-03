import requests
import time

URL = "http://localhost:8080/transaction"


while True:
    try:
        start_time = time.time()
        payload = {"user_Id": "user-1", "amount": 10.0} 
        
        response = requests.post(URL, json=payload, timeout=15.0)
        elapsed_ms = int((time.time() - start_time) * 1000)

        if response.status_code == 200:
            tx_id = response.json().get("transaction_id", "unknown")[:8]
            print(f"[\033[32mOK\033[0m] {elapsed_ms}ms | TX: {tx_id}")
        else:
            print(f"[\033[31mFAIL\033[0m] Status: {response.status_code} | {response.text}")
            
    except requests.exceptions.RequestException as e:
        print(f"[\033[31mERROR\033[0m] Connection lost or timeout.")

    time.sleep(0.5)