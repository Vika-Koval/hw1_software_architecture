import os
import subprocess
import shutil

NAMESPACE = "micro-lab"
OUTPUT_DIR = "service_logs"

def download_logs_final_version():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
    
    kubectl_path = shutil.which("kubectl")
    if not kubectl_path:
        print("Error: 'kubectl' command not found in PATH.")
        return

    print(f"Using kubectl from: {kubectl_path}")
    print(f"Fetching pods from '{NAMESPACE}'...")

    try:
        result = subprocess.run(
            [kubectl_path, "get", "pods", "-n", NAMESPACE, "--no-headers", "-o", "custom-columns=:metadata.name"],
            capture_output=True, text=True, check=True
        )
        pod_names = [name.strip() for name in result.stdout.split('\n') if name.strip()]
    except subprocess.CalledProcessError as e:
        print(f"Failed to get pods: {e.stderr}")
        return

    print(f"Found {len(pod_names)} pods. Downloading logs...")

    for name in pod_names:
        file_path = os.path.join(OUTPUT_DIR, f"{name}.txt")
        
        log_res = subprocess.run(
            [kubectl_path, "logs", "-n", NAMESPACE, name, "--all-containers=true"],
            capture_output=True, text=True
        )

        if log_res.returncode == 0:
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(log_res.stdout)
            print(f"{name}: Saved.")
        else:
            print(f"{name}: Failed! Error: {log_res.stderr.strip()}")

if __name__ == "__main__":
    download_logs_final_version()