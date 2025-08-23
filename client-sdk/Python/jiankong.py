import requests
import time
import sys
from datetime import datetime

# --- 配置 ---
COLLECTION_NAME = "fashion_mnist"
OASISDB_URL = f"http://localhost:8080/v1/collections/{COLLECTION_NAME}"
POLL_INTERVAL_SECONDS = 0.5  # 每 0.5 秒检查一次

def run_monitor():
    """持续轮询 OasisDB 的 get_collection 端点。"""
    print(f"--- 开始监控集合 '{COLLECTION_NAME}' ---")
    collection_existed = False
    
    try:
        while True:
            try:
                response = requests.get(OASISDB_URL, timeout=1)
                
                timestamp = datetime.now().strftime('%H:%M:%S.%f')[:-3] # 格式化时间戳

                if response.status_code == 200:
                    if not collection_existed:
                        print(f"[{timestamp}] 状态: 发现集合！(Status 200)")
                        collection_existed = True
                    # 如果已经存在，可以保持安静，避免刷屏
                    # sys.stdout.write(f"\r[{timestamp}] 状态: 集合存在 (Status 200)      ")
                    # sys.stdout.flush()

                elif response.status_code == 404:
                    if collection_existed:
                        print(f"\n[{timestamp}] 状态: 集合消失！(Status 404)")
                        print("--- 监控结束 ---")
                        break
                    else:
                        # 如果一开始就不存在，也保持安静
                        pass
                else:
                    # 其他状态码
                    print(f"\n[{timestamp}] 收到异常状态码: {response.status_code}")

            except requests.exceptions.RequestException:
                # 连接不上服务器
                sys.stdout.write(f"\r无法连接到 OasisDB 服务器...")
                sys.stdout.flush()

            time.sleep(POLL_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        print("\n--- 手动停止监控 ---")


if __name__ == "__main__":
    run_monitor()
