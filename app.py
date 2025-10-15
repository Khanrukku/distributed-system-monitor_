from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
import redis
import json
import time
import threading
from datetime import datetime
import random
import os

app = Flask(__name__)
CORS(app)

# ---------------------- Redis Configuration ----------------------
REDIS_URL = os.environ.get('REDIS_URL')

# Validate the URL and ensure it's secure
if REDIS_URL is None or not REDIS_URL.startswith("rediss://"):
    raise ValueError("❌ Invalid or missing REDIS_URL. Make sure it's a rediss:// URL from Upstash.")

try:
    redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    redis_client.ping()
    print("✅ Connected to Redis successfully")
except Exception as e:
    print("❌ Redis connection failed:", e)
    raise e

pubsub = redis_client.pubsub()

# ---------------------- System Configuration ----------------------
MAX_NODES = 20
METRICS_PER_NODE = 50000
WORKER_POOL_SIZE = 100

class MetricsProcessor:
    def __init__(self):
        self.worker_pool = []
        self.metrics_count = 0
        self.start_time = time.time()
        self.node_health = {}
        
    def process_metric(self, data):
        """Process individual metric with <50ms latency"""
        start = time.time()
        try:
            metric_key = f"metric:{data['node_id']}:{data['timestamp']}"
            redis_client.setex(metric_key, 3600, json.dumps(data))
            self.update_stats(data)
        except Exception as e:
            print("❌ Error processing metric:", e)
        latency = (time.time() - start) * 1000
        return latency < 50

    def update_stats(self, data):
        node_id = data['node_id']
        stats_key = f"node_stats:{node_id}"
        redis_client.hincrby(stats_key, 'total_metrics', 1)
        redis_client.hset(stats_key, 'last_seen', time.time())
        redis_client.hset(stats_key, 'status', data.get('status', 'healthy'))
        redis_client.expire(stats_key, 7200)

processor = MetricsProcessor()

# ---------------------- Worker Thread ----------------------
class Worker(threading.Thread):
    def __init__(self, worker_id):
        super().__init__(daemon=True)
        self.worker_id = worker_id
        self.is_running = True
        
    def run(self):
        worker_pubsub = redis_client.pubsub()
        worker_pubsub.subscribe('metrics_channel')
        for message in worker_pubsub.listen():
            if not self.is_running:
                break
            if message['type'] == 'message':
                try:
                    data = json.loads(message['data'])
                    processor.process_metric(data)
                except Exception as e:
                    print(f"❌ Worker {self.worker_id} error:", e)

def init_worker_pool():
    for i in range(WORKER_POOL_SIZE):
        worker = Worker(i)
        worker.start()
        processor.worker_pool.append(worker)

# ---------------------- Node Health Check ----------------------
def check_node_health():
    while True:
        try:
            current_time = time.time()
            for node_id in range(MAX_NODES):
                stats_key = f"node_stats:{node_id}"
                last_seen = redis_client.hget(stats_key, 'last_seen')
                if last_seen and (current_time - float(last_seen)) > 60:
                    redis_client.hset(stats_key, 'status', 'failed')
                    recover_failed_node(node_id)
        except Exception as e:
            print(f"❌ Health check error: {e}")
        time.sleep(10)

def recover_failed_node(node_id):
    recovery_key = f"recovery:{node_id}"
    redis_client.setex(recovery_key, 300, json.dumps({
        'node_id': node_id,
        'status': 'recovering',
        'timestamp': time.time()
    }))

node_counter = 0
def get_next_node():
    global node_counter
    node_counter = (node_counter + 1) % MAX_NODES
    return node_counter

# ---------------------- Routes ----------------------
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/metrics', methods=['POST'])
def receive_metrics():
    data = request.json
    data['received_at'] = time.time()
    try:
        redis_client.publish('metrics_channel', json.dumps(data))
        return jsonify({'status': 'received', 'latency_target': '<50ms'}), 200
    except Exception as e:
        print("❌ Failed to publish metric:", e)
        return jsonify({'error': 'Redis publish failed'}), 500

@app.route('/api/stats')
def get_stats():
    stats = {
        'total_nodes': MAX_NODES,
        'worker_pool_size': WORKER_POOL_SIZE,
        'nodes': [],
        'system': {
            'uptime': time.time() - processor.start_time,
            'target_metrics_per_hour': 1000000,
            'target_latency_ms': 50
        }
    }
    try:
        for node_id in range(MAX_NODES):
            stats_key = f"node_stats:{node_id}"
            node_stats = redis_client.hgetall(stats_key)
            if node_stats:
                stats['nodes'].append({
                    'node_id': node_id,
                    'total_metrics': int(node_stats.get('total_metrics', 0)),
                    'status': node_stats.get('status', 'unknown'),
                    'last_seen': float(node_stats.get('last_seen', 0))
                })
        return jsonify(stats)
    except Exception as e:
        print("❌ Failed to fetch stats:", e)
        return jsonify({'error': 'Failed to get stats'}), 500

@app.route('/api/simulate', methods=['POST'])
def simulate_load():
    num_metrics = request.json.get('count', 1000)
    def generate_metrics():
        for _ in range(num_metrics):
            node_id = get_next_node()
            metric = {
                'node_id': node_id,
                'timestamp': time.time(),
                'cpu_usage': random.uniform(0, 100),
                'memory_usage': random.uniform(0, 100),
                'disk_io': random.uniform(0, 1000),
                'network_io': random.uniform(0, 1000),
                'status': 'healthy' if random.random() > 0.05 else 'degraded'
            }
            try:
                redis_client.publish('metrics_channel', json.dumps(metric))
            except Exception as e:
                print("❌ Failed to publish simulated metric:", e)
            time.sleep(0.001)
    threading.Thread(target=generate_metrics, daemon=True).start()
    return jsonify({'status': 'simulation_started', 'metrics': num_metrics})

@app.route('/test-redis')
def test_redis():
    try:
        pong = redis_client.ping()
        return jsonify({'redis_ping': pong}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ---------------------- Entry Point ----------------------
if __name__ == '__main__':
    init_worker_pool()
    threading.Thread(target=check_node_health, daemon=True).start()
    print("🚀 App is starting...")
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
