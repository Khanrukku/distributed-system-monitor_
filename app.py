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
CORS(app, resources={r"/*": {"origins": "*"}})

# Redis Configuration with PREFIX for sharing
REDIS_URL = os.environ.get('REDIS_URL', 'redis://localhost:6379')

# IMPORTANT: Change this prefix for each project!
PROJECT_PREFIX = "sysmonitor:"  # For this project

class PrefixedRedis:
    """Redis wrapper that adds project prefix to all keys"""
    def __init__(self, url, prefix):
        self.client = redis.from_url(url, decode_responses=True)
        self.prefix = prefix
    
    def _key(self, k):
        return f"{self.prefix}{k}"
    
    def get(self, key):
        return self.client.get(self._key(key))
    
    def set(self, key, value):
        return self.client.set(self._key(key), value)
    
    def setex(self, key, time, value):
        return self.client.setex(self._key(key), time, value)
    
    def hget(self, name, key):
        return self.client.hget(self._key(name), key)
    
    def hset(self, name, key, value):
        return self.client.hset(self._key(name), key, value)
    
    def hgetall(self, name):
        return self.client.hgetall(self._key(name))
    
    def hincrby(self, name, key, amount=1):
        return self.client.hincrby(self._key(name), key, amount)
    
    def expire(self, key, time):
        return self.client.expire(self._key(key), time)
    
    def publish(self, channel, message):
        return self.client.publish(self._key(channel), message)
    
    def pubsub(self):
        return self.client.pubsub()
    
    def keys(self, pattern):
        return self.client.keys(self._key(pattern))
    
    def scan_iter(self, match=None):
        if match:
            match = self._key(match)
        return self.client.scan_iter(match=match)
    
    def delete(self, key):
        return self.client.delete(self._key(key))
    
    def ping(self):
        return self.client.ping()

# Initialize with prefix
redis_client = PrefixedRedis(REDIS_URL, PROJECT_PREFIX)

# System Configuration
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
        
        # Cache strategy - store in Redis with TTL
        metric_key = f"metric:{data['node_id']}:{data['timestamp']}"
        redis_client.setex(metric_key, 3600, json.dumps(data))
        
        # Update aggregated stats
        self.update_stats(data)
        
        latency = (time.time() - start) * 1000
        return latency < 50
    
    def update_stats(self, data):
        """Update node health and metrics count"""
        node_id = data['node_id']
        stats_key = f"node_stats:{node_id}"
        
        redis_client.hincrby(stats_key, 'total_metrics', 1)
        redis_client.hset(stats_key, 'last_seen', time.time())
        redis_client.hset(stats_key, 'status', data.get('status', 'healthy'))
        redis_client.expire(stats_key, 7200)

processor = MetricsProcessor()

# Pub/Sub Worker Pool
class Worker(threading.Thread):
    def __init__(self, worker_id):
        super().__init__(daemon=True)
        self.worker_id = worker_id
        self.is_running = True
        
    def run(self):
        """Worker thread for async processing"""
        worker_pubsub = redis_client.pubsub()
        # FIXED: Subscribe without double prefix
        worker_pubsub.subscribe(f"{redis_client.prefix}metrics_channel")
        
        for message in worker_pubsub.listen():
            if not self.is_running:
                break
            if message['type'] == 'message':
                try:
                    data = json.loads(message['data'])
                    processor.process_metric(data)
                except Exception as e:
                    print(f"Worker {self.worker_id} error: {e}")

def init_worker_pool():
    """Initialize 100 workers for 10K concurrent events"""
    for i in range(WORKER_POOL_SIZE):
        worker = Worker(i)
        worker.start()
        processor.worker_pool.append(worker)

def check_node_health():
    """Monitor node health and implement failure recovery"""
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
            print(f"Health check error: {e}")
        time.sleep(10)

def recover_failed_node(node_id):
    """Failure recovery mechanism"""
    recovery_key = f"recovery:{node_id}"
    redis_client.setex(recovery_key, 300, json.dumps({
        'node_id': node_id,
        'status': 'recovering',
        'timestamp': time.time()
    }))

node_counter = 0
def get_next_node():
    """Load balancing across nodes"""
    global node_counter
    node_counter = (node_counter + 1) % MAX_NODES
    return node_counter

@app.route('/')
def index():
    """Serve dashboard or API info"""
    return jsonify({
        'status': 'running',
        'service': 'Distributed System Monitor',
        'endpoints': {
            'stats': '/api/stats',
            'metrics': '/api/metrics (POST)',
            'simulate': '/api/simulate (POST)'
        },
        'config': {
            'max_nodes': MAX_NODES,
            'worker_pool_size': WORKER_POOL_SIZE,
            'redis_prefix': PROJECT_PREFIX
        }
    })

@app.route('/api/metrics', methods=['POST'])
def receive_metrics():
    """Receive metrics from distributed nodes"""
    data = request.json
    data['received_at'] = time.time()
    
    # Publish to Pub/Sub for async processing
    redis_client.publish('metrics_channel', json.dumps(data))
    
    return jsonify({'status': 'received', 'latency_target': '<50ms'}), 200

@app.route('/api/stats')
def get_stats():
    """Get real-time statistics"""
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

@app.route('/api/simulate', methods=['POST'])
def simulate_load():
    """Simulate distributed nodes sending metrics"""
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
            redis_client.publish('metrics_channel', json.dumps(metric))
            time.sleep(0.001)
    
    threading.Thread(target=generate_metrics, daemon=True).start()
    
    return jsonify({'status': 'simulation_started', 'metrics': num_metrics})

@app.route('/health')
def health():
    """Health check endpoint"""
    try:
        redis_client.ping()
        return jsonify({'status': 'healthy', 'redis': 'connected'}), 200
    except:
        return jsonify({'status': 'unhealthy', 'redis': 'disconnected'}), 500

if __name__ == '__main__':
    # Initialize worker pool
    init_worker_pool()
    
    # Start health check thread
    threading.Thread(target=check_node_health, daemon=True).start()
    
    print(f"🚀 Distributed System Monitor starting...")
    print(f"📊 Configured for {MAX_NODES} nodes")
    print(f"⚡ Worker pool: {WORKER_POOL_SIZE} workers")
    print(f"🎯 Target: 1M+ metrics/hour, <50ms latency")
    print(f"🔑 Redis prefix: {PROJECT_PREFIX}")
    
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
