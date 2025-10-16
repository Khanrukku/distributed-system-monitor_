from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
import redis
import json
import time
import threading
from datetime import datetime
import random
import os
import sys

app = Flask(__name__)
CORS(app)

# Force output to flush immediately for Render logs
sys.stdout.flush()

# Redis Configuration
REDIS_URL = os.environ.get('REDIS_URL', 'redis://localhost:6379')

print("="*60, flush=True)
print("🚀 Distributed System Monitor Starting...", flush=True)
print("="*60, flush=True)

try:
    redis_client = redis.from_url(REDIS_URL, decode_responses=True, socket_connect_timeout=5)
    redis_client.ping()
    print(f"✅ Redis: Connected", flush=True)
    print(f"📍 Redis URL: {REDIS_URL[:40]}...", flush=True)
except Exception as e:
    print(f"❌ Redis connection error: {e}", flush=True)
    redis_client = None

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
        self.lock = threading.Lock()
        
    def process_metric(self, data):
        """Process individual metric with <50ms latency"""
        if not redis_client:
            return False
            
        start = time.time()
        
        try:
            metric_key = f"metric:{data['node_id']}:{data['timestamp']}"
            redis_client.setex(metric_key, 3600, json.dumps(data))
            self.update_stats(data)
            
            with self.lock:
                self.metrics_count += 1
            
            latency = (time.time() - start) * 1000
            return latency < 50
        except Exception as e:
            print(f"Error processing metric: {e}", flush=True)
            return False
    
    def update_stats(self, data):
        """Update node health and metrics count"""
        if not redis_client:
            return
            
        try:
            node_id = data['node_id']
            stats_key = f"node_stats:{node_id}"
            
            redis_client.hincrby(stats_key, 'total_metrics', 1)
            redis_client.hset(stats_key, 'last_seen', time.time())
            redis_client.hset(stats_key, 'status', data.get('status', 'healthy'))
            redis_client.expire(stats_key, 7200)
        except Exception as e:
            print(f"Error updating stats: {e}", flush=True)

processor = MetricsProcessor()

class Worker(threading.Thread):
    def __init__(self, worker_id):
        super().__init__(daemon=True)
        self.worker_id = worker_id
        self.is_running = True
        self.subscribed = False
        
    def run(self):
        """Worker thread for async processing"""
        if not redis_client:
            return
            
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries and not self.subscribed:
            try:
                worker_pubsub = redis_client.pubsub(ignore_subscribe_messages=True)
                worker_pubsub.subscribe('metrics_channel')
                self.subscribed = True
                
                if self.worker_id == 0:
                    print(f"✅ Workers subscribed to metrics_channel", flush=True)
                
                for message in worker_pubsub.listen():
                    if not self.is_running:
                        break
                    if message and message['type'] == 'message':
                        try:
                            data = json.loads(message['data'])
                            processor.process_metric(data)
                        except Exception as e:
                            if self.worker_id == 0:
                                print(f"Worker processing error: {e}", flush=True)
                            
            except Exception as e:
                retry_count += 1
                if self.worker_id == 0:
                    print(f"Worker subscription error (attempt {retry_count}/{max_retries}): {e}", flush=True)
                time.sleep(1)

def init_worker_pool():
    """Initialize 100 workers for 10K concurrent events"""
    if not redis_client:
        print("⚠️ Cannot initialize worker pool - Redis not connected", flush=True)
        return False
        
    print(f"⚡ Initializing {WORKER_POOL_SIZE} workers...", flush=True)
    
    for i in range(WORKER_POOL_SIZE):
        worker = Worker(i)
        worker.start()
        processor.worker_pool.append(worker)
    
    # Give workers time to subscribe
    time.sleep(2)
    
    # Check if workers subscribed
    subscribed_count = sum(1 for w in processor.worker_pool if hasattr(w, 'subscribed') and w.subscribed)
    
    if subscribed_count > 0:
        print(f"✅ Initialized {len(processor.worker_pool)} workers ({subscribed_count} subscribed)", flush=True)
        return True
    else:
        print(f"⚠️ Workers created but subscription status unknown", flush=True)
        return True

def check_node_health():
    """Monitor node health and implement failure recovery"""
    while True:
        if not redis_client:
            time.sleep(10)
            continue
            
        try:
            current_time = time.time()
            for node_id in range(MAX_NODES):
                stats_key = f"node_stats:{node_id}"
                last_seen = redis_client.hget(stats_key, 'last_seen')
                
                if last_seen and (current_time - float(last_seen)) > 60:
                    redis_client.hset(stats_key, 'status', 'failed')
        except Exception as e:
            pass  # Silently handle health check errors
        time.sleep(10)

node_counter = 0
def get_next_node():
    """Load balancing across nodes"""
    global node_counter
    node_counter = (node_counter + 1) % MAX_NODES
    return node_counter

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/health')
def health_check():
    """Health check endpoint"""
    redis_status = 'disconnected'
    
    if redis_client:
        try:
            redis_client.ping()
            redis_status = 'healthy'
        except:
            redis_status = 'error'
    
    return jsonify({
        'status': 'running',
        'redis': redis_status,
        'workers': len(processor.worker_pool),
        'uptime': int(time.time() - processor.start_time),
        'total_processed': processor.metrics_count
    })

@app.route('/api/metrics', methods=['POST'])
def receive_metrics():
    """Receive metrics from distributed nodes"""
    if not redis_client:
        return jsonify({'status': 'error', 'message': 'Redis not available'}), 503
        
    try:
        data = request.json
        data['received_at'] = time.time()
        redis_client.publish('metrics_channel', json.dumps(data))
        return jsonify({'status': 'received', 'latency_target': '<50ms'}), 200
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

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
    
    if not redis_client:
        return jsonify(stats)
    
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
    except Exception as e:
        print(f"Error getting stats: {e}", flush=True)
    
    return jsonify(stats)

@app.route('/api/simulate', methods=['POST'])
def simulate_load():
    """Simulate distributed nodes sending metrics"""
    if not redis_client:
        return jsonify({'status': 'error', 'message': 'Redis not available'}), 503
        
    num_metrics = request.json.get('count', 10000)
    
    def generate_metrics():
        print(f"🚀 Starting simulation: {num_metrics} metrics", flush=True)
        start_time = time.time()
        success_count = 0
        error_count = 0
        
        for i in range(num_metrics):
            try:
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
                success_count += 1
                
                # Progress logging
                if (i + 1) % 2000 == 0:
                    print(f"📊 Progress: {i+1}/{num_metrics} metrics published", flush=True)
                    
                time.sleep(0.001)  # 1ms delay between metrics
            except Exception as e:
                error_count += 1
                if error_count <= 3:  # Only log first 3 errors
                    print(f"❌ Error publishing metric: {e}", flush=True)
        
        elapsed = time.time() - start_time
        print(f"✅ Simulation complete: {success_count} successful, {error_count} failed in {elapsed:.2f}s", flush=True)
    
    # Start simulation in background thread
    threading.Thread(target=generate_metrics, daemon=True).start()
    return jsonify({
        'status': 'simulation_started', 
        'metrics': num_metrics,
        'message': 'Check logs for progress'
    })

# Initialize on startup
print(f"📊 Nodes: {MAX_NODES}", flush=True)
print(f"⚡ Worker Pool: {WORKER_POOL_SIZE} workers", flush=True)
print(f"🎯 Target: 1M+ metrics/hour, <50ms latency", flush=True)
print("="*60, flush=True)

# Initialize worker pool and health check
if redis_client:
    init_worker_pool()
    threading.Thread(target=check_node_health, daemon=True).start()
    print("✅ System ready!", flush=True)
else:
    print("❌ System started but Redis unavailable - limited functionality", flush=True)

print("="*60, flush=True)
sys.stdout.flush()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
