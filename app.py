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

# Redis Configuration
REDIS_URL = os.environ.get('REDIS_URL', 'redis://localhost:6379')

try:
    redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    redis_client.ping()
    print("✅ Redis connected successfully!")
except Exception as e:
    print(f"❌ Redis connection error: {e}")
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
        
    def process_metric(self, data):
        """Process individual metric with <50ms latency"""
        if not redis_client:
            return False
            
        start = time.time()
        
        try:
            metric_key = f"metric:{data['node_id']}:{data['timestamp']}"
            redis_client.setex(metric_key, 3600, json.dumps(data))
            self.update_stats(data)
            
            latency = (time.time() - start) * 1000
            return latency < 50
        except Exception as e:
            print(f"Error processing metric: {e}")
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
            print(f"Error updating stats: {e}")

processor = MetricsProcessor()

class Worker(threading.Thread):
    def __init__(self, worker_id):
        super().__init__(daemon=True)
        self.worker_id = worker_id
        self.is_running = True
        
    def run(self):
        """Worker thread for async processing"""
        if not redis_client:
            print(f"Worker {self.worker_id}: Redis not available")
            return
            
        try:
            worker_pubsub = redis_client.pubsub()
            worker_pubsub.subscribe('metrics_channel')
            
            if self.worker_id == 0:  # Only first worker prints
                print(f"✅ Workers subscribed to metrics_channel")
            
            for message in worker_pubsub.listen():
                if not self.is_running:
                    break
                if message['type'] == 'message':
                    try:
                        data = json.loads(message['data'])
                        processor.process_metric(data)
                    except Exception as e:
                        if self.worker_id == 0:  # Only first worker prints errors
                            print(f"Worker error: {e}")
        except Exception as e:
            print(f"Worker {self.worker_id} fatal error: {e}")

def init_worker_pool():
    """Initialize 100 workers for 10K concurrent events"""
    if not redis_client:
        print("⚠️ Cannot initialize worker pool - Redis not connected")
        return
        
    for i in range(WORKER_POOL_SIZE):
        worker = Worker(i)
        worker.start()
        processor.worker_pool.append(worker)
    
    time.sleep(1)  # Give workers time to subscribe
    print(f"✅ Initialized {WORKER_POOL_SIZE} workers")

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
            print(f"Health check error: {e}")
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
        'uptime': int(time.time() - processor.start_time)
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
        print(f"Error getting stats: {e}")
    
    return jsonify(stats)

@app.route('/api/simulate', methods=['POST'])
def simulate_load():
    """Simulate distributed nodes sending metrics"""
    if not redis_client:
        return jsonify({'status': 'error', 'message': 'Redis not available'}), 503
        
    num_metrics = request.json.get('count', 1000)
    
    def generate_metrics():
        print(f"🚀 Starting simulation: {num_metrics} metrics")
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
                time.sleep(0.001)
            except Exception as e:
                print(f"Error in simulation: {e}")
                break
        print(f"✅ Simulation complete")
    
    threading.Thread(target=generate_metrics, daemon=True).start()
    return jsonify({'status': 'simulation_started', 'metrics': num_metrics})

if __name__ == '__main__':
    print("\n" + "="*60)
    print("🚀 Distributed System Monitor Starting...")
    print("="*60)
    
    if redis_client:
        try:
            redis_client.ping()
            print("✅ Redis: Connected")
            print(f"📍 Redis URL: {REDIS_URL[:30]}...")
        except Exception as e:
            print(f"❌ Redis: Connection failed - {e}")
    else:
        print("❌ Redis: Not configured")
    
    print(f"📊 Nodes: {MAX_NODES}")
    print(f"⚡ Worker Pool: {WORKER_POOL_SIZE} workers")
    print(f"🎯 Target: 1M+ metrics/hour, <50ms latency")
    print("="*60 + "\n")
    
    init_worker_pool()
    threading.Thread(target=check_node_health, daemon=True).start()
    
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
