# Video Recommendation System (On-Premise)

Production-ready video recommendation system using Redis/Dragonfly for high-performance content delivery with popularity and freshness feeds.

## Features

- **60/40 Content Mix**: 60% popularity-based, 40% freshness-based recommendations
- **Atomic Operations**: All critical operations use Lua scripts for consistency
- **Bloom Filters**: Efficient duplicate detection with minimal memory usage
- **Auto-refill**: Automatic pool replenishment when content runs low
- **Production Ready**: No simulation data, ready for real BigQuery integration

## Quick Start

### 1. Install Dependencies

```bash
# Create and activate virtual environment
python -m venv venv
source venv/bin/activate

# Install packages using uv (recommended)
uv pip install -e .

# Or using regular pip
pip install -e .
```

### 2. Configure Redis Connection

The application needs a Redis/Dragonfly instance. You have two options:

#### Option A: Use Production Redis
Set environment variables for your production Redis instance:

```bash
export DRAGONFLY_HOST=your-redis-host
export DRAGONFLY_PORT=6379
export DRAGONFLY_PASSWORD=your-redis-password
export DRAGONFLY_CLUSTER_ENABLED=false  # or true
export DRAGONFLY_TLS_ENABLED=false     # or true for production
```

#### Option B: Local Development (Optional)
For local development, use the provided Docker setup:

```bash
# Start local Dragonfly instance
cd redis-local-setup
docker compose up -d
cd ..

# Set local environment variables
export DRAGONFLY_HOST=localhost
export DRAGONFLY_PORT=6379
export DRAGONFLY_PASSWORD=redispass
export DRAGONFLY_CLUSTER_ENABLED=false
```

See `redis-local-setup/README.md` for detailed local setup instructions.

### 3. Run the API Server

```bash
# Activate virtual environment
source venv/bin/activate

# Run the application
python src/api_server.py
```

The server will start at `http://localhost:8000`

## API Endpoints

### Get Recommendations
```bash
# Get 100 mixed recommendations (60% popularity, 40% freshness)
curl http://localhost:8000/recommend/user_123?count=100

# Get specific feed type
curl http://localhost:8000/recommend/user_123?rec_type=popularity&count=50
```

### Health Check
```bash
curl http://localhost:8000/health
```

### Feed Statistics
```bash
curl http://localhost:8000/feed-stats/user_123
```

### API Documentation
- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`

## Production Configuration

### Environment Variables

```bash
# Redis/Dragonfly Configuration (Required)
DRAGONFLY_HOST=<production-redis-host>
DRAGONFLY_PORT=<production-redis-port>
DRAGONFLY_PASSWORD=<production-redis-password>
DRAGONFLY_CLUSTER_ENABLED=true/false
DRAGONFLY_TLS_ENABLED=true  # Enable for production

# Optional Configuration
LOG_LEVEL=INFO
API_PORT=8000
API_HOST=0.0.0.0

# Benchmarking (optional, for testing only)
BENCHMARK_ENABLED=true
BENCHMARK_ITERATIONS=1000
BENCHMARK_BATCH_SIZE=100
```

### Data Integration

The system is ready to receive data from BigQuery or other sources. Implement the following functions in `src/api_server.py`:

1. `fetch_from_bigquery(bucket)` - Fetch popularity bucket videos
2. `fetch_fresh_from_bigquery(window)` - Fetch freshness window videos
3. `fetch_fallback_from_bigquery()` - Fetch fallback pool videos
4. `fetch_user_watch_history_from_bigquery(user_id)` - Fetch user watch history

## Architecture

### Core Components

- **RedisLayer** (`src/main.py`): Handles all Redis operations with Lua scripts
- **VideoMixer** (`src/mixer.py`): Mixes content with 60/40 popularity/freshness ratio
- **API Server** (`src/api_server.py`): FastAPI server with background jobs
- **Config** (`src/config.py`): Centralized configuration

### Data Flow

```
User Request → API → Mixer → RedisLayer → Redis
                        ↓
                  60% Popularity
                  40% Freshness
                        ↓
                  Mixed Results → User
```

## Development

### Running Tests

```bash
source venv/bin/activate

# Test redis utils
python -m unittest src.tests.utils.test_redis_utils -v

# Test common utils
python -m unittest src.tests.utils.test_common_utils -v

# Run all tests
python -m unittest discover src.tests -v
```

### Testing the Mixer

```bash
# Test the production mixer standalone
python src/mixer.py
```

### Populate Redis with Dummy Data

```bash
# Populate Redis/Dragonfly with dummy data (all data expires in 2 hours)
python src/tests/dummy/test_redis_data.py
```

### Project Structure

```
├── src/
│   ├── api_server.py        # Main API server
│   ├── mixer.py             # Production video mixer (60/40 split)
│   ├── mixer_archive.py     # Archived mixer with simulation
│   ├── main.py              # Redis layer with Lua scripts
│   ├── config.py            # Configuration constants
│   └── utils/               # Utility functions
├── redis-local-setup/       # Local Redis setup (dev only)
│   ├── docker-compose.yml   # Dragonfly configuration
│   └── README.md           # Local setup instructions
├── tests/                   # Unit and integration tests
└── local_poc/              # Local proof of concept scripts
```

## Background Jobs

The API server runs these background jobs automatically:

- **Global Pool Refresh** (hourly): Updates popularity/freshness pools
- **Bloom Filter Sync** (30 min): Syncs user watch history
- **Feed Refill** (15 min): Proactive refill of low user feeds

## Dragonfly Bloom Filter Limitations

Dragonfly supports core Bloom filter operations but not all RedisBloom module commands:

### ✅ Supported Commands:
```bash
BF.RESERVE key error_rate capacity  # Create Bloom filter
BF.ADD key item                     # Add item
BF.EXISTS key item                  # Check if item exists
```

### ❌ Not Supported:
```bash
BF.INFO key        # Get filter information
BF.CARD key        # Get cardinality (item count)
BF.MADD key items  # Add multiple items
BF.MEXISTS key items # Check multiple items
```

**Note**: Our Python wrapper provides optimized fallback implementations for unsupported commands using Redis pipelining:
- `bf_madd()` → Pipelined `BF.ADD` commands
- `bf_mexists()` → Pipelined `BF.EXISTS` commands

## Monitoring

- Health endpoint: `/health`
- Metrics endpoint: `/metrics` (Prometheus format - to be implemented)
- Feed statistics: `/feed-stats/{user_id}`

## Docker Containerization

When containerizing this application:

1. **Do NOT include** the `redis-local-setup/` directory in your Docker image
2. Configure Redis connection via environment variables
3. The application is stateless and can be scaled horizontally

Example Dockerfile structure:
```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY src/ ./src/
COPY requirements.txt .
RUN pip install -r requirements.txt
CMD ["python", "src/api_server.py"]
```

## Support

For issues or questions, please create an issue in the repository.