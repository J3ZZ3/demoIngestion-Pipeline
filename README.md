# Ingestion Pipeline

CSV data ingestion.

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Gmail/IMAP    │───▶│  Ingestion Worker │───▶│  CSV Processor  │───▶│   SQLite DB     │
│  jmashoana@...  │    │   (Python)       │    │   (Validator)   │    │  (Storage)      │
└─────────────────┘    └──────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │                       │
         ▼                       ▼                       ▼                       ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Email arrives   │    │  Polls for new   │    │  Validates &    │    │  Stores with    │
│  with CSV file   │    │  unread emails   │    │  normalizes     │    │  deduplication  │
└─────────────────┘    └──────────────────┘    └─────────────────┘    └─────────────────┘
```

## Core Features

### Correctness
- Data validation and normalization
- Type conversion and timezone handling
- Business rule enforcement

### Idempotency
- File-level SHA256 deduplication
- Row-level unique constraints
- Safe reprocessing

### Traceability
- Correlation IDs for end-to-end tracking
- Complete audit trail
- File-to-transaction linking

### Failure Safety
- Robust error handling
- Error isolation
- Automatic recovery

## Quick Start

### One-Click Setup

**Windows:**
```powershell
.\setup_demo_windows.ps1
```

**Linux/Mac:**
```bash
./setup_demo.sh
```

### Manual Setup

```bash
# Install dependencies
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt

# Configure Gmail
cp .env.example .env
# Edit .env with your Gmail credentials

# Initialize database
python init_database.py

# Start demo
python start_demo.py
```

## How to Use

1. **Send CSV files** to `jmashoana@gmail.com`
2. **Watch real-time processing** on the dashboard
3. **Observe metrics** and analytics

### Test Files
- `test_batch_01_high_success.csv` (100% success)
- `test_batch_02_mixed_results.csv` (50% success)
- `test_batch_03_high_failure.csv` (0% success)
- `test_batch_04_edge_cases.csv` (edge cases)

## Configuration

### Environment Variables (.env)
```env
# Gmail IMAP Configuration
IMAP_SERVER=imap.gmail.com
IMAP_PORT=993
IMAP_USERNAME=your_email@gmail.com
IMAP_PASSWORD=your_app_password

# Database
DATABASE_URL=sqlite:///data/mazonda_gas.db
```

### Gmail App Password
1. Visit: https://myaccount.google.com/apppasswords
2. Generate 16-character app password
3. Add to `.env` file

## Dashboard Features

- Real-time KPIs
- Scale performance leaderboard
- Processing activity feed
- System health monitoring

## File Structure

```
mazonda-gas/
├──  setup_demo_windows.ps1    # Windows setup
├──  setup_demo.sh              # Linux/Mac setup
├──  demo_dashboard.py          # Professional dashboard
├──  start_demo.py              # Demo launcher
├──  process_csv_email.py       # Manual processor
├──  run_continuous_worker.py   # Background worker
├──  data/                      # Demo test files
├──  src/                       # Core components
├──  logs/                      # Processing logs
├──  .env.example               # Config template
└──  requirements.txt           # Dependencies
```

## Demo Flow

### Introduction (30s)
Industrial ingestion pipeline with enterprise-grade reliability

### Demo Files (2min)
1. High Success: Perfect processing scenario
2. Mixed Results: Realistic performance
3. High Failure: Error handling demonstration
4. Edge Cases: Validation and unusual values

### Technical Deep Dive (1min)
- SHA256 file hashing
- Database constraints
- Correlation IDs
- Real-time monitoring

### Results (1min)
- Performance metrics
- Scale analytics
- Success rates
- Audit trail

## Production Deployment

```bash
# Docker
docker-compose up -d --build

# Monitoring
python run_continuous_worker.py  # Background processing
python demo_dashboard.py         # Real-time dashboard
```

## Support

### Check System Status
```bash
python -c "
from src.database.sqlite_connection import sqlite_manager
print('Files:', sqlite_manager.execute_query('SELECT COUNT(*) as count FROM ingestion_files')[0]['count'])
print('Transactions:', sqlite_manager.execute_query('SELECT COUNT(*) as count FROM scale_transactions')[0]['count'])
"
```

### Troubleshooting
1. **IMAP Issues**: Check Gmail app password
2. **Database Issues**: Verify SQLite permissions
3. **Docker Issues**: Check container status
