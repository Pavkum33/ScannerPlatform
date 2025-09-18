# Render Deployment Guide

## Overview
This guide explains how to deploy the Scanner Platform on Render with SQLite database and daily scheduled jobs.

## 1. Initial Deployment

### Step 1: Deploy to Render
1. Go to [Render Dashboard](https://dashboard.render.com)
2. Click **New +** → **Web Service**
3. Connect your GitHub repository: `Pavkum33/ScannerPlatform`
4. Render will auto-detect settings from `render.yaml`

### Step 2: Environment Variables (Optional)
In Render Dashboard → Environment:
- Add any API keys or sensitive configuration
- The app already has DHAN API credentials in code

## 2. SQLite Database Setup

### Automatic Initialization
The `build.sh` script automatically:
1. Creates database directory
2. Initializes SQLite database with schema
3. Loads F&O symbols (154 stocks)
4. Fetches last 30 days of historical data
5. Generates weekly/monthly aggregations

### Database Persistence on Render
- **Important**: SQLite database persists between deploys on Render
- Database location: `/opt/render/project/src/database/pattern_scanner.db`
- Data survives redeploys but NOT service deletion

### Manual Database Operations
If you need to reset or update the database:

```bash
# SSH into Render console (from dashboard)
# Reset database
rm database/pattern_scanner.db
# It will recreate on next deploy

# Or run manual update
cd database
python daily_eod_update.py --now
```

## 3. Setting Up Daily Scheduled Jobs

### Option 1: Using Render Cron Jobs (Paid Feature)
Render cron jobs require a paid plan. If you have one:

1. In Render Dashboard → **New +** → **Cron Job**
2. Connect same repository
3. Configure:
   - **Name**: daily-eod-update
   - **Schedule**: `0 11 * * *` (11:00 UTC = 4:30 PM IST)
   - **Command**: `python cron_daily_update.py`
   - **Runtime**: Python 3
   - **Build Command**: `pip install -r requirements.txt`

### Option 2: Using External Cron Service (Free)
Use a free service like [cron-job.org](https://cron-job.org):

1. Sign up for free account
2. Create new cron job:
   - **URL**: `https://your-app.onrender.com/api/eod-update`
   - **Method**: POST
   - **Schedule**: Daily at 4:30 PM IST
   - **Headers**: Add any auth if you implement it

### Option 3: GitHub Actions (Free)
Create `.github/workflows/daily-update.yml`:

```yaml
name: Daily EOD Update
on:
  schedule:
    - cron: '30 11 * * *'  # 11:30 UTC = 5:00 PM IST
  workflow_dispatch:  # Manual trigger

jobs:
  update:
    runs-on: ubuntu-latest
    steps:
      - name: Trigger EOD Update
        run: |
          curl -X POST https://your-app.onrender.com/api/eod-update
```

## 4. Database Loading Timeline

### First Deploy (Initial Load)
1. **Build phase** (5-10 minutes):
   - Install dependencies
   - Create database
   - Load F&O symbols
   - Fetch 30 days historical data (154 symbols)
   - Generate patterns

2. **Result**: Fully loaded database with:
   - ~4,500 daily records (30 days × 154 symbols)
   - Weekly aggregations
   - Monthly aggregations
   - Detected patterns

### Daily Updates
- **Manual**: Click "EOD Update" button in UI
- **Scheduled**: Runs at 4:30 PM IST
- **Duration**: ~2 minutes
- **Data added**: 154 new records (one per symbol)

## 5. Monitoring & Troubleshooting

### Check Database Status
```python
# In Render Shell
import sqlite3
conn = sqlite3.connect('database/pattern_scanner.db')
cur = conn.cursor()

# Check record count
cur.execute("SELECT COUNT(*) FROM daily_ohlc")
print(f"Total records: {cur.fetchone()[0]}")

# Check latest date
cur.execute("SELECT MAX(trade_date) FROM daily_ohlc")
print(f"Latest date: {cur.fetchone()[0]}")
```

### View Logs
- **Build Logs**: Render Dashboard → Service → Events → Build logs
- **Runtime Logs**: Render Dashboard → Service → Logs

### Common Issues & Solutions

1. **Database not found**
   - Solution: Redeploy to trigger `build.sh`

2. **No patterns showing**
   - Solution: Run aggregation script
   ```bash
   cd database && python generate_aggregations.py
   ```

3. **EOD data not updating**
   - Check if market was open today
   - DHAN API updates after 6 PM IST
   - Manual trigger: Click "EOD Update" button

4. **Slow initial load**
   - Normal for first deploy (fetching 30 days data)
   - Subsequent deploys are faster (database persists)

## 6. Data Persistence Strategy

### What Persists
- SQLite database file
- All historical data
- Generated patterns
- User configurations

### What Doesn't Persist
- Temporary scan results (in memory)
- Log files
- Cache files

### Backup Recommendation
For production use, consider:
1. Daily database backups to S3/CloudStorage
2. Using PostgreSQL (Render provides it)
3. Implementing data export endpoints

## 7. Performance Tips

1. **Free Tier Limitations**:
   - App sleeps after 15 mins inactivity
   - First request after sleep is slow (~30 seconds)
   - Solution: Use uptime monitoring to keep alive

2. **Database Optimization**:
   - Current size: ~8-10 MB (efficient)
   - Indexes already optimized
   - Vacuum periodically if needed

3. **Scaling Considerations**:
   - Move to PostgreSQL for production
   - Use Redis for caching
   - Consider CDN for static assets

## 8. Deployment Commands Summary

```bash
# Deploy to Render (automatic from GitHub)
git push origin master

# Manual database update (in Render shell)
cd database && python daily_eod_update.py --now

# Check database status
cd database && python check_progress.py

# Generate patterns
cd database && python generate_aggregations.py

# Test the app locally
python app.py
```

## Support & Monitoring

- **Render Status**: https://status.render.com
- **App Health**: Check `/` endpoint
- **Database Health**: Check `/api/eod-update/status`

---

*Note: Free tier apps on Render spin down after 15 minutes of inactivity. First request after spin down takes 30+ seconds.*