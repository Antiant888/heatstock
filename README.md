# Gelonghui HK Stock Live Data Scraper

A robust web scraper for fetching live stock data from Gelonghui API, with MySQL database storage for deployment on Railway.

## Features

- **Incremental Fetching**: Fetches new data by tracking timestamps
- **Duplicate Detection**: Prevents duplicate entries in database
- **MySQL Storage**: Stores data in MySQL database (Railway compatible)
- **Checkpoint System**: Resumes from last fetched timestamp
- **Automatic Deployment**: Dockerized for Railway deployment

## Project Structure

```
├── scraper.py          # Main scraper script
├── database.py         # Database configuration and models
├── requirements.txt    # Python dependencies
├── Dockerfile          # Docker configuration
├── railway.toml        # Railway deployment config
└── README.md          # This file
```

## Database Schema

The scraper stores data in a MySQL table with the following structure:

| Field | Type | Description |
|-------|------|-------------|
| id | VARCHAR(50) | Primary key (from API) |
| title | TEXT | News title |
| create_timestamp | BIGINT | Creation timestamp |
| update_timestamp | BIGINT | Update timestamp |
| count | TEXT | JSON string (read, comment, favorite, etc.) |
| statistic | TEXT | JSON string (isFavorite, isLike, etc.) |
| content | TEXT | News content |
| content_prefix | TEXT | Content prefix |
| related_stocks | TEXT | JSON string (related stocks) |
| related_infos | TEXT | JSON string (related info) |
| pictures | TEXT | JSON string (picture URLs) |
| related_articles | TEXT | JSON string (related articles) |
| source | TEXT | JSON string (source info) |
| interpretation | TEXT | Interpretation |
| level | BIGINT | News level |
| route | TEXT | Route URL |
| close_comment | BOOLEAN | Close comment flag |
| created_at | DATETIME | Record creation time |
| updated_at | DATETIME | Record update time |

## Deployment to Railway

### Prerequisites

1. Railway account (https://railway.app)
2. GitHub repository with this code
3. MySQL database on Railway

### Step 1: Create MySQL Database on Railway

1. Go to Railway dashboard
2. Click "New Project"
3. Select "Provision MySQL"
4. Wait for database to be ready
5. Click on the MySQL service
6. Go to "Variables" tab
7. Copy the `DATABASE_URL` variable

### Step 2: Deploy to Railway

1. Go to Railway dashboard
2. Click "New Project"
3. Select "Deploy from GitHub repo"
4. Select this repository
5. Railway will automatically detect the Dockerfile

### Step 3: Set Environment Variables

1. In Railway project, go to "Variables" tab
2. Add the following variable:
   - `DATABASE_URL`: The MySQL connection URL from Step 1

### Step 4: Deploy

1. Railway will automatically build and deploy
2. Check the "Deployments" tab for progress
3. Check "Logs" tab to see scraper running

## Local Development

### Prerequisites

- Python 3.11+
- MySQL database

### Setup

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd gelonghui-scraper
   ```

2. Create virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Set environment variables:
   ```bash
   # For Railway MySQL
   export DATABASE_URL="mysql+pymysql://user:password@host:port/database"
   
   # Or use individual variables
   export MYSQL_HOST=localhost
   export MYSQL_PORT=3306
   export MYSQL_USER=root
   export MYSQL_PASSWORD=yourpassword
   export MYSQL_DATABASE=gelonghui
   ```

5. Run the scraper:
   ```bash
   python scraper.py
   ```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| DATABASE_URL | MySQL connection URL (Railway) | - |
| MYSQL_HOST | MySQL host | localhost |
| MYSQL_PORT | MySQL port | 3306 |
| MYSQL_USER | MySQL user | root |
| MYSQL_PASSWORD | MySQL password | (empty) |
| MYSQL_DATABASE | MySQL database name | gelonghui |

## Monitoring

### Check Logs

In Railway:
1. Go to your project
2. Click on the scraper service
3. Go to "Deployments" tab
4. Click on a deployment
5. View "Logs" tab

### Check Database

You can connect to the MySQL database using:
- Railway's built-in MySQL client
- MySQL Workbench
- Any MySQL client with the connection URL

Example query:
```sql
SELECT COUNT(*) FROM hk_stock_lives;
SELECT * FROM hk_stock_lives ORDER BY created_at DESC LIMIT 10;
```

## Troubleshooting

### Scraper not starting

1. Check logs in Railway
2. Verify DATABASE_URL is set correctly
3. Check if MySQL database is running

### Duplicate key errors

The scraper handles duplicates automatically, but if you see errors:
1. Check the database for existing records
2. Verify the ID field is unique

### Rate limiting

If you get rate limited:
1. The scraper has built-in delays (3.2 seconds between requests)
2. If needed, increase the delay in `scraper.py`

## License

MIT License

## Author

jasperchan