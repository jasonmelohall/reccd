# Reccd Items API

FastAPI backend that orchestrates the complete items recommendation pipeline.

## Quick Start

```bash
cd items/api
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Create .env file with your credentials (see .env.example)

# Start server
uvicorn main:app --reload
```

Visit http://localhost:8000/docs for interactive API documentation.

## API Endpoints

**POST /api/search** - Initiate search pipeline
```json
{"search_term": "protein powder", "user_id": 1}
```
Runs all 5 Python scripts in sequence (~2-3 minutes).

**GET /api/results** - Get ranked recommendations
```
/api/results?search_term=protein%20powder&user_id=1
```
Returns products ranked by personalized Reccd scores.

**GET /** - Health check

## What It Does

When `/api/search` is called, the backend:
1. Updates search term in `reccd_items.py`
2. Runs `1_search_items_rainforest.py` - Amazon search
3. Runs `2_items_get_listed_date_keepa.py` - Product dates
4. Runs `3_get_first_available_rainforest.py` - Additional data
5. Runs `8_regression_items.py` - Update ML coefficients
6. Runs `9_reccd_items.py` - Calculate recommendations

All your existing Python scripts still work standalone!

## Configuration

Edit `.env` file:
- Database credentials
- Rainforest API key
- Keepa API key
- User email

## Notes

- Images served from Rainforest API URLs (Amazon CDN)
- Pipeline takes 2-3 minutes per search
- Supports multiple search terms
- ML model updates with each search



