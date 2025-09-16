## Installation

1. Clone or create your project directory
2. Make sure you have your nasdaq_companies.py file in the same directory
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Running the API

```bash
python app.py
```

The API will start on http://localhost:5000

## API Endpoints

### 1. List all companies
```
GET /companies
```
Returns all available NASDAQ companies with their symbols and developer names.

### 2. Get reviews in JSON format
```
GET /<SYMBOL>/reviews
```
Example: `GET /AAPL/reviews`
Returns all 2025 reviews for Apple's apps in JSON format.

### 3. Download CSV file
```
GET /<SYMBOL>/reviews/csv
```
Example: `GET /AAPL/reviews/csv`
Downloads a CSV file with all 2025 reviews for the company's apps.

## CSV Columns

The generated CSV will include:
- company_symbol: NASDAQ symbol (e.g., "AAPL")
- developer_name: Play Store developer name
- app_id: Google Play Store app ID
- app_title: Name of the app
- review_date: Date of review (YYYY-MM-DD)
- score: Review score (1-5 stars)
- review_text: Full review content
- reviewer_name: Name of the reviewer
- helpful_count: Number of helpful votes
- review_id: Unique review identifier

## Example Usage

```bash
# Get Apple's reviews as CSV
curl "http://localhost:5000/AAPL/reviews/csv" -o apple_reviews_2025.csv

# Get Microsoft's reviews in JSON
curl "http://localhost:5000/MSFT/reviews"

# List all available companies
curl "http://localhost:5000/companies"
```

## Rate Limiting

The API includes built-in rate limiting (1.5 seconds between requests) to avoid being blocked by Google Play Store.

## Notes

- Only reviews from 2025 are included
- The API searches for apps by exact developer name match
- All apps from the developer are included in one CSV
- Reviews are fetched fresh each time (no caching)