"""
Google Play Store Reviews API
Flask API to fetch reviews from NASDAQ companies' apps
"""

from flask import Flask, jsonify, send_file, request
from google_play_scraper import app, search, reviews_all
import pandas as pd
import time
import os
from datetime import datetime, timezone
import tempfile
import logging
from typing import List, Dict, Any
import re

# Import NASDAQ companies data
from nasdaq_companies import NASDAQ_100_COMPANIES, get_company_info

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PlayStoreReviewsFetcher:
    def __init__(self, rate_limit_delay: float = 1.0):
        """
        Initialize the reviews fetcher
        
        Args:
            rate_limit_delay: Delay between requests in seconds
        """
        self.rate_limit_delay = rate_limit_delay
        self.current_year = datetime.now().year
    
    def search_apps_by_developer(self, developer_name: str) -> List[Dict[str, Any]]:
        """
        Search for apps by developer name
        
        Args:
            developer_name: Name of the developer to search for
            
        Returns:
            List of app dictionaries with basic info
        """
        try:
            # Search for apps by developer
            search_results = search(developer_name, n_hits=50)
            
            # Filter apps by exact developer match (case insensitive)
            developer_apps = []
            for app_info in search_results:
                if app_info.get('developer', '').lower() == developer_name.lower():
                    developer_apps.append({
                        'appId': app_info['appId'],
                        'title': app_info['title'],
                        'developer': app_info['developer'],
                        'score': app_info.get('score', 0),
                        'installs': app_info.get('installs', '0+')
                    })
            
            logger.info(f"Found {len(developer_apps)} apps for developer: {developer_name}")
            return developer_apps
            
        except Exception as e:
            logger.error(f"Error searching apps for developer {developer_name}: {str(e)}")
            return []
    
    def fetch_app_reviews(self, app_id: str, app_title: str = None) -> List[Dict[str, Any]]:
        """
        Fetch all 2025 reviews for a specific app
        
        Args:
            app_id: Google Play Store app ID
            app_title: App title for logging purposes
            
        Returns:
            List of review dictionaries
        """
        try:
            logger.info(f"Fetching reviews for app: {app_title or app_id}")
            
            # Fetch all reviews for the app
            app_reviews = reviews_all(
                app_id,
                sleep_milliseconds=int(self.rate_limit_delay * 1000),
                lang='en',  # You can modify this or make it configurable
                country='us'  # You can modify this or make it configurable
            )
            
            # Filter reviews from current year (2025)
            current_year_reviews = []
            for review in app_reviews:
                review_date = review.get('at')
                if review_date and review_date.year == self.current_year:
                    # Structure the review data
                    structured_review = {
                        'app_id': app_id,
                        'app_title': app_title or app_id,
                        'review_date': review_date.strftime('%Y-%m-%d'),
                        'score': review.get('score', 0),
                        'review_text': review.get('content', ''),
                        'reviewer_name': review.get('userName', 'Anonymous'),
                        'helpful_count': review.get('thumbsUpCount', 0),
                        'review_id': review.get('reviewId', '')
                    }
                    current_year_reviews.append(structured_review)
            
            logger.info(f"Found {len(current_year_reviews)} reviews from {self.current_year} for {app_title or app_id}")
            return current_year_reviews
            
        except Exception as e:
            logger.error(f"Error fetching reviews for app {app_id}: {str(e)}")
            return []
    
    def fetch_developer_reviews(self, developer_name: str, company_symbol: str = None) -> List[Dict[str, Any]]:
        """
        Fetch all 2025 reviews for all apps from a developer
        
        Args:
            developer_name: Name of the developer
            company_symbol: NASDAQ symbol for reference
            
        Returns:
            List of all reviews from all developer's apps
        """
        all_reviews = []
        
        # Find all apps by this developer
        apps = self.search_apps_by_developer(developer_name)
        
        if not apps:
            logger.warning(f"No apps found for developer: {developer_name}")
            return []
        
        # Fetch reviews for each app
        for app_info in apps:
            app_reviews = self.fetch_app_reviews(
                app_info['appId'], 
                app_info['title']
            )
            
            # Add company context to each review
            for review in app_reviews:
                review['company_symbol'] = company_symbol or 'N/A'
                review['developer_name'] = developer_name
            
            all_reviews.extend(app_reviews)
            
            # Rate limiting between apps
            time.sleep(self.rate_limit_delay)
        
        return all_reviews

# Initialize the fetcher
fetcher = PlayStoreReviewsFetcher(rate_limit_delay=1.5)

@app.route('/')
def home():
    """API home endpoint with usage information"""
    return jsonify({
        "message": "Google Play Store Reviews API",
        "version": "1.0.0",
        "endpoints": {
            "/companies": "GET - List all available NASDAQ companies",
            "/<symbol>/reviews": "GET - Get reviews for a company's apps",
            "/<symbol>/reviews/csv": "GET - Download CSV of company's app reviews",
            "/all-companies/reviews/csv": "GET - Download CSV with ALL NASDAQ companies' reviews",
            "/batch/reviews/csv": "POST - Download CSV for specific companies (JSON body required)"
        },
        "year": f"Reviews are filtered for year {fetcher.current_year}",
        "bulk_operations": {
            "all_companies": "Downloads reviews for all ~100 NASDAQ companies (may take 30+ minutes)",
            "batch": "Send POST with JSON body: {'symbols': ['AAPL', 'MSFT', 'GOOGL']}"
        }
    })

@app.route('/companies')
def list_companies():
    """List all available NASDAQ companies"""
    companies = []
    for symbol, info in NASDAQ_100_COMPANIES.items():
        companies.append({
            "symbol": symbol,
            "company_name": info[0],
            "developer_name": info[1],
            "subsidiaries_count": len(info[2])
        })
    
    return jsonify({
        "total_companies": len(companies),
        "companies": companies
    })

@app.route('/<string:symbol>/reviews')
def get_company_reviews(symbol):
    """Get all reviews for a company's apps in JSON format"""
    company_info = get_company_info(symbol)
    
    if not company_info:
        return jsonify({
            "error": f"Company symbol '{symbol}' not found",
            "available_symbols": list(NASDAQ_100_COMPANIES.keys())
        }), 404
    
    try:
        # Fetch reviews for the main developer
        all_reviews = fetcher.fetch_developer_reviews(
            company_info['play_store_developer'],
            symbol
        )
        
        # Optionally, also search subsidiary developers
        # This is commented out to avoid too many requests, but you can enable it
        """
        for subsidiary in company_info['subsidiaries']:
            if subsidiary != company_info['play_store_developer']:
                subsidiary_reviews = fetcher.fetch_developer_reviews(subsidiary, symbol)
                all_reviews.extend(subsidiary_reviews)
        """
        
        return jsonify({
            "company": company_info['company_name'],
            "symbol": symbol,
            "developer": company_info['play_store_developer'],
            "total_reviews": len(all_reviews),
            "year": fetcher.current_year,
            "reviews": all_reviews
        })
        
    except Exception as e:
        logger.error(f"Error fetching reviews for {symbol}: {str(e)}")
        return jsonify({
            "error": f"Failed to fetch reviews for {symbol}",
            "details": str(e)
        }), 500

@app.route('/<string:symbol>/reviews/csv')
def download_company_reviews_csv(symbol):
    """Download all reviews for a company's apps as CSV"""
    company_info = get_company_info(symbol)
    
    if not company_info:
        return jsonify({
            "error": f"Company symbol '{symbol}' not found"
        }), 404
    
    try:
        # Fetch reviews
        all_reviews = fetcher.fetch_developer_reviews(
            company_info['play_store_developer'],
            symbol
        )
        
        if not all_reviews:
            return jsonify({
                "message": f"No reviews found for {symbol} in {fetcher.current_year}"
            }), 404
        
        # Create DataFrame
        df = pd.DataFrame(all_reviews)
        
        # Reorder columns for better CSV structure
        column_order = [
            'company_symbol', 'developer_name', 'app_id', 'app_title',
            'review_date', 'score', 'review_text', 'reviewer_name',
            'helpful_count', 'review_id'
        ]
        df = df.reindex(columns=column_order)
        
        # Create temporary file
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', encoding='utf-8') as tmp_file:
            df.to_csv(tmp_file.name, index=False, encoding='utf-8')
            tmp_filename = tmp_file.name
        
        # Generate filename
        filename = f"{symbol}_reviews_{fetcher.current_year}.csv"
        
        def remove_file():
            try:
                os.unlink(tmp_filename)
            except:
                pass
        
        # Send file and schedule cleanup
        return send_file(
            tmp_filename,
            as_attachment=True,
            download_name=filename,
            mimetype='text/csv'
        )
        
    except Exception as e:
        logger.error(f"Error generating CSV for {symbol}: {str(e)}")
        return jsonify({
            "error": f"Failed to generate CSV for {symbol}",
            "details": str(e)
        }), 500

@app.route('/all-companies/reviews/csv')
def download_all_companies_reviews_csv():
    """Download reviews for ALL NASDAQ companies as one massive CSV"""
    try:
        all_reviews = []
        successful_companies = []
        failed_companies = []
        
        logger.info(f"Starting bulk fetch for all {len(NASDAQ_100_COMPANIES)} companies...")
        
        for i, (symbol, company_data) in enumerate(NASDAQ_100_COMPANIES.items(), 1):
            try:
                company_info = get_company_info(symbol)
                logger.info(f"[{i}/{len(NASDAQ_100_COMPANIES)}] Fetching reviews for {symbol} - {company_info['company_name']}")
                
                # Fetch reviews for this company
                company_reviews = fetcher.fetch_developer_reviews(
                    company_info['play_store_developer'],
                    symbol
                )
                
                if company_reviews:
                    all_reviews.extend(company_reviews)
                    successful_companies.append(symbol)
                    logger.info(f"✓ {symbol}: Found {len(company_reviews)} reviews")
                else:
                    logger.warning(f"✗ {symbol}: No reviews found")
                    failed_companies.append(symbol)
                
                # Extra delay between companies to be respectful
                time.sleep(2.0)
                
            except Exception as e:
                logger.error(f"✗ {symbol}: Error - {str(e)}")
                failed_companies.append(symbol)
                continue
        
        if not all_reviews:
            return jsonify({
                "message": f"No reviews found for any company in {fetcher.current_year}",
                "attempted_companies": len(NASDAQ_100_COMPANIES),
                "successful": len(successful_companies),
                "failed": len(failed_companies)
            }), 404
        
        # Create DataFrame
        df = pd.DataFrame(all_reviews)
        
        # Reorder columns
        column_order = [
            'company_symbol', 'developer_name', 'app_id', 'app_title',
            'review_date', 'score', 'review_text', 'reviewer_name',
            'helpful_count', 'review_id'
        ]
        df = df.reindex(columns=column_order)
        
        # Sort by company symbol then date for better organization
        df = df.sort_values(['company_symbol', 'review_date'])
        
        # Create temporary file
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', encoding='utf-8') as tmp_file:
            df.to_csv(tmp_file.name, index=False, encoding='utf-8')
            tmp_filename = tmp_file.name
        
        # Generate filename
        filename = f"nasdaq_100_all_reviews_{fetcher.current_year}.csv"
        
        logger.info(f"Successfully generated CSV with {len(all_reviews)} total reviews from {len(successful_companies)} companies")
        
        return send_file(
            tmp_filename,
            as_attachment=True,
            download_name=filename,
            mimetype='text/csv'
        )
        
    except Exception as e:
        logger.error(f"Error generating bulk CSV: {str(e)}")
        return jsonify({
            "error": "Failed to generate bulk CSV for all companies",
            "details": str(e)
        }), 500

@app.route('/batch/reviews/csv', methods=['POST'])
def download_batch_companies_reviews_csv():
    """Download reviews for a specific list of companies as CSV
    
    POST body should be JSON: {"symbols": ["AAPL", "MSFT", "GOOGL"]}
    """
    try:
        data = request.get_json()
        if not data or 'symbols' not in data:
            return jsonify({
                "error": "Request must include 'symbols' array in JSON body",
                "example": {"symbols": ["AAPL", "MSFT", "GOOGL"]}
            }), 400
        
        symbols = data['symbols']
        if not isinstance(symbols, list) or len(symbols) == 0:
            return jsonify({
                "error": "Symbols must be a non-empty array"
            }), 400
        
        all_reviews = []
        successful_companies = []
        failed_companies = []
        
        logger.info(f"Starting batch fetch for {len(symbols)} companies: {symbols}")
        
        for i, symbol in enumerate(symbols, 1):
            try:
                company_info = get_company_info(symbol.upper())
                if not company_info:
                    failed_companies.append(symbol)
                    logger.warning(f"✗ {symbol}: Company not found")
                    continue
                
                logger.info(f"[{i}/{len(symbols)}] Fetching reviews for {symbol} - {company_info['company_name']}")
                
                # Fetch reviews for this company
                company_reviews = fetcher.fetch_developer_reviews(
                    company_info['play_store_developer'],
                    symbol.upper()
                )
                
                if company_reviews:
                    all_reviews.extend(company_reviews)
                    successful_companies.append(symbol.upper())
                    logger.info(f"✓ {symbol}: Found {len(company_reviews)} reviews")
                else:
                    logger.warning(f"✗ {symbol}: No reviews found")
                    failed_companies.append(symbol.upper())
                
                # Delay between companies
                time.sleep(1.5)
                
            except Exception as e:
                logger.error(f"✗ {symbol}: Error - {str(e)}")
                failed_companies.append(symbol)
                continue
        
        if not all_reviews:
            return jsonify({
                "message": f"No reviews found for any of the requested companies in {fetcher.current_year}",
                "requested_symbols": symbols,
                "successful": successful_companies,
                "failed": failed_companies
            }), 404
        
        # Create DataFrame
        df = pd.DataFrame(all_reviews)
        
        # Reorder columns
        column_order = [
            'company_symbol', 'developer_name', 'app_id', 'app_title',
            'review_date', 'score', 'review_text', 'reviewer_name',
            'helpful_count', 'review_id'
        ]
        df = df.reindex(columns=column_order)
        
        # Sort by company symbol then date
        df = df.sort_values(['company_symbol', 'review_date'])
        
        # Create temporary file
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', encoding='utf-8') as tmp_file:
            df.to_csv(tmp_file.name, index=False, encoding='utf-8')
            tmp_filename = tmp_file.name
        
        # Generate filename
        symbols_str = "_".join(successful_companies[:5])  # Limit filename length
        if len(successful_companies) > 5:
            symbols_str += f"_and_{len(successful_companies)-5}_more"
        filename = f"batch_{symbols_str}_reviews_{fetcher.current_year}.csv"
        
        logger.info(f"Successfully generated batch CSV with {len(all_reviews)} total reviews from {len(successful_companies)} companies")
        
        return send_file(
            tmp_filename,
            as_attachment=True,
            download_name=filename,
            mimetype='text/csv'
        )
        
    except Exception as e:
        logger.error(f"Error generating batch CSV: {str(e)}")
        return jsonify({
            "error": "Failed to generate batch CSV",
            "details": str(e)
        }), 500

@app.route('/health')
def health_check():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc).isoformat()
    })

if __name__ == '__main__':
    # Make sure required packages are available
    try:
        import google_play_scraper
        import pandas
    except ImportError as e:
        print(f"Missing required package: {e}")
        print("Please install with: pip install google-play-scraper pandas flask")
        exit(1)
    
    print("Starting Google Play Store Reviews API...")
    print("Available endpoints:")
    print("- GET / : API information")
    print("- GET /companies : List all NASDAQ companies")
    print("- GET /<SYMBOL>/reviews : Get reviews in JSON")
    print("- GET /<SYMBOL>/reviews/csv : Download CSV file")
    print("\nExample: http://localhost:5000/AAPL/reviews/csv")
    
    app.run(debug=True, host='0.0.0.0', port=5000)