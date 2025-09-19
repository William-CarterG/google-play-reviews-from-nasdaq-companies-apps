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
import threading
import queue
from concurrent.futures import ThreadPoolExecutor
import csv

# Import NASDAQ companies data
from nasdaq_companies import NASDAQ_100_COMPANIES, get_company_info

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PlayStoreReviewsFetcher:
    def __init__(self, rate_limit_delay: float = 0.0):
        """
        Initialize the reviews fetcher
        
        Args:
            rate_limit_delay: Delay between requests in seconds (0 = no delay)
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
        Fetch up to 2000 most recent reviews from 2025 for a specific app
        
        Args:
            app_id: Google Play Store app ID
            app_title: App title for logging purposes
            
        Returns:
            List of review dictionaries (max 2000)
        """
        try:
            logger.info(f"Fetching reviews for app: {app_title or app_id}")
            
            # Fetch all reviews with no rate limiting
            app_reviews = reviews_all(
                app_id,
                sleep_milliseconds=0,  # No rate limiting
                lang='en',
                country='us'
            )
            
            # Limit to first 2000 reviews (most recent) since count parameter doesn't work
            limited_reviews = app_reviews[:2000]
            
            # Filter reviews from current year (2025)
            current_year_reviews = []
            for review in limited_reviews:
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
            
            # Remove rate limiting between apps
            # time.sleep(self.rate_limit_delay)  # Eliminated!
        
    def fetch_company_reviews_worker(self, symbol: str, reviews_queue: queue.Queue, progress_queue: queue.Queue):
        """
        Worker function to fetch reviews for a single company (thread-safe)
        
        Args:
            symbol: NASDAQ symbol
            reviews_queue: Queue to put review data
            progress_queue: Queue to put progress updates
        """
        try:
            company_info = get_company_info(symbol)
            if not company_info:
                progress_queue.put(('error', symbol, f"Company {symbol} not found"))
                return
            
            progress_queue.put(('start', symbol, f"Starting {company_info['company_name']}"))
            
            # Fetch reviews for this company
            company_reviews = self.fetch_developer_reviews(
                company_info['play_store_developer'],
                symbol
            )
            
            if company_reviews:
                # Put reviews into queue for writing
                for review in company_reviews:
                    reviews_queue.put(review)
                
                progress_queue.put(('success', symbol, f"Completed {symbol}: {len(company_reviews)} reviews"))
            else:
                progress_queue.put(('warning', symbol, f"No reviews found for {symbol}"))
                
        except Exception as e:
            progress_queue.put(('error', symbol, f"Error processing {symbol}: {str(e)}"))

    def csv_writer_worker(self, reviews_queue: queue.Queue, progress_queue: queue.Queue, csv_file_path: str, stop_event: threading.Event):
        """
        Dedicated thread to write reviews to CSV as they come in
        
        Args:
            reviews_queue: Queue containing review dictionaries
            progress_queue: Queue for progress updates  
            csv_file_path: Path to CSV file
            stop_event: Event to signal when to stop
        """
        try:
            with open(csv_file_path, 'w', newline='', encoding='utf-8') as csvfile:
                column_order = [
                    'company_symbol', 'developer_name', 'app_id', 'app_title',
                    'review_date', 'score', 'review_text', 'reviewer_name',
                    'helpful_count', 'review_id'
                ]
                
                writer = csv.DictWriter(csvfile, fieldnames=column_order)
                writer.writeheader()
                
                total_written = 0
                
                while not stop_event.is_set() or not reviews_queue.empty():
                    try:
                        # Get review with timeout to check stop_event periodically
                        review = reviews_queue.get(timeout=1.0)
                        writer.writerow(review)
                        csvfile.flush()  # Ensure data is written immediately
                        total_written += 1
                        
                        # Progress update every 100 reviews
                        if total_written % 100 == 0:
                            progress_queue.put(('csv_progress', 'writer', f"Written {total_written} reviews to CSV"))
                        
                        reviews_queue.task_done()
                        
                    except queue.Empty:
                        continue  # Check stop_event and try again
                        
                progress_queue.put(('csv_complete', 'writer', f"CSV writing complete: {total_written} total reviews"))
                
        except Exception as e:
            progress_queue.put(('error', 'csv_writer', f"CSV writer error: {str(e)}"))

    def fetch_all_companies_parallel(self, max_workers: int = 4) -> tuple:
        """
        Fetch reviews for all NASDAQ companies using parallel processing with streaming CSV
        
        Args:
            max_workers: Number of parallel threads
            
        Returns:
            tuple: (csv_file_path, successful_companies, failed_companies, total_reviews)
        """
        # Create queues for thread communication
        reviews_queue = queue.Queue()
        progress_queue = queue.Queue()
        
        # Create temporary CSV file
        tmp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', encoding='utf-8')
        csv_file_path = tmp_file.name
        tmp_file.close()  # Close so CSV writer can open it
        
        # Event to signal CSV writer to stop
        stop_event = threading.Event()
        
        successful_companies = []
        failed_companies = []
        
        # Start CSV writer thread
        csv_writer_thread = threading.Thread(
            target=self.csv_writer_worker,
            args=(reviews_queue, progress_queue, csv_file_path, stop_event)
        )
        csv_writer_thread.start()
        
        try:
            # Start parallel company processing
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all companies to thread pool
                future_to_symbol = {
                    executor.submit(self.fetch_company_reviews_worker, symbol, reviews_queue, progress_queue): symbol
                    for symbol in NASDAQ_100_COMPANIES.keys()
                }
                
                completed_companies = 0
                total_companies = len(NASDAQ_100_COMPANIES)
                
                # Process results as they complete
                for future in future_to_symbol:
                    symbol = future_to_symbol[future]
                    try:
                        future.result()  # This will raise any exception that occurred
                        completed_companies += 1
                        
                        # Process any progress messages
                        while not progress_queue.empty():
                            try:
                                msg_type, msg_symbol, message = progress_queue.get_nowait()
                                if msg_type == 'success':
                                    successful_companies.append(msg_symbol)
                                elif msg_type in ['error', 'warning']:
                                    failed_companies.append(msg_symbol)
                                
                                logger.info(f"[{completed_companies}/{total_companies}] {message}")
                                
                            except queue.Empty:
                                break
                        
                    except Exception as e:
                        logger.error(f"Thread for {symbol} failed: {str(e)}")
                        failed_companies.append(symbol)
        
        finally:
            # Signal CSV writer to stop and wait for completion
            stop_event.set()
            csv_writer_thread.join(timeout=30)  # Wait up to 30 seconds
            
            # Process any remaining progress messages
            while not progress_queue.empty():
                try:
                    msg_type, msg_symbol, message = progress_queue.get_nowait()
                    logger.info(f"Final: {message}")
                except queue.Empty:
                    break
        
        # Count total reviews by reading the CSV (quick way to get accurate count)
        total_reviews = 0
        try:
            with open(csv_file_path, 'r', encoding='utf-8') as f:
                total_reviews = sum(1 for line in f) - 1  # Subtract header
        except:
            pass
        
        return csv_file_path, successful_companies, failed_companies, total_reviews

# Initialize the fetcher with no rate limiting
fetcher = PlayStoreReviewsFetcher(rate_limit_delay=0.0)

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
    """Download reviews for ALL NASDAQ companies using parallel processing with streaming CSV"""
    try:
        logger.info(f"Starting parallel bulk fetch for all {len(NASDAQ_100_COMPANIES)} companies...")
        
        # Use parallel processing with streaming CSV
        csv_file_path, successful_companies, failed_companies, total_reviews = fetcher.fetch_all_companies_parallel(max_workers=4)
        
        if total_reviews == 0:
            os.unlink(csv_file_path)  # Clean up empty file
            return jsonify({
                "message": f"No reviews found for any company in {fetcher.current_year}",
                "attempted_companies": len(NASDAQ_100_COMPANIES),
                "successful": len(successful_companies),
                "failed": len(failed_companies),
                "failed_companies": failed_companies
            }), 404
        
        # Generate filename
        filename = f"nasdaq_100_all_reviews_{fetcher.current_year}_parallel.csv"
        
        logger.info(f"Parallel processing complete: {total_reviews} total reviews from {len(successful_companies)} companies")
        logger.info(f"Successful: {successful_companies}")
        logger.info(f"Failed: {failed_companies}")
        
        return send_file(
            csv_file_path,
            as_attachment=True,
            download_name=filename,
            mimetype='text/csv'
        )
        
    except Exception as e:
        logger.error(f"Error in parallel bulk CSV generation: {str(e)}")
        return jsonify({
            "error": "Failed to generate parallel bulk CSV for all companies",
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
                
                # Removed delay between companies for speed
                
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