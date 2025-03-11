import requests
import json
import os

def test_arxiv_api():
    """Test ArXiv API connection"""
    print("Testing ArXiv API...")
    url = "http://export.arxiv.org/api/query"
    params = {
        "search_query": "all:artificial intelligence AND (labor market OR employment OR jobs)",
        "start": 0,
        "max_results": 5,
        "sortBy": "submittedDate",
        "sortOrder": "descending"
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        print(f"Status code: {response.status_code}")
        if response.status_code == 200:
            print("✅ ArXiv API connection successful")
            print(f"Response length: {len(response.text)} characters")
            return True
        else:
            print("❌ ArXiv API connection failed")
            print(f"Response: {response.text[:200]}...")
            return False
    except Exception as e:
        print(f"❌ ArXiv API connection error: {str(e)}")
        return False

def test_bls_api(api_key=None):
    """Test BLS API connection"""
    print("\nTesting BLS API...")
    url = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
    headers = {"Content-Type": "application/json"}
    
    # Request data for Information sector employment
    payload = {
        "seriesid": ["CEU6054130001"],  # Information sector employment
        "startyear": "2022",
        "endyear": "2023",
    }
    
    # Add API key if provided
    if api_key:
        payload["registrationKey"] = api_key
        print("Using provided BLS API key")
    else:
        print("No BLS API key provided (limited to 50 requests/day)")
    
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=10)
        print(f"Status code: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            if data["status"] == "REQUEST_SUCCEEDED":
                print("✅ BLS API connection successful")
                print(f"Response status: {data['status']}")
                return True
            else:
                print(f"❌ BLS API request failed: {data['status']}")
                print(f"Message: {data.get('message', 'No message')}")
                return False
        else:
            print("❌ BLS API connection failed")
            print(f"Response: {response.text[:200]}...")
            return False
    except Exception as e:
        print(f"❌ BLS API connection error: {str(e)}")
        return False

def test_remote_jobs_api():
    """Test Remote Jobs API connection"""
    print("\nTesting Remote Jobs API...")
    url = "https://remotive.com/api/remote-jobs"
    params = {"category": "software-dev"}
    
    try:
        response = requests.get(url, params=params, timeout=10)
        print(f"Status code: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            jobs_count = len(data.get("jobs", []))
            print("✅ Remote Jobs API connection successful")
            print(f"Found {jobs_count} jobs")
            
            # Check for AI-related jobs
            ai_jobs = [
                job for job in data.get("jobs", [])
                if any(kw in job.get("title", "").lower() or kw in job.get("description", "").lower() 
                       for kw in ["ai", "artificial intelligence", "machine learning", "ml", "deep learning"])
            ]
            print(f"Found {len(ai_jobs)} AI-related jobs")
            return True
        else:
            print("❌ Remote Jobs API connection failed")
            print(f"Response: {response.text[:200]}...")
            return False
    except Exception as e:
        print(f"❌ Remote Jobs API connection error: {str(e)}")
        return False

def test_news_api(api_key):
    """Test News API connection"""
    if not api_key:
        print("\nSkipping News API test - API key required")
        return False
        
    print("\nTesting News API...")
    url = "https://newsapi.org/v2/everything"
    params = {
        "q": "AI layoffs hiring",
        "apiKey": api_key,
        "pageSize": 5,
        "language": "en",
        "sortBy": "publishedAt"
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        print(f"Status code: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            articles_count = len(data.get("articles", []))
            print("✅ News API connection successful")
            print(f"Found {articles_count} articles")
            return True
        else:
            print("❌ News API connection failed")
            print(f"Response: {response.text[:200]}...")
            return False
    except Exception as e:
        print(f"❌ News API connection error: {str(e)}")
        return False

if __name__ == "__main__":
    print("===== API CONNECTION TESTER =====")
    print("Testing connections to data sources for AI Labor Market Index\n")
    
    # Get API keys from environment variables or input
    bls_api_key = os.environ.get("BLS_API_KEY")
    if not bls_api_key:
        bls_api_key = input("Enter BLS API key (optional, press Enter to skip): ").strip()
        if not bls_api_key:
            bls_api_key = None
    
    news_api_key = os.environ.get("NEWS_API_KEY")
    if not news_api_key:
        news_api_key = input("Enter News API key (optional, press Enter to skip): ").strip()
        if not news_api_key:
            news_api_key = None
    
    # Run tests
    arxiv_success = test_arxiv_api()
    bls_success = test_bls_api(bls_api_key)
    remote_jobs_success = test_remote_jobs_api()
    news_success = test_news_api(news_api_key)
    
    # Summary
    print("\n===== TEST SUMMARY =====")
    print(f"ArXiv API: {'✅ SUCCESS' if arxiv_success else '❌ FAILED'}")
    print(f"BLS API: {'✅ SUCCESS' if bls_success else '❌ FAILED'}")
    print(f"Remote Jobs API: {'✅ SUCCESS' if remote_jobs_success else '❌ FAILED'}")
    print(f"News API: {'✅ SUCCESS' if news_success else '❌ FAILED or SKIPPED'}")
    
    # Overall assessment
    working_apis = sum([arxiv_success, bls_success, remote_jobs_success, news_success])
    total_apis = 4
    
    print(f"\n{working_apis}/{total_apis} APIs are working correctly")
    
    if working_apis == 0:
        print("\n❌ CRITICAL: All APIs are failing. Check your internet connection.")
    elif working_apis < total_apis:
        print("\n⚠️ WARNING: Some APIs are failing. Review the output above for details.")
    else:
        print("\n✅ SUCCESS: All APIs are working correctly.")