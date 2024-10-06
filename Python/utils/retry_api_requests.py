import requests
from tenacity import (
    retry,
    stop_after_attempt,
    wait_fixed,
    retry_if_exception_type,
    wait_exponential,
    wait_random_exponential,
)

# Define retryable status codes
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

# Define unretryable status codes
UNRETRYABLE_STATUS_CODES = {400, 401, 403, 404, 405}


# Function to check if the status code is retryable
def is_retryable(status_code):
    return status_code in RETRYABLE_STATUS_CODES


# Function to check if the status code is unretryable
def is_unretryable(status_code):
    return status_code in UNRETRYABLE_STATUS_CODES


# Function to make a RESTful request with retry logic
@retry(
    stop=stop_after_attempt(3),
    wait=wait_random_exponential(multiplier=1, max=10),
    retry=retry_if_exception_type(requests.exceptions.RequestException),
)
def make_request(url):
    response = requests.get(url, timeout=1)
    if is_unretryable(response.status_code):
        response.raise_for_status()  # Raise an exception for unretryable status codes
    return response.json()


url = "https://openlibrary.org/search.json?q=the+lord+of+the+rings"
try:
    data = make_request(url)
    print(data)
except requests.exceptions.RequestException as e:
    print(f"Request failed: {e}")
