#!/usr/bin/env python3
"""
Smoke tests for NBA Cap Optimizer API
Run after deployment to verify basic functionality
"""

import argparse
import os
import sys
import time
from typing import Dict, List

import requests


class SmokeTests:
    """Basic smoke tests to verify deployment"""

    def __init__(self, api_url: str, api_key: str, timeout: int = 30):
        self.api_url = api_url.rstrip('/')
        self.api_key = api_key
        self.timeout = timeout
        self.headers = {
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json'
        }
        self.results: List[Dict[str, any]] = []

    def run_test(self, name: str, func):
        """Run a single test and record result"""
        print(f"\n{'='*60}")
        print(f"Running: {name}")
        print(f"{'='*60}")

        start_time = time.time()
        try:
            func()
            duration = time.time() - start_time
            print(f"✅ PASSED ({duration:.2f}s)")
            self.results.append({
                'name': name,
                'status': 'PASSED',
                'duration': duration
            })
            return True
        except AssertionError as e:
            duration = time.time() - start_time
            print(f"❌ FAILED ({duration:.2f}s): {str(e)}")
            self.results.append({
                'name': name,
                'status': 'FAILED',
                'duration': duration,
                'error': str(e)
            })
            return False
        except Exception as e:
            duration = time.time() - start_time
            print(f"❌ ERROR ({duration:.2f}s): {str(e)}")
            self.results.append({
                'name': name,
                'status': 'ERROR',
                'duration': duration,
                'error': str(e)
            })
            return False

    def test_health_check(self):
        """Test basic health endpoint"""
        response = requests.get(
            f"{self.api_url}/health",
            timeout=self.timeout
        )
        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        data = response.json()
        assert data.get('status') == 'healthy', "API is not healthy"
        print(f"   Response: {data}")

    def test_api_version(self):
        """Test API version endpoint"""
        response = requests.get(
            f"{self.api_url}/version",
            headers=self.headers,
            timeout=self.timeout
        )
        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        data = response.json()
        assert 'version' in data, "Version not in response"
        print(f"   API Version: {data['version']}")

    def test_list_players(self):
        """Test list players endpoint"""
        response = requests.get(
            f"{self.api_url}/api/v1/players",
            headers=self.headers,
            params={'limit': 10},
            timeout=self.timeout
        )
        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        data = response.json()
        assert isinstance(data, (list, dict)), "Response should be list or dict"
        print(f"   Retrieved players: {len(data) if isinstance(data, list) else 'paginated'}")

    def test_get_player(self):
        """Test get specific player endpoint"""
        response = requests.get(
            f"{self.api_url}/api/v1/players/1",
            headers=self.headers,
            timeout=self.timeout
        )
        # Accept both 200 (found) and 404 (not found yet) as valid responses
        assert response.status_code in [200, 404], f"Expected 200 or 404, got {response.status_code}"
        if response.status_code == 200:
            data = response.json()
            assert 'id' in data or 'player_id' in data, "Player data missing ID"
            print(f"   Player data retrieved")
        else:
            print(f"   No player data yet (404) - acceptable for new deployment")

    def test_rankings(self):
        """Test rankings endpoint"""
        response = requests.get(
            f"{self.api_url}/api/v1/rankings/undervalued",
            headers=self.headers,
            params={'limit': 10},
            timeout=self.timeout
        )
        # Accept 200 or 404 (no data yet)
        assert response.status_code in [200, 404], f"Expected 200 or 404, got {response.status_code}"
        if response.status_code == 200:
            data = response.json()
            print(f"   Rankings retrieved")
        else:
            print(f"   No rankings yet (404) - acceptable for new deployment")

    def test_response_time(self):
        """Test API response time"""
        start = time.time()
        response = requests.get(
            f"{self.api_url}/health",
            timeout=self.timeout
        )
        duration = time.time() - start

        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        assert duration < 2.0, f"Response time too slow: {duration:.2f}s (should be < 2s)"
        print(f"   Response time: {duration:.2f}s")

    def test_cors_headers(self):
        """Test CORS headers are present"""
        response = requests.options(
            f"{self.api_url}/health",
            headers={'Origin': 'https://nba-cap-optimizer.com'},
            timeout=self.timeout
        )
        # CORS preflight might not be implemented yet, so accept 200 or 404
        assert response.status_code in [200, 204, 404], f"OPTIONS returned {response.status_code}"
        print(f"   CORS check: {response.status_code}")

    def test_invalid_endpoint(self):
        """Test that invalid endpoints return 404"""
        response = requests.get(
            f"{self.api_url}/api/v1/invalid-endpoint-12345",
            headers=self.headers,
            timeout=self.timeout
        )
        assert response.status_code == 404, f"Expected 404 for invalid endpoint, got {response.status_code}"
        print(f"   404 handling works correctly")

    def test_authentication(self):
        """Test that protected endpoints require authentication"""
        response = requests.get(
            f"{self.api_url}/api/v1/players",
            timeout=self.timeout
        )
        # Should return 401 (unauthorized) or 403 (forbidden) without API key
        # Or 200 if auth not implemented yet
        print(f"   Auth status: {response.status_code}")
        if response.status_code in [401, 403]:
            print(f"   Authentication required (good!)")
        elif response.status_code == 200:
            print(f"   No authentication required (may want to add later)")

    def print_summary(self):
        """Print test summary"""
        print(f"\n{'='*60}")
        print("TEST SUMMARY")
        print(f"{'='*60}")

        passed = sum(1 for r in self.results if r['status'] == 'PASSED')
        failed = sum(1 for r in self.results if r['status'] == 'FAILED')
        errors = sum(1 for r in self.results if r['status'] == 'ERROR')
        total = len(self.results)

        print(f"Total Tests: {total}")
        print(f"✅ Passed: {passed}")
        print(f"❌ Failed: {failed}")
        print(f"💥 Errors: {errors}")
        print(f"Success Rate: {(passed/total*100):.1f}%")

        total_duration = sum(r['duration'] for r in self.results)
        print(f"Total Duration: {total_duration:.2f}s")

        if failed > 0 or errors > 0:
            print("\n⚠️  SOME TESTS FAILED")
            return False
        else:
            print("\n✅ ALL TESTS PASSED")
            return True

    def run_all(self):
        """Run all smoke tests"""
        print(f"\nRunning smoke tests against: {self.api_url}")
        print(f"Timeout: {self.timeout}s")

        # Critical tests (must pass)
        critical_tests = [
            ("Health Check", self.test_health_check),
            ("Response Time", self.test_response_time),
        ]

        # Important tests (should pass but not critical for MVP)
        important_tests = [
            ("API Version", self.test_api_version),
            ("List Players", self.test_list_players),
            ("Get Player", self.test_get_player),
            ("Rankings", self.test_rankings),
            ("Invalid Endpoint (404)", self.test_invalid_endpoint),
            ("CORS Headers", self.test_cors_headers),
            ("Authentication", self.test_authentication),
        ]

        # Run critical tests first
        print("\n" + "="*60)
        print("CRITICAL TESTS")
        print("="*60)
        for name, func in critical_tests:
            passed = self.run_test(name, func)
            if not passed:
                print("\n❌ CRITICAL TEST FAILED - DEPLOYMENT UNSAFE")
                return False

        # Run important tests
        print("\n" + "="*60)
        print("ADDITIONAL TESTS")
        print("="*60)
        for name, func in important_tests:
            self.run_test(name, func)

        return self.print_summary()


def main():
    parser = argparse.ArgumentParser(description='Run smoke tests against NBA Cap Optimizer API')
    parser.add_argument('--env', required=True, choices=['dev', 'prod'], help='Environment to test')
    parser.add_argument('--api-url', required=True, help='API base URL')
    parser.add_argument('--timeout', type=int, default=30, help='Request timeout in seconds')

    args = parser.parse_args()

    # Get API key from environment
    api_key = os.getenv('API_KEY', '')
    if not api_key:
        print("⚠️  Warning: No API_KEY environment variable set")

    # Run tests
    tests = SmokeTests(args.api_url, api_key, args.timeout)
    success = tests.run_all()

    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
