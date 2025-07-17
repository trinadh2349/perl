#!/usr/bin/env python3
"""
Test runner script for zoe_converter.py unit tests.

This script sets up the test environment and runs the complete test suite
with coverage reporting.
"""

import sys
import os
import subprocess
import argparse
from pathlib import Path


def setup_environment():
    """Set up the test environment."""
    # Add current directory to Python path
    current_dir = Path(__file__).parent
    sys.path.insert(0, str(current_dir))
    
    # Set environment variables for testing
    os.environ['PYTHONPATH'] = str(current_dir)
    os.environ['TESTING'] = '1'


def run_tests(test_type='all', verbose=False, coverage=True):
    """Run the test suite."""
    cmd = ['python', '-m', 'pytest']
    
    if verbose:
        cmd.append('-v')
    
    if coverage:
        cmd.extend(['--cov=zoe_converter', '--cov-report=html', '--cov-report=term-missing'])
    
    if test_type == 'unit':
        cmd.extend(['-m', 'unit'])
    elif test_type == 'integration':
        cmd.extend(['-m', 'integration'])
    elif test_type == 'fast':
        cmd.extend(['-m', 'not slow'])
    
    # Add test file
    cmd.append('test_zoe_converter.py')
    
    print(f"Running command: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(cmd, check=True, capture_output=False)
        return result.returncode == 0
    except subprocess.CalledProcessError as e:
        print(f"Tests failed with exit code: {e.returncode}")
        return False


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description='Run zoe_converter tests')
    parser.add_argument(
        '--type', 
        choices=['all', 'unit', 'integration', 'fast'],
        default='all',
        help='Type of tests to run'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Verbose output'
    )
    parser.add_argument(
        '--no-coverage',
        action='store_true',
        help='Skip coverage reporting'
    )
    
    args = parser.parse_args()
    
    setup_environment()
    
    success = run_tests(
        test_type=args.type,
        verbose=args.verbose,
        coverage=not args.no_coverage
    )
    
    if success:
        print("\n✅ All tests passed!")
        if not args.no_coverage:
            print("📊 Coverage report generated in htmlcov/index.html")
    else:
        print("\n❌ Some tests failed!")
        sys.exit(1)


if __name__ == '__main__':
    main()