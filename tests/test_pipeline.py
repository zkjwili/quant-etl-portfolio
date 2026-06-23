import pytest
import pandas as pd
from click.testing import CliRunner
from unittest.mock import patch
import sys
import os

# Make sure Python can find your main.py file
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import your actual CLI function
from main import run_pipeline

@patch("main.requests.get")  # Intercepts FRED requests
@patch("main.yf.download")   # Intercepts YFinance requests
def test_full_pipeline_with_mocked_data(mock_yf, mock_req):
    # 1. Create Fake YFinance Data
    dates = pd.date_range("2023-10-01", periods=2)
    yf_fake_df = pd.DataFrame({"Close": [1900.0, 1910.0]}, index=dates)
    mock_yf.return_value = yf_fake_df

    # 2. Create Fake FRED Data (exactly as the website sends it)
    fake_csv = "observation_date,VIXCLS,DGS10\n2023-10-01,18.0,4.5\n2023-10-02,19.0,4.6"
    mock_req.return_value.status_code = 200
    mock_req.return_value.text = fake_csv

    # 3. Run your CLI script with a specific date range
    runner = CliRunner()
    with runner.isolated_filesystem():
        # THE SENIOR FIX: Directly inject the fake API key into Click's environment
        result = runner.invoke(
            run_pipeline, 
            ["--start", "2023-10-01", "--end", "2023-10-02"],
            env={"FRED_API_KEY": "fake_test_key_12345"} # <--- Injected here!
        )
        
        # 4. Prove it worked
        assert result.exit_code == 0, f"Pipeline failed with: {result.output}"
        assert os.path.exists("data_lake/"), "Data lake folder was not created!"
        print("\n[TEST SUCCESS] Mock pipeline executed perfectly!")

@patch("main.requests.get")  # Intercepts FRED
@patch("main.yf.download")   # Intercepts YFinance
def test_weekend_circuit_breaker(mock_yf, mock_req):
    """Test that the pipeline exits gracefully when the market is closed (returns empty data)."""
    
    # 1. Create an EMPTY Fake YFinance DataFrame (Simulating a weekend)
    mock_yf.return_value = pd.DataFrame(columns=["Close"])

    # 2. Create Fake FRED Data
    fake_csv = "observation_date,VIXCLS,DGS10\n2023-10-01,18.0,4.5"
    mock_req.return_value.status_code = 200
    mock_req.return_value.text = fake_csv

    # 3. Run the pipeline
    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(
            run_pipeline, 
            ["--start", "2023-10-01", "--end", "2023-10-02"],
            env={"FRED_API_KEY": "fake_test_key_12345"}
        )
        
        # 4. Prove it exited with a 0 (Success) code, NOT a crash
        assert result.exit_code == 0, f"Weekend breaker failed with: {result.output}"
        print("\n[TEST SUCCESS] Weekend circuit breaker handled empty data perfectly!")