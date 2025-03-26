import requests
from dotenv import load_dotenv

import os
# Load environment variables from .env file
load_dotenv()

# Set default environment variables if not set
default_env = {
    'DB_HOST': '',
    'DB_PORT': '',
    'DB_NAME': '',
    'DB_USER': '',
    'DB_PASSWORD': '',
    'API_KEY': '',
    'SECRET_KEY': ''
}

# Set environment variables if not already set
for key, value in default_env.items():
    if not os.getenv(key):
        os.environ[key] = value

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import GetAssetsRequest

trading_client = TradingClient(os.getenv('API_KEY'), os.getenv('SECRET_KEY'))

# Get our account information.
account = trading_client.get_account()

# Check if our account is restricted from trading.
if account.trading_blocked:
    print('Account is currently restricted from trading.')

# Check how much money we can use to open new positions.
print('${} is available as buying power.'.format(account.buying_power))