# XRP Trading Bot

An automated trading bot for XRP on Alpaca Markets.

## Setup Instructions

1. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

2. Configure environment variables:
   - Copy `.env.example` to `.env`
   - Fill in your database and Alpaca API credentials

3. Setup PostgreSQL database:
   - Create a database matching your DB_NAME
   - The bot will create necessary tables automatically

4. Run the bot:
   ```
   python xrp_streamer.py
   ```

## Trading Strategy

This bot implements a simple mean-reversion strategy:
- Buys when price drops 2.5% from reference price
- Sets take profit at +3.5% from entry
- Sets stop loss at -1.5% from entry
- Accounts for trading fees (0.4%) and network fees

## Configuration

Key parameters can be adjusted in the `XRPStreamer` class:
- `price_threshold`: % drop required to enter (default 2.5%)
- `take_profit_threshold`: % gain target (default 3.5%)
- `stop_loss_threshold`: % loss limit (default 1.5%)
- `min_position_value`: Minimum position size in USD (default $4000)
- `position_value`: Target position size in USD (default $20000)

## Data Sources

The bot leverages multiple data sources from Alpaca Markets:
- **Quotes**: Bid/ask data used for real-time price updates
- **Trades**: Actual executed trades on the market
- **Bars**: OHLC (Open, High, Low, Close) minute bars for trend analysis

## Error Handling

The bot implements:
- WebSocket reconnection logic
- Database retry mechanisms with exponential backoff
- Order verification safeguards
- Position tracking with fallback methods
- Duplicate order prevention

## Logging

- Trading activities, orders, and position changes are logged to the console
- Detailed position status is logged every minute
- Bar data is logged at the start of each new minute
- Price updates are selectively logged to reduce noise
- All errors are captured with stack traces for debugging

## Known Limitations

- Buy orders only trigger when market price hits the stop price
- Position detection may be delayed by a few seconds after order fills
- Paper trading mode might have slight differences from live trading
