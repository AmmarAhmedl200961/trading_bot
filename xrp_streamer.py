import logging
import asyncio
import json
import websockets
from datetime import datetime, timedelta
import pytz
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import (
    MarketOrderRequest,
    LimitOrderRequest,
    StopLimitOrderRequest,
    GetOrdersRequest,
)
from alpaca.trading.enums import OrderSide, TimeInForce, OrderStatus, QueryOrderStatus
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Float,
    DateTime,
    MetaData,
    Table,
    text,
    inspect,
)
import sqlalchemy
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from dotenv import load_dotenv
import os
import time

# Load environment variables from .env file
load_dotenv()

# Set default environment variables if not set
default_env = {
    "DB_HOST": "",
    "DB_PORT": "",
    "DB_NAME": "",
    "DB_USER": "",
    "DB_PASSWORD": "",
    "API_KEY": "",
    "SECRET_KEY": "",
}

# Set environment variables if not already set
for key, value in default_env.items():
    if not os.getenv(key):
        os.environ[key] = value

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s",
)

logger = logging.getLogger(__name__)

# API credentials
API_KEY = os.getenv("API_KEY")
SECRET_KEY = os.getenv("SECRET_KEY")


class XRPStreamer:
    def __init__(self):
        self.symbol = "XRP/USD"
        self.ws = None
        self.connected = False
        self.ws_url = "wss://stream.data.alpaca.markets/v1beta3/crypto/us"

        # Initialize database connection
        self.init_database()

        # Trading settings
        self.trading_client = TradingClient(API_KEY, SECRET_KEY, paper=True)
        self.reference_price = None
        self.last_check_time = datetime.now()
        self.check_interval = timedelta(
            seconds=5
        )  # Reduced from 15 minutes to 5 seconds

        # Fee structure - adjusted to realistic Alpaca fees
        self.trading_fee_pct = 0.004  # 0.4% trading fee (more realistic than 1.6%)
        self.network_fee_xrp = 0.56  # XRP network fee

        # Adjusted thresholds to account for fees
        self.price_threshold = 0.025  # Wait for 2.5% price movement to enter
        self.take_profit_threshold = 0.035  # Take profit at 3.5% (covers fees + profit)
        self.stop_loss_threshold = 0.015  # Stop loss at 1.5% to minimize losses

        # Position sizing
        self.min_position_value = 4000  # Further reduced minimum trade size
        self.max_position_size = (
            25000  # Maximum position size based on available balance
        )
        self.position_value = 20000  # Standard position size

        # Trading intervals
        self.last_order_time = None
        self.min_order_interval = timedelta(minutes=2)  # Minimum time between trades
        self.last_logged_price = None

        # Tracking
        self.total_fees_paid = 0
        self.total_profit_loss = 0
        self.orders = []  # Initialize orders list

        # Get initial account balance
        self.cash_balance = None
        self.update_account_balance()
        logging.info(
            f"Initial account status: {self.trading_client.get_account().status}"
        )
        logging.info(f"Cash balance: ${self.cash_balance}")
        self._last_logged_price = None

        # Add order tracking
        self.active_orders = {"buy": None, "take_profit": None, "stop_loss": None}

        # Add safety checks
        self.max_retries = 3
        self.retry_delay = 5

        # Add tracking for last logged trading plan to avoid redundant logs
        self.last_trading_plan_log = datetime.now() - timedelta(minutes=5)
        self.last_order_status = {}  # To track order status changes

        # Add configuration for logging intervals
        self.position_log_interval = 60  # seconds between position status logs
        self.price_log_interval = 30  # seconds between price update logs
        self.bar_log_interval = 60  # seconds between bar data logs

    def init_database(self):
        """Initialize PostgreSQL database connection and tables"""
        try:
            db_host = os.getenv("DB_HOST")
            db_port = os.getenv("DB_PORT", "5432")
            db_name = os.getenv("DB_NAME")
            db_user = os.getenv("DB_USER")
            db_password = os.getenv("DB_PASSWORD")

            if not all([db_host, db_name, db_user, db_password]):
                raise ValueError("Database environment variables not properly set")

            self.db_url = (
                f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
            )
            self.engine = create_engine(
                self.db_url,
                poolclass=QueuePool,
                pool_size=5,
                max_overflow=10,
                pool_timeout=30,
            )

            # Create MetaData instance
            metadata = MetaData()

            # Define only price_history table for now
            self.price_history = Table(
                "price_history",
                metadata,
                Column("id", Integer, primary_key=True),
                Column("symbol", String, nullable=False),
                Column("price", Float),
                Column("timestamp", DateTime),
            )

            # Define orders table with unique constraint on order_id
            self.orders = Table(
                "orders",
                metadata,
                Column("id", Integer, primary_key=True),
                Column(
                    "order_id", String, nullable=False, unique=True
                ),  # Added unique constraint
                Column("timestamp", DateTime),
                Column("side", String, nullable=False),
                Column("quantity", Float),
                Column("price", Float),
                Column("status", String),
                Column("filled_qty", Float),
                Column("filled_avg_price", Float),
                Column("fees", Float),
            )

            # Create tables
            self.price_history.create(self.engine, checkfirst=True)
            self.orders.create(self.engine, checkfirst=True)
            logger.info("Successfully initialized database connection and tables")

            # Add a check to log whether price history table has data
            with self.engine.connect() as conn:
                result = conn.execute(
                    sqlalchemy.select(sqlalchemy.func.count()).select_from(
                        self.price_history
                    )
                ).scalar()

                if result == 0:
                    logging.warning(
                        "Price history database is empty. Waiting for initial quotes..."
                    )
                else:
                    logging.info(f"Found {result} price records in database")

        except Exception as e:
            logger.error(f"Error initializing database: {e}")
            raise

    def update_account_balance(self):
        """Update the current cash balance"""
        account = self.trading_client.get_account()
        self.cash_balance = min(float(account.cash), float(account.buying_power))
        return self.cash_balance

    def store_price(self, price, timestamp):
        """Store price data in the database with improved error handling and retries"""
        retries = 0
        max_retries = self.max_retries

        while retries < max_retries:
            try:
                with self.engine.connect() as conn:
                    conn.execute(
                        self.price_history.insert().values(
                            symbol=self.symbol, price=price, timestamp=timestamp
                        )
                    )
                    conn.commit()
                    return True
            except Exception as e:
                retries += 1
                logging.error(
                    f"Error storing price (attempt {retries}/{max_retries}): {e}",
                    exc_info=True,
                )
                if retries >= max_retries:
                    return False
                time.sleep(self.retry_delay)
        return False

    def get_last_price(self):
        """Get the most recent price from database with improved error handling"""
        retries = 0
        max_retries = self.max_retries

        while retries < max_retries:
            try:
                with self.engine.connect() as conn:
                    result = conn.execute(
                        self.price_history.select()
                        .order_by(self.price_history.c.timestamp.desc())
                        .limit(1)
                    ).fetchone()

                    if not result:
                        logging.warning("No price data available in database")
                        return None

                    return result.price
            except Exception as e:
                retries += 1
                logger.error(
                    f"Error getting last price (attempt {retries}/{max_retries}): {e}"
                )
                if retries >= max_retries:
                    return None
                time.sleep(self.retry_delay)
        return None

    async def update_position(self, position_info=None):
        """Update current position information with improved null handling"""
        try:
            # Get current positions
            positions = self.trading_client.get_all_positions()
            position = None
            for pos in positions:
                if pos.symbol == "XRP/USD":
                    position = pos
                    break

            # Get all orders
            orders = self.trading_client.get_orders()
            has_tp_order = False
            has_sl_order = False

            # Check existing orders
            for order in orders:
                if order.symbol == "XRP/USD" and order.side == "sell":
                    if order.type == "limit":
                        has_tp_order = True
                    elif order.type == "stop_limit":
                        has_sl_order = True

            if position:
                # We have a position - ensure sell orders exist
                position_info = {
                    "qty": float(position.qty),
                    "avg_entry_price": float(position.avg_entry_price),
                    "side": position.side,
                    "market_value": float(position.market_value),
                    "cost_basis": float(position.cost_basis),
                    "unrealized_pl": float(position.unrealized_pl),
                    "unrealized_plpc": float(position.unrealized_plpc),
                    "current_price": float(position.current_price),
                    "change_today": float(position.change_today),
                }

                # Calculate take profit and stop loss levels
                take_profit_price = round(
                    position_info["avg_entry_price"] * (1 + self.take_profit_threshold),
                    4,
                )
                stop_loss_price = round(
                    position_info["avg_entry_price"] * (1 - self.stop_loss_threshold), 4
                )

                # Place missing sell orders if needed
                if not has_tp_order or not has_sl_order:
                    await self.ensure_exit_orders(
                        position, position_info["current_price"]
                    )

                # Log position status
                logging.info(
                    f"\n=== Current Trading Status ===\n"
                    f"Position: {position_info['qty']:.2f} XRP @ ${position_info['avg_entry_price']:.4f}\n"
                    f"Current Price: ${position_info['current_price']:.4f}\n"
                    f"Market Value: ${position_info['market_value']:.2f}\n"
                    f"Cost Basis: ${position_info['cost_basis']:.2f}\n"
                    f"P/L: ${position_info['unrealized_pl']:.2f} ({position_info['unrealized_plpc']:.2%})\n"
                    f"24h Change: {position_info['change_today']:.2%}\n"
                    f"Total Fees Paid: ${self.total_fees_paid:.2f}\n"
                    f"\nSell Orders Status:\n"
                    f"Take Profit Order: {'YES' if has_tp_order else 'NO'} (Target: ${take_profit_price:.4f})\n"
                    f"Stop Loss Order: {'YES' if has_sl_order else 'NO'} (Target: ${stop_loss_price:.4f})\n"
                    f"\nTrading Mode: SELLING - Waiting for exit at +3.5% profit or -1.5% stop loss"
                )

            else:
                # No position - check if we should place a buy order
                current_price = self.get_last_price()
                if current_price is not None:  # Explicit None check
                    has_buy_order = any(
                        o.symbol == "XRP/USD" and o.side == "buy" for o in orders
                    )

                    if (
                        not has_buy_order
                        and self.cash_balance >= self.min_position_value
                    ):
                        await self.place_buy_order(current_price)

                    logging.info(
                        f"\n=== Current Trading Status ===\n"
                        f"No Active Position\n"
                        f"Current Price: ${current_price:.4f}\n"
                        f"Cash Balance: ${self.cash_balance:.2f}\n"
                        f"Buy Order: {'YES' if has_buy_order else 'NO'}\n"
                        f"\nTrading Mode: BUYING - Looking for entry at -2.5% below current price\n"
                        f"Target Entry: ${round(current_price * 0.975, 4):.4f}"
                    )
                else:
                    logging.warning(
                        "Cannot update position: No price data available. Will retry later."
                    )
                    # Try to wait for more data
                    await asyncio.sleep(10)  # Wait before retrying
                    return

            # Store last price for reference
            if position_info:
                self._last_logged_price = position_info["current_price"]

        except Exception as e:
            logging.error(f"Error updating position: {e}")

    async def calculate_fees(self, trade_amount_usd, current_price):
        """Calculate total fees for a trade"""
        trading_fee = trade_amount_usd * self.trading_fee_pct  # 0.4% trading fee
        network_fee_usd = self.network_fee_xrp * current_price  # XRP network fee
        total_fees = trading_fee + network_fee_usd
        return total_fees

    async def get_position(self):
        """Get current position if it exists, otherwise return None"""
        try:
            positions = self.trading_client.get_all_positions()
            for pos in positions:
                if pos.symbol == "XRP/USD":
                    return pos
            return None
        except Exception as e:
            logging.warning(f"Error getting position: {e}")
            return None

    async def check_trading_conditions(self, current_price):
        """Enhanced trading conditions check"""
        try:
            position = await self.get_position()
            orders = self.trading_client.get_orders()

            # No position case - ensure buy order exists
            if not position:
                has_buy_order = any(
                    o.symbol == "XRP/USD" and o.side == "buy" for o in orders
                )

                if not has_buy_order and self.cash_balance >= self.position_value:
                    # Place new buy order
                    buy_order = await self.place_buy_order(current_price)
                    if buy_order:
                        self.active_orders["buy"] = buy_order.id
                    return True

            # Position exists - ensure exit orders are in place
            else:
                has_tp_order = any(
                    o.symbol == "XRP/USD" and o.side == "sell" and o.type == "limit"
                    for o in orders
                )
                has_sl_order = any(
                    o.symbol == "XRP/USD"
                    and o.side == "sell"
                    and o.type == "stop_limit"
                    for o in orders
                )

                if not (has_tp_order and has_sl_order):
                    await self.ensure_exit_orders(position, current_price)

            return False

        except Exception as e:
            logging.error(f"Error in trading conditions check: {e}")
            return False

    async def place_buy_order(self, current_price):
        """Place a stop-limit buy order for XRP"""
        try:
            # Get account details
            account = self.trading_client.get_account()
            available_balance = float(account.cash)
            buying_power = float(account.buying_power)

            # Use the smaller of cash balance and buying power
            actual_available = min(available_balance, buying_power)
            max_affordable = (
                actual_available * 0.95
            )  # Use 95% of available balance to account for fees
            position_value = min(self.position_value, max_affordable)

            if position_value < self.min_position_value:
                logging.warning(
                    f"Insufficient funds for minimum position size:\n"
                    f"Required: ${self.min_position_value:.2f}\n"
                    f"Cash Balance: ${available_balance:.2f}\n"
                    f"Buying Power: ${buying_power:.2f}\n"
                    f"Actually Available: ${actual_available:.2f}"
                )
                return None

            # Calculate order details
            target_entry = round(
                current_price * 0.975, 4
            )  # Target 2.5% below current price
            stop_price = round(target_entry * 1.002, 4)
            limit_price = round(target_entry * 1.005, 4)
            quantity = round(position_value / target_entry, 1)

            # Place stop-limit buy order
            buy_order = self.trading_client.submit_order(
                StopLimitOrderRequest(
                    symbol="XRP/USD",
                    qty=str(quantity),
                    side=OrderSide.BUY,
                    time_in_force=TimeInForce.GTC,
                    stop_price=str(stop_price),
                    limit_price=str(limit_price),
                )
            )

            logging.info(
                f"\nPlaced new buy order:\n"
                f"Entry Price: ${target_entry:.4f} (-2.5%)\n"
                f"Quantity: {quantity} XRP\n"
                f"Position Value: ${position_value:.2f}\n"
                f"Available Balance: ${actual_available:.2f}"
            )

            return buy_order

        except Exception as e:
            logging.error(f"Error placing buy order: {e}")
            return None

    async def place_sell_order(self, current_price, reason):
        """Place a limit sell order for XRP"""
        try:
            # Get current position
            positions = self.trading_client.get_all_positions()
            position = None
            for pos in positions:
                if pos.symbol == "XRP/USD":
                    position = pos
                    break

            if not position:
                logging.warning("No XRP position found to sell")
                return

            # Get available quantity
            available_qty = float(position.qty_available)
            if available_qty <= 0:
                logging.warning(
                    f"No available XRP to place orders (total: {position.qty}, available: {available_qty})"
                )
                return

            sell_value = available_qty * current_price
            entry_price = float(position.avg_entry_price)

            # Calculate fees for the sell
            fees = await self.calculate_fees(sell_value, current_price)

            # Calculate limit price based on reason
            if reason == "take_profit":
                limit_price = entry_price * (1 + self.take_profit_threshold)
            elif reason == "stop_loss":
                limit_price = (
                    current_price * 0.995
                )  # Slightly below current price for quick execution
            else:
                limit_price = (
                    current_price * 0.998
                )  # Regular sell slightly below current price

            # Round limit price to 4 decimal places
            limit_price = round(limit_price, 4)

            # Create limit order request
            order_data = LimitOrderRequest(
                symbol="XRP/USD",
                qty=str(available_qty),
                side=OrderSide.SELL,
                time_in_force=TimeInForce.GTC,
                limit_price=str(limit_price),
            )

            # Place the order
            order = self.trading_client.submit_order(order_data)
            self.last_order_time = datetime.now()

            # Update total fees
            self.total_fees_paid += fees

            # Store order details with fees
            self.store_order(order, fees)

            logging.info(
                f"\nLimit sell order placed: {available_qty} XRP ({reason})\n"
                f"Current Price: ${current_price:.4f}\n"
                f"Limit Price: ${limit_price:.4f}\n"
                f"Order Value: ${sell_value:.2f}\n"
                f"Fees: ${fees:.2f}\n"
                f"Total Fees Paid: ${self.total_fees_paid:.2f}\n"
                f"\nPreparing for next trade:\n"
                f"Will attempt to buy when price drops -2.5% below ${current_price:.4f}\n"
                f"Target entry around: ${current_price * 0.975:.4f}"
            )

            # Update position information
            await self.update_position()

            return order

        except Exception as e:
            logging.error(f"Error placing sell order: {e}")
            return None

    async def initialize_orders(self):
        """Initialize orders on startup without canceling positions"""
        try:
            position = await self.get_position()
            current_price = self.get_last_price()

            if not current_price:
                logging.error("No price data available")
                return

            # Get existing orders WITHOUT canceling them
            orders = self.trading_client.get_orders()
            has_buy_order = any(o.side == "buy" for o in orders)
            has_tp_order = any(o.side == "sell" and o.type == "limit" for o in orders)
            has_sl_order = any(
                o.side == "sell" and o.type == "stop_limit" for o in orders
            )

            if position:
                entry_price = float(position.avg_entry_price)
                available_qty = float(position.qty_available)

                # Only add missing orders, don't cancel existing ones
                if available_qty > 0:
                    take_profit_price = entry_price * (1 + self.take_profit_threshold)
                    stop_loss_price = entry_price * (1 - self.stop_loss_threshold)

                    if not has_tp_order:
                        try:
                            tp_order = self.trading_client.submit_order(
                                LimitOrderRequest(
                                    symbol="XRP/USD",
                                    qty=str(available_qty),
                                    side=OrderSide.SELL,
                                    time_in_force=TimeInForce.GTC,
                                    limit_price=str(round(take_profit_price, 4)),
                                )
                            )
                            logging.info(
                                f"Added take profit order at ${take_profit_price:.4f} (+3.5%)"
                            )
                        except Exception as e:
                            logging.error(f"Error placing take profit order: {e}")

                    if not has_sl_order:
                        try:
                            sl_order = self.trading_client.submit_order(
                                StopLimitOrderRequest(
                                    symbol="XRP/USD",
                                    qty=str(available_qty),
                                    side=OrderSide.SELL,
                                    time_in_force=TimeInForce.GTC,
                                    stop_price=str(round(stop_loss_price, 4)),
                                    limit_price=str(round(stop_loss_price * 0.995, 4)),
                                )
                            )
                            logging.info(
                                f"Added stop loss order at ${stop_loss_price:.4f} (-1.5%)"
                            )
                        except Exception as e:
                            logging.error(f"Error placing stop loss order: {e}")
            else:
                # Only place buy order if no position and no existing buy order
                if not has_buy_order and self.cash_balance >= self.min_position_value:
                    await self.place_buy_order(current_price)

            # Log current status - Pass the position and orders to _log_trading_status
            self._log_trading_status(position, orders)

        except Exception as e:
            logging.error(f"Error initializing orders: {e}")

    async def process_message(self, message):
        """Enhanced message processing to handle all data types"""
        try:
            data = json.loads(message)
            if not isinstance(data, list):
                logging.debug("Received non-list message, skipping")
                return

            for msg in data:
                msg_type = msg.get("T")

                if msg_type == "q":  # Quote message
                    bid_price = float(msg.get("bp", 0))
                    ask_price = float(msg.get("ap", 0))
                    if bid_price > 0 and ask_price > 0:
                        current_price = (
                            bid_price + ask_price
                        ) / 2  # Calculate mid-price
                        timestamp_str = msg.get("t")

                        # Handle different timestamp formats
                        if isinstance(timestamp_str, str):
                            timestamp = datetime.fromisoformat(
                                timestamp_str.replace("Z", "+00:00")
                            )
                        else:
                            # Assuming it's a millisecond timestamp
                            timestamp = datetime.fromtimestamp(timestamp_str / 1000)

                        success = self.store_price(current_price, timestamp)

                        # Log less frequently - only once per 30 seconds
                        current_time = datetime.now()
                        if (
                            current_time.second % self.price_log_interval == 0
                        ) and success:
                            logging.info(f"Stored quote price: {current_price:.4f}")

                        # Only trigger position check on significant price changes
                        if (
                            self._last_logged_price is None
                            or abs(current_price - self._last_logged_price) > 0.01
                        ):
                            await self.update_position()
                            self._last_logged_price = current_price

                elif msg_type == "t":  # Trade message
                    # Handle trade message
                    price = float(msg.get("p", 0))
                    timestamp_str = msg.get("t")

                    if price > 0:
                        # Handle different timestamp formats
                        if isinstance(timestamp_str, str):
                            timestamp = datetime.fromisoformat(
                                timestamp_str.replace("Z", "+00:00")
                            )
                        else:
                            # Assuming it's a millisecond timestamp
                            timestamp = datetime.fromtimestamp(timestamp_str / 1000)

                        success = self.store_price(price, timestamp)

                        # Reduce logging frequency for trades
                        current_time = datetime.now()
                        if (
                            current_time.second % self.price_log_interval == 0
                        ) and success:
                            logging.info(f"Stored trade price: {price:.4f}")

                        # Trigger position check
                        await self.update_position()
                elif (
                    msg_type == "b"
                ):  # Bar message - enhanced to use for technical analysis
                    # Enhanced bar data handling
                    symbol = msg.get("S")
                    open_price = float(msg.get("o", 0))
                    high_price = float(msg.get("h", 0))
                    low_price = float(msg.get("l", 0))
                    close_price = float(msg.get("c", 0))
                    volume = float(msg.get("v", 0))
                    timestamp_str = msg.get("t")

                    # Only process if we have valid data
                    if close_price > 0 and symbol == self.symbol:
                        # Handle different timestamp formats
                        if isinstance(timestamp_str, str):
                            timestamp = datetime.fromisoformat(
                                timestamp_str.replace("Z", "+00:00")
                            )
                        else:
                            timestamp = datetime.fromtimestamp(timestamp_str / 1000)

                        # Only log bar data at the beginning of each minute to reduce noise
                        if timestamp.second == 0:
                            logging.info(
                                f"Bar [{timestamp.strftime('%H:%M')}]: O:{open_price:.4f} H:{high_price:.4f} "
                                f"L:{low_price:.4f} C:{close_price:.4f} V:{volume:.1f}"
                            )

                            # Calculate some basic technical indicators
                            await self.analyze_bar_data(
                                open_price,
                                high_price,
                                low_price,
                                close_price,
                                volume,
                                timestamp,
                            )

                        # Store closing price with low priority - don't duplicate quotes/trades data
                        stored_price = self.get_last_price()
                        if (
                            stored_price is None
                            or abs(stored_price - close_price) > 0.001
                        ):
                            success = self.store_price(close_price, timestamp)

                            # Reduce noise - only log once per minute
                            if success and timestamp.second == 0:
                                logging.info(
                                    f"Stored bar close price: {close_price:.4f}"
                                )

                                # Only update position on new minute bars
                                await self.update_position()

        except Exception as e:
            logging.error(f"Error processing message: {e}", exc_info=True)

    # Add a new method for basic technical analysis using bar data
    async def analyze_bar_data(
        self, open_price, high_price, low_price, close_price, volume, timestamp
    ):
        """Perform basic technical analysis on bar data"""
        try:
            # Get recent prices to calculate moving averages
            with self.engine.connect() as conn:
                # Get last 20 prices for simple moving average calculation
                recent_prices = conn.execute(
                    self.price_history.select()
                    .order_by(self.price_history.c.timestamp.desc())
                    .limit(20)
                ).fetchall()

                if len(recent_prices) >= 5:  # Minimum 5 bars needed for analysis
                    # Calculate 5-period SMA
                    sma5 = sum(row.price for row in recent_prices[:5]) / 5

                    # Calculate 20-period SMA if enough data
                    sma20 = None
                    if len(recent_prices) >= 20:
                        sma20 = sum(row.price for row in recent_prices[:20]) / 20

                    # Basic trend analysis
                    if sma5 and sma20:
                        if sma5 > sma20:
                            trend = "BULLISH"
                        elif sma5 < sma20:
                            trend = "BEARISH"
                        else:
                            trend = "NEUTRAL"

                        # Log technical analysis only once per minute
                        logging.info(
                            f"Technical Analysis [{timestamp.strftime('%H:%M')}]: "
                            f"SMA5: ${sma5:.4f}, SMA20: ${sma20:.4f}, Trend: {trend}"
                        )

                        # Use trend information to adjust trading parameters
                        if trend == "BULLISH" and self.take_profit_threshold < 0.04:
                            # In bullish trend, slightly increase take profit target
                            self.take_profit_threshold = (
                                0.04  # 4% take profit in bullish trend
                            )
                            logging.info(
                                f"Adjusted take profit threshold to {self.take_profit_threshold:.1%} due to bullish trend"
                            )
                        elif trend == "BEARISH" and self.take_profit_threshold > 0.03:
                            # In bearish trend, lower take profit target to exit faster
                            self.take_profit_threshold = (
                                0.03  # 3% take profit in bearish trend
                            )
                            logging.info(
                                f"Adjusted take profit threshold to {self.take_profit_threshold:.1%} due to bearish trend"
                            )
        except Exception as e:
            logging.error(f"Error analyzing bar data: {e}")

    async def connect(self):
        """Connect to the WebSocket and authenticate with enhanced subscription"""
        try:
            # Close existing connection if any
            if self.ws:
                await self.ws.close()
                self.ws = None
                await asyncio.sleep(1)  # Wait for connection to fully close

            self.ws = await websockets.connect(self.ws_url)
            auth_data = {
                "action": "auth",
                "key": os.getenv("API_KEY"),
                "secret": os.getenv("SECRET_KEY"),
            }
            await self.ws.send(json.dumps(auth_data))
            response = await self.ws.recv()
            logging.info(f"Auth response: {response}")

            response_data = json.loads(response)
            if (
                isinstance(response_data, list)
                and response_data[0].get("msg") == "connected"
            ):
                # Initialize orders for any existing position
                await self.initialize_orders()

                # View price history before starting
                self.view_price_history()

                # Subscribe to trades, quotes, and minute bars - ensure quotes are included
                subscribe_data = {
                    "action": "subscribe",
                    "trades": [self.symbol],
                    "quotes": [self.symbol],
                    "bars": [self.symbol],
                }

                await self.ws.send(json.dumps(subscribe_data))
                response = await self.ws.recv()
                logging.info(f"Subscription response: {response}")
                logging.info(f"Starting to stream {self.symbol} data...")
                self.connected = True
                return True
            else:
                logging.error("Authentication failed")
                return False

        except Exception as e:
            logging.error(f"Error connecting to WebSocket: {e}")
            return False

    async def stream(self):
        """Main streaming loop with improved message handling"""
        try:
            # Connect to WebSocket
            await self.connect()

            # Start position checking in the background
            position_check_task = asyncio.create_task(self.continuous_position_check())

            # Start buy order monitoring in the background
            buy_order_monitor_task = asyncio.create_task(self.monitor_buy_orders())

            # Initialize database tables if needed
            self.init_database()

            # Main WebSocket loop
            while self.ws and self.connected:
                try:
                    message = await asyncio.wait_for(self.ws.recv(), timeout=30)

                    # Process raw message first
                    await self.process_message(message)

                    # Then try more specific handlers if needed
                    try:
                        data = json.loads(message)

                        if isinstance(data, list):
                            for msg in data:
                                if "T" in msg and msg["T"] == "trade":
                                    await self.handle_trade(msg)
                        elif "T" in data and data["T"] == "trade":
                            await self.handle_trade(data)
                    except Exception as e:
                        logging.error(f"Error processing JSON in stream: {e}")

                except asyncio.TimeoutError:
                    # Send ping to keep connection alive
                    try:
                        pong = await self.ws.ping()
                        await asyncio.wait_for(pong, timeout=10)
                        logging.debug("Ping successful, connection still alive")
                    except:
                        logging.warning("Ping failed, reconnecting...")
                        self.connected = False
                        break
                except Exception as e:
                    logging.error(f"Error in WebSocket loop: {e}")
                    await asyncio.sleep(5)

            # Cancel tasks if connection is lost
            if position_check_task and not position_check_task.done():
                position_check_task.cancel()

            if buy_order_monitor_task and not buy_order_monitor_task.done():
                buy_order_monitor_task.cancel()

        except Exception as e:
            logging.error(f"Error in stream: {e}")

        finally:
            # Try to reconnect if connection was lost
            if not self.connected:
                logging.info("Reconnecting in 5 seconds...")
                await asyncio.sleep(5)
                await self.stream()

    async def continuous_position_check(self):
        """Continuously check positions and orders regardless of price updates"""
        last_buy_attempt_time = datetime.now() - timedelta(
            minutes=10
        )  # Initialize with past time
        last_log_time = datetime.now() - timedelta(
            minutes=5
        )  # Initialize last log time

        while True:
            try:
                # Get current positions and orders
                position = await self.get_position()
                orders = self.trading_client.get_orders()
                current_price = self.get_last_price()

                # Only log detailed position status every 60 seconds to reduce noise
                current_time = datetime.now()
                if (
                    current_time - last_log_time
                ).total_seconds() >= 60:  # Log every 60 seconds instead of every 5 seconds
                    # Use the previously unused method to log detailed status
                    self._log_trading_status(position, orders)
                    last_log_time = current_time

                # Continue with normal position checking
                if position:
                    has_tp_order = any(
                        o.symbol == "XRP/USD" and o.side == "sell" and o.type == "limit"
                        for o in orders
                    )
                    has_sl_order = any(
                        o.symbol == "XRP/USD"
                        and o.side == "sell"
                        and o.type == "stop_limit"
                        for o in orders
                    )

                    if not (has_tp_order and has_sl_order):
                        entry_price = float(position.avg_entry_price)
                        available_qty = float(position.qty_available)

                        if available_qty > 0:
                            if not has_tp_order:
                                take_profit_price = round(
                                    entry_price * (1 + self.take_profit_threshold), 4
                                )
                                try:
                                    tp_order = self.trading_client.submit_order(
                                        LimitOrderRequest(
                                            symbol="XRP/USD",
                                            qty=str(available_qty),
                                            side=OrderSide.SELL,
                                            time_in_force=TimeInForce.GTC,
                                            limit_price=str(take_profit_price),
                                        )
                                    )
                                    self.active_orders["take_profit"] = tp_order.id
                                    self.store_order(tp_order)
                                    logging.info(
                                        f"Placed take profit order: {available_qty} XRP @ ${take_profit_price:.4f}"
                                    )
                                except Exception as e:
                                    logging.error(
                                        f"Error placing take profit order: {e}"
                                    )

                            if not has_sl_order:
                                stop_loss_price = round(
                                    entry_price * (1 - self.stop_loss_threshold), 4
                                )
                                limit_price = round(stop_loss_price * 0.995, 4)
                                try:
                                    sl_order = self.trading_client.submit_order(
                                        StopLimitOrderRequest(
                                            symbol="XRP/USD",
                                            qty=str(available_qty),
                                            side=OrderSide.SELL,
                                            time_in_force=TimeInForce.GTC,
                                            stop_price=str(stop_loss_price),
                                            limit_price=str(limit_price),
                                        )
                                    )
                                    self.active_orders["stop_loss"] = sl_order.id
                                    self.store_order(sl_order)
                                    logging.info(
                                        f"Placed stop loss order: {available_qty} XRP @ ${stop_loss_price:.4f}"
                                    )
                                except Exception as e:
                                    logging.error(f"Error placing stop loss order: {e}")
                else:
                    # More robust check for existing buy orders
                    buy_orders = [
                        o for o in orders if o.symbol == "XRP/USD" and o.side == "buy"
                    ]
                    buy_order_count = len(buy_orders)

                    # Only place a new buy order if:
                    # 1. There are no existing buy orders
                    # 2. We have enough cash balance
                    # 3. It's been at least 2 minutes since our last buy attempt
                    current_time = datetime.now()
                    time_since_last_attempt = (
                        current_time - last_buy_attempt_time
                    ).total_seconds()

                    if (
                        buy_order_count == 0
                        and current_price
                        and self.cash_balance >= self.min_position_value
                        and time_since_last_attempt >= 120
                    ):  # 2 minutes cooldown

                        logging.info(
                            f"No active buy orders found. Placing new buy order..."
                        )
                        await self.place_buy_order(current_price)
                        last_buy_attempt_time = current_time
                    elif buy_order_count > 0:
                        logging.info(
                            f"Found {buy_order_count} existing buy orders. Not placing additional orders."
                        )
                    elif time_since_last_attempt < 120:
                        logging.info(
                            f"Waiting for cooldown period. {120 - int(time_since_last_attempt)} seconds remaining before next buy attempt."
                        )

                # Update order status
                await self.update_order_status()

                # Wait before next check
                await asyncio.sleep(5)

            except Exception as e:
                logging.error(f"Error in continuous position check: {e}")
                await asyncio.sleep(5)

    async def monitor_buy_orders(self):
        """Continuously monitor buy orders until they're filled, then place sell orders"""
        logging.info("Starting buy order monitoring...")
        last_order_log_time = datetime.now() - timedelta(minutes=5)

        while True:
            try:
                # Get all orders
                orders = self.trading_client.get_orders()

                # Only log summary every minute to reduce noise
                current_time = datetime.now()
                if (current_time - last_order_log_time).total_seconds() >= 60:
                    logging.info(f"Monitoring {len(orders)} total orders")
                    last_order_log_time = current_time

                # Check for active buy orders
                buy_orders = [
                    o
                    for o in orders
                    if o.symbol == "XRP/USD" and o.side == OrderSide.BUY
                ]

                if buy_orders:
                    # Only log the number of orders, not each individual one
                    if (
                        current_time - last_order_log_time
                    ).total_seconds() < 5:  # Avoid double logging
                        logging.info(f"Monitoring {len(buy_orders)} active buy orders")

                    for buy_order in buy_orders:
                        order_id = buy_order.id
                        status = buy_order.status

                        # Only log status changes or significant events
                        if (
                            order_id not in self.last_order_status
                            or self.last_order_status[order_id] != status
                        ):
                            logging.info(
                                f"Buy order {order_id} status changed to: {status}"
                            )
                            self.last_order_status[order_id] = status

                        # Process order status
                        # If order is filled, place sell orders
                        if status == OrderStatus.FILLED:
                            filled_price = float(buy_order.filled_avg_price)
                            filled_qty = float(buy_order.filled_qty)

                            logging.info(
                                f"\n=== Buy Order Filled ===\n"
                                f"Order ID: {order_id}\n"
                                f"Bought: {filled_qty:.2f} XRP @ ${filled_price:.4f}\n"
                                f"Total Cost: ${(filled_price * filled_qty):.2f}\n"
                                f"Setting up exit orders..."
                            )

                            await self.place_exit_orders(
                                filled_price, filled_qty, order_id
                            )
                            break

                        # Check if a partially filled order is almost complete
                        elif status == OrderStatus.PARTIALLY_FILLED:
                            filled_price = float(buy_order.filled_avg_price)
                            filled_qty = float(buy_order.filled_qty)
                            total_qty = float(buy_order.qty)
                            fill_percentage = (filled_qty / total_qty) * 100

                            logging.info(
                                f"Buy order {order_id} is partially filled: {fill_percentage:.2f}% complete\n"
                                f"Filled: {filled_qty:.2f} of {total_qty:.2f} XRP @ ${filled_price:.4f}"
                            )

                            # Display the trading plan for this partially filled order
                            await self.display_order_trading_plan(buy_order)

                            # If the order is more than 95% filled, consider it as good as filled
                            if fill_percentage > 95:
                                logging.info(
                                    f"Buy order {order_id} is more than 95% filled, treating as complete\n"
                                    f"Setting up exit orders for the filled portion..."
                                )

                                await self.place_exit_orders(
                                    filled_price, filled_qty, order_id
                                )
                                break
                        else:
                            # Display the trading plan for this order
                            await self.display_order_trading_plan(buy_order)
                else:
                    logging.info("No buy orders found to monitor")

                # Wait before checking again
                await asyncio.sleep(5)

            except Exception as e:
                logging.error(f"Error in monitor_buy_orders: {e}")
                await asyncio.sleep(5)

    async def place_exit_orders(self, filled_price, filled_qty, order_id):
        """Place take profit and stop loss orders after a buy order is filled with better asset verification"""
        # Get current position to verify available quantity
        try:
            # Add retry mechanism to handle timing issues
            max_retries = 3
            retry_delay = 1  # seconds
            available_qty = 0
            position_found = False

            for retry in range(max_retries):
                positions = self.trading_client.get_all_positions()
                position = None
                for pos in positions:
                    if pos.symbol == "XRP/USD":
                        position = pos
                        break

                if position and float(position.qty) > 0:
                    available_qty = float(position.qty)
                    position_found = True
                    logging.info(
                        f"Current position found on attempt {retry+1}: {available_qty} XRP available for exit orders"
                    )
                    break
                else:
                    if retry < max_retries - 1:
                        logging.info(
                            f"Position not found or zero quantity on attempt {retry+1}, retrying in {retry_delay} seconds..."
                        )
                        await asyncio.sleep(retry_delay)
                        retry_delay *= 2  # Exponential backoff

            if position_found:
                # Place take profit order
                take_profit_price = round(
                    filled_price * (1 + self.take_profit_threshold), 4
                )
                try:
                    tp_order = self.trading_client.submit_order(
                        LimitOrderRequest(
                            symbol="XRP/USD",
                            qty=str(available_qty),
                            side=OrderSide.SELL,
                            time_in_force=TimeInForce.GTC,
                            limit_price=str(take_profit_price),
                        )
                    )
                    self.active_orders["take_profit"] = tp_order.id
                    self.store_order(tp_order)
                    logging.info(
                        f"Take profit order placed: {available_qty} XRP @ ${take_profit_price:.4f}"
                    )
                except Exception as e:
                    logging.error(f"Error placing take profit order: {e}")

                # Place stop loss order
                stop_loss_price = round(
                    filled_price * (1 - self.stop_loss_threshold), 4
                )
                limit_price = round(stop_loss_price * 0.995, 4)
                try:
                    sl_order = self.trading_client.submit_order(
                        StopLimitOrderRequest(
                            symbol="XRP/USD",
                            qty=str(available_qty),
                            side=OrderSide.SELL,
                            time_in_force=TimeInForce.GTC,
                            stop_price=str(stop_loss_price),
                            limit_price=str(limit_price),
                        )
                    )
                    self.active_orders["stop_loss"] = sl_order.id
                    self.store_order(sl_order)
                    logging.info(
                        f"Stop loss order placed: {available_qty} XRP @ ${stop_loss_price:.4f}"
                    )
                except Exception as e:
                    logging.error(f"Error placing stop loss order: {e}")
            else:
                # If position still not found after retries, improve the fallback mechanism
                logging.warning(
                    "Position not found after retries. Checking account assets directly..."
                )

                try:
                    # Get account assets directly
                    account = self.trading_client.get_account()

                    # Check if we actually have any XRP holdings
                    has_xrp = False
                    available_qty = 0

                    # Try multiple methods to find XRP balance
                    try:
                        # First method: Check non-marginable buying power
                        for balance in account.non_marginable_buying_power:
                            if balance.asset_id == "XRP":
                                available_qty = float(balance.available)
                                has_xrp = True
                                logging.info(
                                    f"Found XRP balance in non-marginable assets: {available_qty}"
                                )
                                break
                    except (AttributeError, TypeError):
                        logging.info(
                            "Could not check non-marginable assets, trying positions API..."
                        )

                    # Second method: Try positions API again with different parameters
                    if not has_xrp:
                        try:
                            positions = self.trading_client.get_all_positions()
                            for pos in positions:
                                if pos.symbol == "XRP" or pos.symbol == "XRP/USD":
                                    available_qty = float(pos.qty)
                                    has_xrp = True
                                    logging.info(
                                        f"Found XRP position via secondary check: {available_qty}"
                                    )
                                    break
                        except Exception as e:
                            logging.warning(f"Error in secondary position check: {e}")

                    # Only place orders if we've verified XRP holdings
                    if has_xrp and available_qty > 0:
                        # Place take profit and stop loss orders
                        # Place take profit order
                        take_profit_price = round(
                            filled_price * (1 + self.take_profit_threshold), 4
                        )
                        try:
                            tp_order = self.trading_client.submit_order(
                                LimitOrderRequest(
                                    symbol="XRP/USD",
                                    qty=str(available_qty),
                                    side=OrderSide.SELL,
                                    time_in_force=TimeInForce.GTC,
                                    limit_price=str(take_profit_price),
                                )
                            )
                            self.active_orders["take_profit"] = tp_order.id
                            self.store_order(tp_order)
                            logging.info(
                                f"Take profit order placed: {available_qty} XRP @ ${take_profit_price:.4f}"
                            )
                        except Exception as e:
                            logging.error(f"Error placing take profit order: {e}")

                        # Place stop loss order
                        stop_loss_price = round(
                            filled_price * (1 - self.stop_loss_threshold), 4
                        )
                        limit_price = round(stop_loss_price * 0.995, 4)
                        try:
                            sl_order = self.trading_client.submit_order(
                                StopLimitOrderRequest(
                                    symbol="XRP/USD",
                                    qty=str(available_qty),
                                    side=OrderSide.SELL,
                                    time_in_force=TimeInForce.GTC,
                                    stop_price=str(stop_loss_price),
                                    limit_price=str(limit_price),
                                )
                            )
                            self.active_orders["stop_loss"] = sl_order.id
                            self.store_order(sl_order)
                            logging.info(
                                f"Stop loss order placed: {available_qty} XRP @ ${stop_loss_price:.4f}"
                            )
                        except Exception as e:
                            logging.error(f"Error placing stop loss order: {e}")
                    else:
                        logging.error(
                            "Could not verify XRP holdings. Will not place exit orders."
                        )

                        # As a last resort, check if the original filled quantity is reliable
                        if filled_qty > 0:
                            logging.warning(
                                f"Using filled quantity as last resort: {filled_qty} XRP"
                            )
                            # Place limited exit orders with the filled quantity
                            # Place take profit order
                            take_profit_price = round(
                                filled_price * (1 + self.take_profit_threshold), 4
                            )
                            try:
                                tp_order = self.trading_client.submit_order(
                                    LimitOrderRequest(
                                        symbol="XRP/USD",
                                        qty=str(filled_qty),
                                        side=OrderSide.SELL,
                                        time_in_force=TimeInForce.GTC,
                                        limit_price=str(take_profit_price),
                                    )
                                )
                                self.active_orders["take_profit"] = tp_order.id
                                self.store_order(tp_order)
                                logging.info(
                                    f"Take profit order placed: {filled_qty} XRP @ ${take_profit_price:.4f}"
                                )
                            except Exception as e:
                                logging.error(f"Error placing take profit order: {e}")

                            # Place stop loss order
                            stop_loss_price = round(
                                filled_price * (1 - self.stop_loss_threshold), 4
                            )
                            limit_price = round(stop_loss_price * 0.995, 4)
                            try:
                                sl_order = self.trading_client.submit_order(
                                    StopLimitOrderRequest(
                                        symbol="XRP/USD",
                                        qty=str(filled_qty),
                                        side=OrderSide.SELL,
                                        time_in_force=TimeInForce.GTC,
                                        stop_price=str(stop_loss_price),
                                        limit_price=str(limit_price),
                                    )
                                )
                                self.active_orders["stop_loss"] = sl_order.id
                                self.store_order(sl_order)
                                logging.info(
                                    f"Stop loss order placed: {filled_qty} XRP @ ${stop_loss_price:.4f}"
                                )
                            except Exception as e:
                                logging.error(f"Error placing stop loss order: {e}")
                        else:
                            logging.error(
                                "No reliable quantity data available. Cannot place exit orders."
                            )
                except Exception as e:
                    logging.error(
                        f"Error checking account balance and placing fallback orders: {e}"
                    )
        except Exception as e:
            logging.error(f"Error retrieving position after buy order fill: {e}")

    async def update_order_status(self):
        """Track and update order status"""
        try:
            # Get active orders
            orders = self.trading_client.get_orders()
            active_ids = {o.id for o in orders}

            # Get completed orders using the correct API format
            closed_orders_request = GetOrdersRequest(
                status=QueryOrderStatus.CLOSED, limit=100
            )
            completed_orders = self.trading_client.get_orders(
                filter=closed_orders_request
            )

            # Check for recently filled sell orders
            for order in completed_orders:
                if (
                    order.symbol == "XRP/USD"
                    and order.side == "sell"
                    and order.status == "filled"
                    and order.filled_qty
                ):

                    # A sell order was filled, clear active orders and prepare for new buy
                    self.active_orders = {
                        "buy": None,
                        "take_profit": None,
                        "stop_loss": None,
                    }

                    # Log the completed trade
                    filled_price = float(order.filled_avg_price)
                    filled_qty = float(order.filled_qty)
                    total_value = filled_price * filled_qty

                    logging.info(
                        f"\n=== Trade Completed ===\n"
                        f"Sold: {filled_qty:.2f} XRP @ ${filled_price:.4f}\n"
                        f"Total Value: ${total_value:.2f}\n"
                        f"Preparing for next trade..."
                    )

                    # Update account balance
                    self.update_account_balance()

                    # Place new buy order if we have enough balance
                    current_price = self.get_last_price()
                    if current_price and self.cash_balance >= self.min_position_value:
                        await self.place_buy_order(current_price)
                    break

            # Check for recently filled buy orders
            for order in completed_orders:
                if (
                    order.symbol == "XRP/USD"
                    and order.side == "buy"
                    and order.status == "filled"
                    and order.filled_qty
                ):

                    # A buy order was filled, place take profit and stop loss orders
                    filled_price = float(order.filled_avg_price)
                    filled_qty = float(order.filled_qty)

                    logging.info(
                        f"\n=== Buy Order Filled ===\n"
                        f"Bought: {filled_qty:.2f} XRP @ ${filled_price:.4f}\n"
                        f"Total Cost: ${(filled_price * filled_qty):.2f}\n"
                        f"Setting up exit orders..."
                    )

                    # Clear old orders
                    self.active_orders["buy"] = None

                    # Get current position to verify available quantity
                    try:
                        positions = self.trading_client.get_all_positions()
                        position = None
                        for pos in positions:
                            if pos.symbol == "XRP/USD":
                                position = pos
                                break

                        if position:
                            available_qty = float(position.qty)
                            logging.info(
                                f"Current position: {available_qty} XRP available for exit orders"
                            )

                            # Place take profit order
                            take_profit_price = round(
                                filled_price * (1 + self.take_profit_threshold), 4
                            )
                            try:
                                tp_order = self.trading_client.submit_order(
                                    LimitOrderRequest(
                                        symbol="XRP/USD",
                                        qty=str(available_qty),
                                        side=OrderSide.SELL,
                                        time_in_force=TimeInForce.GTC,
                                        limit_price=str(take_profit_price),
                                    )
                                )
                                self.active_orders["take_profit"] = tp_order.id
                                self.store_order(tp_order)
                                logging.info(
                                    f"Take profit order placed: {available_qty} XRP @ ${take_profit_price:.4f}"
                                )
                            except Exception as e:
                                logging.error(f"Error placing take profit order: {e}")

                            # Place stop loss order
                            stop_loss_price = round(
                                filled_price * (1 - self.stop_loss_threshold), 4
                            )
                            limit_price = round(stop_loss_price * 0.995, 4)
                            try:
                                sl_order = self.trading_client.submit_order(
                                    StopLimitOrderRequest(
                                        symbol="XRP/USD",
                                        qty=str(available_qty),
                                        side=OrderSide.SELL,
                                        time_in_force=TimeInForce.GTC,
                                        stop_price=str(stop_loss_price),
                                        limit_price=str(limit_price),
                                    )
                                )
                                self.active_orders["stop_loss"] = sl_order.id
                                self.store_order(sl_order)
                                logging.info(
                                    f"Stop loss order placed: {available_qty} XRP @ ${stop_loss_price:.4f}"
                                )
                            except Exception as e:
                                logging.error(f"Error placing stop loss order: {e}")
                        else:
                            logging.error(
                                "Buy order was filled but no position was found. Cannot place exit orders."
                            )
                    except Exception as e:
                        logging.error(
                            f"Error retrieving position after buy order fill: {e}"
                        )
                    break

            # Clear filled or cancelled orders from tracking
            for order_type, order_id in self.active_orders.items():
                if order_id and order_id not in active_ids:
                    self.active_orders[order_type] = None

        except Exception as e:
            logging.error(f"Error updating order status: {e}")

    async def handle_trade(self, data):
        """Handle incoming trade data"""
        try:
            # Extract trade data
            if isinstance(data, list):
                for item in data:
                    if item.get("T") == "trade":
                        self.process_trade_data(item)
            else:
                self.process_trade_data(data)

        except Exception as e:
            logging.error(f"Error handling trade data: {e}")

    def process_trade_data(self, trade):
        """Process a single trade data point with improved error handling"""
        try:
            # Extract trade information
            symbol = trade.get("S")
            price = float(trade.get("p", 0))

            # For quote data, calculate mid-price
            if trade.get("T") == "q":
                bid_price = float(trade.get("bp", 0))
                ask_price = float(trade.get("ap", 0))
                if bid_price > 0 and ask_price > 0:
                    price = (bid_price + ask_price) / 2

            timestamp_str = trade.get("t")

            if not all([symbol, price, timestamp_str]):
                logging.debug(
                    f"Incomplete trade data: Symbol={symbol}, Price={price}, Timestamp={timestamp_str}"
                )
                return

            # Convert timestamp to datetime
            try:
                if isinstance(timestamp_str, str):
                    timestamp = datetime.fromisoformat(
                        timestamp_str.replace("Z", "+00:00")
                    )
                else:
                    # Assuming it's a millisecond timestamp
                    timestamp = datetime.fromtimestamp(timestamp_str / 1000)
            except Exception as e:
                logging.error(f"Error parsing timestamp {timestamp_str}: {e}")
                timestamp = datetime.now()

            # Store price in memory
            self.last_price = price

            # Store price in database
            success = self.store_price(price, timestamp)

            # Log trade (only occasionally to avoid too much output)
            if timestamp.second % 10 == 0:  # Log only every 10 seconds
                logging.info(
                    f"{timestamp.strftime('%Y-%m-%d %H:%M:%S')} | {symbol} | ${price:.4f}"
                )

        except Exception as e:
            logging.error(f"Error processing trade data: {e}")

    def view_price_history(self):
        """View limited recent price history to avoid excessive logging"""
        try:
            with self.engine.connect() as conn:
                # Limit to last 10 entries to avoid excessive logging
                result = conn.execute(
                    self.price_history.select()
                    .order_by(self.price_history.c.timestamp.desc())
                    .limit(10)  # Only show last 10 entries
                ).fetchall()

                if not result:
                    logging.info("No price history found")
                    return

                logging.info("\nRecent Price History (Last 10 entries):")
                logging.info("Timestamp | Symbol | Price")
                logging.info("-" * 50)

                for row in result:
                    dt = row.timestamp
                    formatted_time = dt.strftime("%Y-%m-%d %H:%M:%S")
                    logging.info(f"{formatted_time} | {row.symbol} | ${row.price:.4f}")

        except Exception as e:
            logger.error(f"Error viewing price history: {e}")

    async def display_trading_plan(self):
        """Display current trading plan based on XRP price"""
        try:
            # Get current price
            current_price = None
            with self.engine.connect() as conn:
                result = conn.execute(
                    self.price_history.select()
                    .order_by(self.price_history.c.timestamp.desc())
                    .limit(1)
                ).fetchone()
                if result:
                    current_price = float(result.price)

            if not current_price:
                logging.info("No current price available")
                return

            # Get current position
            position = await self.get_position()

            logging.info("\n=== XRP Trading Plan ===")
            logging.info(f"Current XRP Price: ${current_price:.4f}")

            if position:
                # Position exists - show exit strategy
                entry_price = float(position.avg_entry_price)
                qty = float(position.qty)
                market_value = float(position.market_value)
                unrealized_pl = float(position.unrealized_pl)
                unrealized_plpc = float(position.unrealized_plpc)

                logging.info("\nCurrent Position:")
                logging.info(f"Holdings: {qty:.2f} XRP @ ${entry_price:.4f}")
                logging.info(f"Position Value: ${market_value:.2f}")
                logging.info(
                    f"Unrealized P/L: ${unrealized_pl:.2f} ({unrealized_plpc:.2%})"
                )
                logging.info("\nExit Strategy:")
                logging.info(
                    f"Take Profit Target: ${entry_price * (1 + self.take_profit_threshold):.4f} (+{self.take_profit_threshold:.1%})"
                )
                logging.info(
                    f"Stop Loss Level: ${entry_price * (1 - self.stop_loss_threshold)::.4f} (-{self.stop_loss_threshold:.1%})"
                )

            else:
                # No position - show entry strategy
                # Calculate potential entry points
                entry_price = round(
                    current_price * 0.975, 4
                )  # 2.5% below current price
                position_size = min(self.position_value, self.cash_balance)
                qty = round(position_size / entry_price, 1)

                logging.info("\nEntry Strategy:")
                logging.info(f"Target Entry Price: ${entry_price:.4f} (-2.5%)")
                logging.info(f"Planned Position Size: {qty:.1f} XRP")
                logging.info(f"Required Capital: ${position_size:.2f}")

                if self.cash_balance >= self.min_position_value:
                    take_profit_price = entry_price * (1 + self.take_profit_threshold)
                    stop_loss_price = entry_price * (1 - self.stop_loss_threshold)

                    potential_profit = (take_profit_price - entry_price) * qty
                    max_loss = (entry_price - stop_loss_price) * qty

                    logging.info("\nPlanned Exit Levels:")
                    logging.info(
                        f"Take Profit Target: ${take_profit_price:.4f} (+{self.take_profit_threshold:.1%})"
                    )
                    logging.info(
                        f"Stop Loss Level: ${stop_loss_price:.4f} (-{self.stop_loss_threshold:.1%})"
                    )
                    logging.info(f"Potential Profit: ${potential_profit:.2f}")
                    logging.info(f"Maximum Loss: ${max_loss:.2f}")
                else:
                    logging.info("\nInsufficient funds for new position")
                    logging.info(f"Required: ${self.min_position_value:.2f}")
                    logging.info(f"Available: ${self.cash_balance:.2f}")

            logging.info("=" * 30)

        except Exception as e:
            logging.error(f"Error displaying trading plan: {e}")

    async def display_order_trading_plan(self, buy_order):
        """Display the trading plan for a specific buy order with reduced redundancy"""
        try:
            order_id = buy_order.id
            order_status = buy_order.status

            # Only display the trading plan if the order status changed or it's been a while
            current_time = datetime.now()
            time_since_last_log = (
                current_time - self.last_trading_plan_log
            ).total_seconds()

            if (
                order_id not in self.last_order_status
                or self.last_order_status[order_id] != order_status
                or time_since_last_log >= 300
            ):  # Log at most every 5 minutes

                # Update tracking
                self.last_order_status[order_id] = order_status
                self.last_trading_plan_log = current_time

                # Continue with existing code...
                order_price = (
                    float(buy_order.limit_price)
                    if buy_order.limit_price
                    else float(buy_order.filled_avg_price)
                )
                order_qty = float(buy_order.qty)
                filled_qty = float(buy_order.filled_qty) if buy_order.filled_qty else 0
                remaining_qty = order_qty - filled_qty

                # Calculate the planned exit prices
                take_profit_price = round(
                    order_price * (1 + self.take_profit_threshold), 4
                )
                stop_loss_price = round(order_price * (1 - self.stop_loss_threshold), 4)

                # Calculate potential profit/loss
                potential_profit = (take_profit_price - order_price) * order_qty
                potential_loss = (order_price - stop_loss_price) * order_qty

                # Calculate risk-reward ratio with division by zero protection
                risk_reward_ratio = (
                    potential_profit / potential_loss if potential_loss > 0 else "∞"
                )

                logging.info(
                    f"\n=== Trading Plan for Order {order_id} ===\n"
                    f"Status: {buy_order.status}\n"
                    f"Buy Price: ${order_price:.4f}\n"
                    f"Quantity: {order_qty:.2f} XRP\n"
                    f"Filled: {filled_qty:.2f} XRP ({(filled_qty/order_qty*100):.2f}%)\n"
                    f"Remaining: {remaining_qty:.2f} XRP\n"
                    f"\nWhen this order fills, the following exit orders will be placed:\n"
                    f"Take Profit: Sell {order_qty:.2f} XRP @ ${take_profit_price:.4f} (+{self.take_profit_threshold*100:.2f}%)\n"
                    f"Stop Loss: Sell {order_qty:.2f} XRP @ ${stop_loss_price:.4f} (-{self.stop_loss_threshold*100:.2f}%)\n"
                    f"\nPotential Profit: ${potential_profit:.2f}\n"
                    f"Potential Loss: ${potential_loss:.2f}\n"
                    f"Risk-Reward Ratio: {risk_reward_ratio if isinstance(risk_reward_ratio, str) else f'{risk_reward_ratio:.2f}'}\n"
                    f"========================================="
                )
        except Exception as e:
            logging.error(
                f"Error displaying trading plan for order {buy_order.id}: {e}"
            )

    async def check_and_set_orders(self):
        """Check for filled positions and set appropriate sell orders"""
        try:
            # Get current positions
            position = await self.get_position()

            # Get all orders
            orders = self.trading_client.get_orders()

            if position:
                entry_price = float(position.avg_entry_price)
                available_qty = float(position.qty_available)

                # Check if we already have sell orders for this position
                has_tp_order = False
                has_sl_order = False

                for order in orders:
                    if order.symbol == "XRP/USD" and order.side == "sell":
                        if float(order.limit_price) > entry_price:
                            has_tp_order = True
                        else:
                            has_sl_order = True

                # If we have a position but no take profit order, create one
                if available_qty > 0 and not has_tp_order:
                    take_profit_price = round(
                        entry_price * (1 + self.take_profit_threshold), 4
                    )

                    try:
                        tp_order = self.trading_client.submit_order(
                            LimitOrderRequest(
                                symbol="XRP/USD",
                                qty=str(available_qty),
                                side=OrderSide.SELL,
                                time_in_force=TimeInForce.GTC,
                                limit_price=str(take_profit_price),
                            )
                        )
                        logging.info(f"\nNew take profit order placed:")
                        logging.info(f"Entry Price: ${entry_price:.4f}")
                        logging.info(
                            f"Take Profit: ${take_profit_price:.4f} (+{self.take_profit_threshold:.1%})"
                        )
                        logging.info(f"Quantity: {available_qty} XRP")

                        # Store order in database
                        self.store_order(tp_order)

                    except Exception as e:
                        logging.error(f"Error placing take profit order: {e}")

                # If we have a position but no stop loss order, create one
                if available_qty > 0 and not has_sl_order:
                    stop_loss_price = round(
                        entry_price * (1 - self.stop_loss_threshold), 4
                    )
                    limit_price = round(
                        stop_loss_price * 0.995, 4
                    )  # Slightly below stop price

                    try:
                        sl_order = self.trading_client.submit_order(
                            StopLimitOrderRequest(
                                symbol="XRP/USD",
                                qty=str(available_qty),
                                side=OrderSide.SELL,
                                time_in_force=TimeInForce.GTC,
                                stop_price=str(stop_loss_price),
                                limit_price=str(limit_price),
                            )
                        )
                        logging.info(f"\nNew stop loss order placed:")
                        logging.info(
                            f"Stop Loss: ${stop_loss_price:.4f} (-{self.stop_loss_threshold:.1%})"
                        )
                        logging.info(f"Limit Price: ${limit_price:.4f}")

                        # Store order in database
                        self.store_order(sl_order)

                    except Exception as e:
                        logging.error(f"Error placing stop loss order: {e}")

        except Exception as e:
            logging.error(f"Error in check_and_set_orders: {e}")

    def store_order(self, order, fees=0):
        """Store order details in database with retry logic and duplicate handling"""
        retries = 0
        max_retries = self.max_retries

        while retries < max_retries:
            try:
                with self.engine.connect() as conn:
                    # Check if order already exists to avoid duplicates
                    existing_order = conn.execute(
                        sqlalchemy.select(self.orders.c.order_id).where(
                            self.orders.c.order_id == order.id
                        )
                    ).fetchone()

                    if existing_order:
                        # Update existing order instead of inserting
                        conn.execute(
                            self.orders.update()
                            .where(self.orders.c.order_id == order.id)
                            .values(
                                timestamp=datetime.now(),
                                side=order.side,
                                quantity=float(order.qty) if order.qty else 0,
                                price=(
                                    float(order.limit_price) if order.limit_price else 0
                                ),
                                status=order.status,
                                filled_qty=(
                                    float(order.filled_qty) if order.filled_qty else 0
                                ),
                                filled_avg_price=(
                                    float(order.filled_avg_price)
                                    if order.filled_avg_price
                                    else 0
                                ),
                                fees=fees,
                            )
                        )
                    else:
                        # Insert new order
                        conn.execute(
                            self.orders.insert().values(
                                order_id=order.id,
                                timestamp=datetime.now(),
                                side=order.side,
                                quantity=float(order.qty) if order.qty else 0,
                                price=(
                                    float(order.limit_price) if order.limit_price else 0
                                ),
                                status=order.status,
                                filled_qty=(
                                    float(order.filled_qty) if order.filled_qty else 0
                                ),
                                filled_avg_price=(
                                    float(order.filled_avg_price)
                                    if order.filled_avg_price
                                    else 0
                                ),
                                fees=fees,
                            )
                        )
                    conn.commit()
                    return True
            except Exception as e:
                retries += 1
                logging.error(
                    f"Error storing order (attempt {retries}/{max_retries}): {e}"
                )
                if retries >= max_retries:
                    return False
                time.sleep(self.retry_delay)
        return False

    def _log_trading_status(self, position=None, orders=None):
        """Log detailed trading status"""
        try:
            logging.info("\n=== Detailed Position & Order Status ===")

            # Get current position if not provided
            if not position:
                try:
                    positions = self.trading_client.get_all_positions()
                    for pos in positions:
                        if pos.symbol == "XRP/USD":
                            position = pos
                            break
                except Exception as e:
                    logging.error(
                        f"Error getting positions in _log_trading_status: {e}"
                    )

            # Get all orders if not provided
            if not orders:
                try:
                    orders = self.trading_client.get_orders()
                except Exception as e:
                    logging.error(f"Error getting orders in _log_trading_status: {e}")
                    orders = []

            if position:
                entry_price = float(position.avg_entry_price)
                qty = float(position.qty)
                market_value = float(position.market_value)
                unrealized_pl = float(position.unrealized_pl)
                unrealized_plpc = float(position.unrealized_plpc)

                logging.info(f"\nActive Position:")
                logging.info(f"  Entry Price: ${entry_price:.4f}")
                logging.info(f"  Current Quantity: {qty} XRP")
                logging.info(f"  Market Value: ${market_value:.2f}")
                logging.info(
                    f"  Unrealized P/L: ${unrealized_pl:.2f} ({unrealized_plpc:.2%})"
                )
            else:
                logging.info("\nNo Active Position")

            if orders:
                logging.info("\nActive Orders:")
                for order in orders:
                    order_type = "Buy Order" if order.side == "buy" else "Sell Order"
                    price = (
                        float(order.limit_price)
                        if hasattr(order, "limit_price") and order.limit_price
                        else 0
                    )
                    qty = float(order.qty)
                    filled = float(order.filled_qty) if order.filled_qty else 0

                    logging.info(
                        f"{order_type}:"
                        f"  Status: {order.status}"
                        f"  Price: ${price:.4f}"
                        f"  Quantity: {qty} XRP"
                        f"  Filled: {filled} XRP"
                    )
            else:
                logging.info("\nNo Active Orders")

            # Get account info
            account = self.trading_client.get_account()
            self.cash_balance = float(account.cash)
            buying_power = float(account.buying_power)

            logging.info(f"\nAccount Status:")
            logging.info(f"Cash Balance: ${self.cash_balance:.2f}")
            logging.info(f"Buying Power: ${buying_power:.2f}")
            logging.info("=" * 40)

        except Exception as e:
            logging.error(f"Error logging trading status: {e}")


async def main():
    streamer = XRPStreamer()
    await streamer.stream()


if __name__ == "__main__":
    asyncio.run(main())
