import logging
import asyncio
import json
import websockets
from datetime import datetime, timedelta
import pytz
import time
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import (
    MarketOrderRequest,
    LimitOrderRequest,
    StopLimitOrderRequest,
    GetOrdersRequest,
    TrailingStopOrderRequest,
    StopOrderRequest,
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
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from dotenv import load_dotenv
import os
import pandas as pd
import numpy as np

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

        # Fee structure
        self.trading_fee_pct = 0.016  # 1.6% trading fee
        self.network_fee_xrp = 0.56  # XRP network fee

        # Dynamic thresholds using Average True Range (ATR)
        self.atr_period = 14
        self.risk_multiplier = 1.5  # Adjust aggressiveness
        self.take_profit_multiplier = 2.0
        self.stop_loss_multiplier = 1.0

        # These will be replaced with dynamic values from ATR
        self.take_profit_threshold = 0.035  # Will be dynamically set
        self.stop_loss_threshold = 0.015  # Will be dynamically set

        # Position sizing
        self.min_position_value = 4000
        self.max_position_size = 25000
        self.position_value = 20000
        self.risk_percentage = 0.015  # Risk 1.5% of account equity per trade

        # Order management
        self.max_open_orders = 2
        self.order_expiry = timedelta(minutes=15)
        self.last_order_time = None
        self.min_order_interval = timedelta(minutes=2)

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

            # Define orders table
            self.orders = Table(
                "orders",
                metadata,
                Column("id", Integer, primary_key=True),
                Column("order_id", String, nullable=False),
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

        except Exception as e:
            logger.error(f"Error initializing database: {e}")
            raise

    def store_price(self, price, symbol=None, timestamp=None):
        """Store a new price"""
        try:
            with self.engine.connect() as conn:
                if timestamp is None:
                    timestamp = datetime.now()
                if symbol is None:
                    symbol = self.symbol
                conn.execute(
                    self.price_history.insert().values(
                        timestamp=timestamp, price=price, symbol=symbol
                    )
                )
                conn.commit()
        except Exception as e:
            logger.error(f"Error storing price: {e}")

    def get_last_price(self):
        """Get the most recent price from database"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    self.price_history.select()
                    .order_by(self.price_history.c.timestamp.desc())
                    .limit(1)
                ).fetchone()
                return result.price if result else None
        except Exception as e:
            logger.error(f"Error getting last price: {e}")
            return None

    def store_order(self, order, fees=0):
        """Store order information in database"""
        try:
            with self.engine.connect() as conn:
                conn.execute(
                    self.orders.insert().values(
                        order_id=order.id,
                        timestamp=datetime.now(),
                        side=order.side,
                        quantity=float(order.qty),
                        price=float(order.limit_price or 0),
                        status=order.status,
                        filled_qty=float(order.filled_qty or 0),
                        filled_avg_price=float(order.filled_avg_price or 0),
                        fees=fees,
                    )
                )
                conn.commit()
        except Exception as e:
            logger.error(f"Error storing order: {e}")

    async def update_position(self, position_info=None):
        """Update current position information and manage orders"""
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
                if current_price:
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

            # Store last price for reference
            if position_info:
                self._last_logged_price = position_info["current_price"]

        except Exception as e:
            logging.error(f"Error updating position: {e}")

    def update_account_balance(self):
        """Update the current cash balance"""
        account = self.trading_client.get_account()
        self.cash_balance = min(float(account.cash), float(account.buying_power))
        return self.cash_balance

    async def calculate_fees(self, trade_amount_usd, current_price):
        """Calculate total fees for a trade"""
        trading_fee = trade_amount_usd * self.trading_fee_pct  # 1.6% trading fee
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

    def get_historical_prices(self, lookback=100):
        """Retrieve price history for ATR calculation"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    self.price_history.select()
                    .order_by(self.price_history.c.timestamp.desc())
                    .limit(lookback)
                ).fetchall()

                if not result:
                    logging.warning("No historical prices found for ATR calculation")
                    return []

                # Convert to list of prices
                prices = [float(row.price) for row in result]
                return prices
        except Exception as e:
            logging.error(f"Error getting historical prices: {e}")
            return []

    async def calculate_dynamic_thresholds(self):
        """Calculate volatility-based thresholds using custom ATR implementation"""
        prices = self.get_historical_prices()

        # Use default values if not enough price history
        if len(prices) < self.atr_period:
            logging.info(f"Not enough price history for ATR. Using default thresholds.")
            return {
                "entry_offset": 0.015,  # Default 1.5% entry offset
                "take_profit": 0.035,  # Default 3.5% take profit
                "stop_loss": 0.015,  # Default 1.5% stop loss
            }

        # Calculate ATR manually without pandas_ta
        df = pd.DataFrame(prices, columns=["close"])
        # Need high/low/close for ATR, but we only have close prices
        # Use close price for all to approximate
        df["high"] = df["close"]
        df["low"] = df["close"]

        # Calculate True Range
        df["previous_close"] = df["close"].shift(1)
        df["tr1"] = abs(df["high"] - df["low"])
        df["tr2"] = abs(df["high"] - df["previous_close"])
        df["tr3"] = abs(df["low"] - df["previous_close"])
        df["tr"] = df[["tr1", "tr2", "tr3"]].max(axis=1)

        # Calculate ATR
        df["atr"] = df["tr"].rolling(window=self.atr_period).mean()
        current_atr = df["atr"].iloc[-1]

        if pd.isna(current_atr) or current_atr == 0:
            logging.warning("ATR calculation returned invalid value, using defaults")
            return {"entry_offset": 0.015, "take_profit": 0.035, "stop_loss": 0.015}

        # Calculate percentage ATR (relative to current price)
        current_price = prices[0] if prices else 1.0
        atr_pct = current_atr / current_price

        # Set thresholds based on ATR
        entry_offset_pct = atr_pct * self.risk_multiplier
        take_profit_pct = atr_pct * self.take_profit_multiplier
        stop_loss_pct = atr_pct * self.stop_loss_multiplier

        # Update the instance variables for later reference
        self.take_profit_threshold = take_profit_pct
        self.stop_loss_threshold = stop_loss_pct

        logging.info(
            f"Dynamic thresholds calculated: Entry offset: {entry_offset_pct:.4f}, Take profit: {take_profit_pct:.4f}, Stop loss: {stop_loss_pct:.4f}"
        )

        return {
            "entry_offset": entry_offset_pct,
            "take_profit": take_profit_pct,
            "stop_loss": stop_loss_pct,
        }

    async def check_trading_conditions(self, current_price):
        """Enhanced trading conditions check with dynamic thresholds"""
        try:
            # Update thresholds using ATR
            thresholds = await self.calculate_dynamic_thresholds()

            position = await self.get_position()
            orders = self.trading_client.get_orders()

            # No position case - ensure buy order exists
            if not position:
                has_buy_order = any(
                    o.symbol == "XRP/USD" and o.side == "buy" for o in orders
                )

                if not has_buy_order and self.cash_balance >= self.position_value:
                    # Place new buy order with dynamic entry
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
                    and (o.type == "stop_limit" or o.type == "trailing_stop")
                    for o in orders
                )

                if not (has_tp_order and has_sl_order):
                    await self.ensure_exit_orders(position, current_price)

            return False

        except Exception as e:
            logging.error(f"Error in trading conditions check: {e}")
            return False

    async def place_buy_order(self, current_price):
        """Place a stop-limit buy order for XRP using dynamic thresholds"""
        try:
            # Get account details
            account = self.trading_client.get_account()
            available_balance = float(account.cash)
            buying_power = float(account.buying_power)
            equity = float(account.equity)

            # Use the smaller of cash balance and buying power
            actual_available = min(available_balance, buying_power)

            # First check for any existing orders and cancel them
            try:
                orders = self.trading_client.get_orders()
                existing_buy_orders = [
                    o
                    for o in orders
                    if o.symbol == "XRP/USD"
                    and o.side == "buy"
                    and o.status not in ["filled", "canceled"]
                ]

                if existing_buy_orders:
                    logging.info(
                        f"Found {len(existing_buy_orders)} existing buy orders. Cancelling to avoid conflicts."
                    )
                    for order in existing_buy_orders:
                        try:
                            self.trading_client.cancel_order_by_id(order.id)
                            logging.info(f"Cancelled existing buy order {order.id}")
                            await asyncio.sleep(1)
                        except Exception as e:
                            logging.warning(
                                f"Failed to cancel buy order {order.id}: {e}"
                            )

                    # Wait for cancellations to process
                    await asyncio.sleep(3)
            except Exception as e:
                logging.warning(f"Error checking/cancelling existing buy orders: {e}")

            # Use fixed 3% discount instead of dynamic ATR-based threshold
            fixed_entry_offset = 0.03  # 3% discount

            # Fixed target entry at 3% below current price
            target_entry = round(current_price * (1 - fixed_entry_offset), 4)
            stop_price = round(target_entry * 1.002, 4)
            limit_price = round(target_entry * 1.005, 4)

            # Still calculate dynamic thresholds for take profit and stop loss
            thresholds = await self.calculate_dynamic_thresholds()

            # Risk-based position sizing (risk % of equity)
            risk_amount = equity * self.risk_percentage
            max_affordable = actual_available * 0.95  # Use 95% of available balance

            # Calculate position size based on risk and stop loss
            stop_loss_amount = target_entry * thresholds["stop_loss"]
            if stop_loss_amount > 0:
                position_value = min(
                    risk_amount / thresholds["stop_loss"], max_affordable
                )
            else:
                # Fallback if stop loss calculation fails
                position_value = min(self.position_value, max_affordable)

            # Ensure minimum and maximum position size
            position_value = max(
                min(position_value, self.max_position_size), self.min_position_value
            )

            if position_value < self.min_position_value:
                logging.warning(
                    f"Insufficient funds for minimum position size:\n"
                    f"Required: ${self.min_position_value:.2f}\n"
                    f"Cash Balance: ${available_balance:.2f}\n"
                    f"Buying Power: ${buying_power:.2f}\n"
                    f"Actually Available: ${actual_available:.2f}"
                )
                return None

            # Calculate quantity
            quantity = round(position_value / target_entry, 1)

            # Place stop-limit buy order with expiry
            buy_order = self.trading_client.submit_order(
                StopLimitOrderRequest(
                    symbol="XRP/USD",
                    qty=str(quantity),
                    side=OrderSide.BUY,
                    time_in_force=TimeInForce.GTC,
                    stop_price=str(stop_price),
                    limit_price=str(limit_price),
                    expire_at=datetime.now() + self.order_expiry,
                )
            )

            logging.info(
                f"\nPlaced new buy order:\n"
                f"Entry Price: ${target_entry:.4f} (-{fixed_entry_offset*100:.2f}%)\n"
                f"Quantity: {quantity} XRP\n"
                f"Position Value: ${position_value:.2f}\n"
                f"Available Balance: ${actual_available:.2f}\n"
                f"Risk Amount: ${risk_amount:.2f}"
            )

            return buy_order

        except Exception as e:
            logging.error(f"Error placing buy order: {e}")
            return None

    async def place_exit_orders(self, filled_price, filled_qty, order_id):
        """Place take profit and trailing stop orders after a buy order is filled"""
        try:
            # Wait longer for the position to be reflected in the system
            await asyncio.sleep(10)  # Increased from 5 seconds to 10 seconds

            # First, check and cancel ALL existing orders including the order that just filled
            # This is crucial to prevent wash trade errors
            try:
                orders = self.trading_client.get_orders()
                existing_orders = [o for o in orders if o.symbol == "XRP/USD"]

                if existing_orders:
                    logging.info(
                        f"Found {len(existing_orders)} existing orders. Cancelling ALL to prevent conflicts."
                    )
                    for order in existing_orders:
                        try:
                            # Skip already filled orders
                            if order.status in ["filled", "canceled", "expired"]:
                                continue

                            self.trading_client.cancel_order_by_id(order.id)
                            logging.info(f"Successfully cancelled order {order.id}")
                            await asyncio.sleep(1)  # Brief pause between cancellations
                        except Exception as e:
                            logging.warning(f"Failed to cancel order {order.id}: {e}")

                    # Wait to ensure cancellations have processed
                    await asyncio.sleep(5)
            except Exception as e:
                logging.error(f"Error cancelling existing orders: {e}")

            # Now check for existing position with retries
            max_retries = 5
            retry_delay = 3
            available_qty = 0
            position_found = False

            for retry in range(max_retries):
                # Double check that all orders are indeed cancelled
                try:
                    active_orders = self.trading_client.get_orders()
                    active_orders = [
                        o
                        for o in active_orders
                        if o.symbol == "XRP/USD"
                        and o.status not in ["filled", "canceled", "expired"]
                    ]
                    if active_orders:
                        logging.warning(
                            f"Still found {len(active_orders)} active orders. Waiting..."
                        )
                        await asyncio.sleep(2)  # Wait a bit more
                except Exception as e:
                    logging.warning(f"Error checking active orders: {e}")

                # Get positions
                try:
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
                            f"Current position found on attempt {retry+1}: {available_qty} XRP available"
                        )
                        break
                    else:
                        if retry < max_retries - 1:
                            logging.info(
                                f"Position not found on attempt {retry+1}, retrying in {retry_delay} seconds..."
                            )
                            await asyncio.sleep(retry_delay)
                except Exception as e:
                    logging.warning(
                        f"Error checking positions on attempt {retry+1}: {e}"
                    )
                    await asyncio.sleep(retry_delay)

            # If no position found, use a more conservative estimate of filled quantity
            if not position_found:
                logging.warning(
                    f"Position not found after {max_retries} retries. Using conservative estimate of filled quantity."
                )
                available_qty = round(
                    filled_qty * 0.9, 1
                )  # Use 90% of filled quantity to be safe
                logging.info(f"Using conservative estimate: {available_qty} XRP")

            # If quantity is still too large, use what we know is available from error message
            if available_qty > 4000:  # Based on error message showing ~4285 available
                available_qty = 4000  # Use a safe limit well under what error showed
                logging.info(
                    f"Limiting quantity to {available_qty} XRP based on known availability"
                )

            # Ensure we have a valid quantity
            available_qty = round(float(available_qty), 1)  # Round to 1 decimal place

            if available_qty <= 0:
                logging.error("No quantity available to place exit orders")
                return

            # Calculate dynamic thresholds
            thresholds = await self.calculate_dynamic_thresholds()
            take_profit_threshold = thresholds["take_profit"]
            stop_loss_threshold = thresholds["stop_loss"]

            # Place take profit order with progressively smaller quantities
            attempt_succeeded = False
            for attempt in range(3):
                qty_to_use = round(
                    available_qty * (0.9 - attempt * 0.1), 1
                )  # Start with 90%, then 80%, then 70%
                if qty_to_use <= 0:
                    break

                take_profit_price = round(filled_price * (1 + take_profit_threshold), 4)
                try:
                    logging.info(
                        f"Placing take profit order (attempt {attempt+1}): {qty_to_use} XRP @ ${take_profit_price:.4f}"
                    )

                    tp_order = self.trading_client.submit_order(
                        LimitOrderRequest(
                            symbol="XRP/USD",
                            qty=str(qty_to_use),
                            side=OrderSide.SELL,
                            time_in_force=TimeInForce.GTC,
                            limit_price=str(take_profit_price),
                        )
                    )
                    self.active_orders["take_profit"] = tp_order.id
                    self.store_order(tp_order)
                    logging.info(
                        f"Take profit order placed: {qty_to_use} XRP @ ${take_profit_price:.4f}"
                    )
                    attempt_succeeded = True

                    # Use the same successful quantity for stop limit
                    available_qty = qty_to_use
                    break
                except Exception as e:
                    logging.error(
                        f"Error placing take profit order (attempt {attempt+1}): {e}"
                    )
                    await asyncio.sleep(1)

            if not attempt_succeeded:
                logging.error(
                    "Failed to place take profit order after multiple attempts"
                )
                return  # Don't continue if we can't place take profit order

            # Place stop limit order (NOT stop order which is invalid for crypto)
            stop_loss_price = round(filled_price * (1 - stop_loss_threshold), 4)
            # Make limit price significantly lower to ensure execution in volatile market
            limit_price = round(stop_loss_price * 0.99, 4)

            try:
                logging.info(
                    f"Placing stop limit order: {available_qty} XRP @ stop ${stop_loss_price:.4f}, limit ${limit_price:.4f}"
                )

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
                    f"Stop limit order placed: {available_qty} XRP @ stop ${stop_loss_price:.4f}, limit ${limit_price:.4f}"
                )
            except Exception as e:
                logging.error(f"Error placing stop limit order: {e}")

                # Try emergency limit sell order at a very low price as last resort
                try:
                    emergency_price = round(
                        filled_price * 0.85, 4
                    )  # 15% below filled price
                    emergency_order = self.trading_client.submit_order(
                        LimitOrderRequest(
                            symbol="XRP/USD",
                            qty=str(
                                round(available_qty * 0.9, 1)
                            ),  # Further reduce quantity
                            side=OrderSide.SELL,
                            time_in_force=TimeInForce.GTC,
                            limit_price=str(emergency_price),
                        )
                    )
                    logging.info(
                        f"Emergency limit sell order placed: {round(available_qty * 0.9, 1)} XRP @ ${emergency_price:.4f}"
                    )
                    self.active_orders["stop_loss"] = emergency_order.id
                    self.store_order(emergency_order)
                except Exception as e2:
                    logging.error(f"Failed to place emergency limit order: {e2}")
        except Exception as e:
            logging.error(f"Error in place_exit_orders: {e}")

    async def _place_take_profit_order(
        self, filled_price, available_qty, take_profit_threshold
    ):
        """Helper method to place take profit order with multiple fallback attempts"""
        # Start with the full quantity
        current_qty = available_qty
        take_profit_price = round(filled_price * (1 + take_profit_threshold), 4)

        # Try up to 3 times with reducing quantity
        for attempt in range(3):
            try:
                qty_to_use = round(
                    current_qty * (1 - attempt * 0.1), 1
                )  # Reduce by 10% each attempt
                if qty_to_use <= 0:
                    return False

                logging.info(
                    f"Placing take profit order attempt {attempt+1}: {qty_to_use} XRP @ ${take_profit_price:.4f}"
                )

                tp_order = self.trading_client.submit_order(
                    LimitOrderRequest(
                        symbol="XRP/USD",
                        qty=str(qty_to_use),
                        side=OrderSide.SELL,
                        time_in_force=TimeInForce.GTC,
                        limit_price=str(take_profit_price),
                    )
                )
                self.active_orders["take_profit"] = tp_order.id
                self.store_order(tp_order)
                logging.info(
                    f"Take profit order placed: {qty_to_use} XRP @ ${take_profit_price:.4f} (+{take_profit_threshold*100:.2f}%)"
                )
                return True

            except Exception as e:
                logging.warning(
                    f"Error placing take profit order (attempt {attempt+1}): {e}"
                )
                # Wait briefly before retrying
                await asyncio.sleep(1)

        return False

    async def _place_stop_limit_order(
        self, filled_price, available_qty, stop_loss_threshold
    ):
        """Helper method to place stop limit order with proper parameters for crypto"""
        # For crypto, make the limit price much lower than stop price to ensure execution
        stop_loss_price = round(filled_price * (1 - stop_loss_threshold), 4)
        limit_price = round(
            stop_loss_price * 0.98, 4
        )  # 2% below stop to ensure execution

        try:
            # Use progressively smaller quantities to avoid errors
            for attempt in range(3):
                qty_to_try = round(available_qty * (0.9 - attempt * 0.1), 1)
                if qty_to_try <= 0:
                    return False

                logging.info(
                    f"Attempting to place stop limit with {qty_to_try} XRP (attempt {attempt+1})"
                )
                sl_order = self.trading_client.submit_order(
                    StopLimitOrderRequest(
                        symbol="XRP/USD",
                        qty=str(qty_to_try),
                        side=OrderSide.SELL,
                        time_in_force=TimeInForce.GTC,
                        stop_price=str(stop_loss_price),
                        limit_price=str(limit_price),
                    )
                )
                self.active_orders["stop_loss"] = sl_order.id
                self.store_order(sl_order)
                logging.info(
                    f"Stop limit order placed: {qty_to_try} XRP @ stop ${stop_loss_price:.4f}, limit ${limit_price:.4f}"
                )
                return True

            return False
        except Exception as e:
            logging.error(f"Error placing stop limit order: {e}")
            return False

    def _place_fallback_stop_loss(
        self, filled_price, available_qty, stop_loss_threshold
    ):
        """Fallback method to place a regular stop loss if trailing stop fails"""
        try:
            # Ensure we have a valid quantity
            available_qty = round(float(available_qty), 1)  # Round to 1 decimal place
            stop_loss_price = round(filled_price * (1 - stop_loss_threshold), 4)
            limit_price = round(stop_loss_price * 0.995, 4)

            # Cancel any existing stop orders to avoid wash trade errors
            try:
                orders = self.trading_client.get_orders()
                existing_stop_orders = [
                    o
                    for o in orders
                    if o.symbol == "XRP/USD"
                    and o.side == "sell"
                    and (o.type == "stop" or o.type == "stop_limit")
                ]

                for order in existing_stop_orders:
                    self.trading_client.cancel_order_by_id(order.id)
                    logging.info(f"Cancelled existing stop order {order.id}")
                    # Brief pause after cancellation
                    time.sleep(0.5)
            except Exception as e:
                logging.warning(f"Error cancelling existing stop orders: {e}")

            # Try with slightly reduced quantity first
            reduced_qty = round(available_qty * 0.95, 1)  # Use 95% of available

            try:
                sl_order = self.trading_client.submit_order(
                    StopLimitOrderRequest(
                        symbol="XRP/USD",
                        qty=str(reduced_qty),
                        side=OrderSide.SELL,
                        time_in_force=TimeInForce.GTC,
                        stop_price=str(stop_loss_price),
                        limit_price=str(limit_price),
                    )
                )
                self.active_orders["stop_loss"] = sl_order.id
                self.store_order(sl_order)
                logging.info(
                    f"Fallback stop loss order placed: {reduced_qty} XRP @ ${stop_loss_price:.4f} (-{stop_loss_threshold*100:.2f}%)"
                )
                return True
            except Exception as e:
                logging.error(f"Error placing fallback stop loss: {e}")

                # If there's a wash trade error, try placing a limit order at a price well below current market
                if "wash trade" in str(e).lower():
                    try:
                        emergency_price = round(
                            filled_price * 0.9, 4
                        )  # 10% below filled price
                        emergency_order = self.trading_client.submit_order(
                            LimitOrderRequest(
                                symbol="XRP/USD",
                                qty=str(
                                    round(reduced_qty * 0.9, 1)
                                ),  # Further reduce quantity
                                side=OrderSide.SELL,
                                time_in_force=TimeInForce.GTC,
                                limit_price=str(emergency_price),
                            )
                        )
                        logging.info(
                            f"Emergency limit sell order placed: {round(reduced_qty * 0.9, 1)} XRP @ ${emergency_price:.4f}"
                        )
                        self.active_orders["stop_loss"] = emergency_order.id
                        self.store_order(emergency_order)
                        return True
                    except Exception as e2:
                        logging.error(f"Failed to place emergency limit order: {e2}")
                        return False
                return False
        except Exception as e:
            logging.error(f"Error in fallback stop loss placement: {e}")
            return False

    async def ensure_exit_orders(self, position, current_price):
        """Ensure take profit and trailing stop orders exist for an open position"""
        if not position:
            return

        try:
            entry_price = float(position.avg_entry_price)
            available_qty = float(position.qty_available)

            # Round to one decimal place to avoid precision issues
            available_qty = round(available_qty, 1)

            if available_qty <= 0:
                return

            # Get all orders
            orders = self.trading_client.get_orders()

            # Check if we already have the necessary orders
            has_tp_order = any(
                o.symbol == "XRP/USD" and o.side == "sell" and o.type == "limit"
                for o in orders
            )
            has_sl_order = any(
                o.symbol == "XRP/USD"
                and o.side == "sell"
                and (
                    o.type == "stop_limit"
                )  # Changed from o.type == "stop" or o.type == "stop_limit"
                for o in orders
            )

            # Calculate dynamic thresholds
            thresholds = await self.calculate_dynamic_thresholds()
            take_profit_threshold = thresholds["take_profit"]
            stop_loss_threshold = thresholds["stop_loss"]

            # Place take profit order if needed
            if not has_tp_order:
                await self._place_take_profit_order(
                    entry_price, available_qty, take_profit_threshold
                )

            # Place stop limit order if needed (NOT using StopOrderRequest which is invalid for crypto)
            if not has_sl_order:
                await self._place_stop_limit_order(
                    entry_price, available_qty, stop_loss_threshold
                )

        except Exception as e:
            logging.error(f"Error ensuring exit orders: {e}")

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

            # Log current status with the correct parameters
            self._log_trading_status(position, orders)

        except Exception as e:
            logging.error(f"Error initializing orders: {e}")

    async def process_message(self, message):
        """Enhanced message processing with frequent position checks"""
        try:
            data = json.loads(message)
            if not isinstance(data, list):
                return

            for msg in data:
                msg_type = msg.get("T")

                if msg_type in ["q", "t"]:
                    current_price = None

                    if msg_type == "q":
                        bid_price = float(msg.get("bp", 0))
                        ask_price = float(msg.get("ap", 0))
                        if bid_price > 0 and ask_price > 0:
                            current_price = (bid_price + ask_price) / 2
                    else:  # Trade
                        current_price = float(msg.get("p", 0))

                    if current_price:
                        self.store_price(current_price)
                        # Check position and orders every 5 seconds
                        current_time = datetime.now()
                        if (
                            not self.last_check_time
                            or (current_time - self.last_check_time).total_seconds()
                            >= 5
                        ):
                            await self.update_position()  # This will now handle everything
                            self.last_check_time = current_time

        except Exception as e:
            logging.error(f"Error processing message: {e}")

    async def connect(self):
        """Connect to the WebSocket and authenticate with improved error handling"""
        try:
            # Close existing connection if any
            await self.close_websocket()

            # Connect with a timeout
            try:
                self.ws = await asyncio.wait_for(
                    websockets.connect(
                        self.ws_url,
                        ping_interval=20,  # Send ping every 20 seconds
                        ping_timeout=10,  # Wait 10 seconds for pong response
                        close_timeout=10,  # Allow 10 seconds for closing
                    ),
                    timeout=30,
                )
            except asyncio.TimeoutError:
                logging.error("Timeout while connecting to WebSocket")
                return False

            auth_data = {
                "action": "auth",
                "key": os.getenv("API_KEY"),
                "secret": os.getenv("SECRET_KEY"),
            }
            await self.ws.send(json.dumps(auth_data))

            try:
                response = await asyncio.wait_for(self.ws.recv(), timeout=10)
                logging.info(f"Auth response: {response}")
            except asyncio.TimeoutError:
                logging.error("Timeout waiting for authentication response")
                await self.close_websocket()
                return False

            response_data = json.loads(response)
            if (
                isinstance(response_data, list)
                and response_data[0].get("msg") == "connected"
            ):
                # Initialize orders for any existing position
                await self.initialize_orders()

                # View price history before starting
                self.view_price_history()

                # Subscribe to trades, quotes, and minute bars
                subscribe_data = {
                    "action": "subscribe",
                    "trades": [self.symbol],
                    "quotes": [self.symbol],
                    "bars": [self.symbol],
                }
                await self.ws.send(json.dumps(subscribe_data))

                try:
                    response = await asyncio.wait_for(self.ws.recv(), timeout=10)
                    logging.info(f"Subscription response: {response}")
                    logging.info(f"Starting to stream {self.symbol} data...")
                    self.connected = True
                    return True
                except asyncio.TimeoutError:
                    logging.error("Timeout waiting for subscription response")
                    await self.close_websocket()
                    return False
            else:
                logging.error("Authentication failed")
                await self.close_websocket()
                return False

        except Exception as e:
            logging.error(f"Error connecting to WebSocket: {e}")
            await self.close_websocket()
            return False

    async def close_websocket(self):
        """Safely close the WebSocket connection"""
        if self.ws:
            try:
                await asyncio.wait_for(self.ws.close(code=1000), timeout=5)
                logging.info("WebSocket closed properly")
            except Exception as e:
                logging.warning(f"Error closing WebSocket: {e}")
            finally:
                self.ws = None
                self.connected = False
                await asyncio.sleep(1)  # Wait for connection to fully close

    async def stream(self):
        """Main streaming loop with improved error handling and reconnection"""
        reconnect_delay = 5  # Initial reconnect delay
        max_reconnect_delay = 60  # Maximum reconnect delay

        while True:  # Continuous reconnection loop
            try:
                # Connect to WebSocket
                connection_successful = await self.connect()
                if not connection_successful:
                    logging.warning(
                        f"Connection failed, retrying in {reconnect_delay} seconds..."
                    )
                    await asyncio.sleep(reconnect_delay)
                    # Exponential backoff for reconnection attempts
                    reconnect_delay = min(reconnect_delay * 1.5, max_reconnect_delay)
                    continue

                # Reset reconnect delay on successful connection
                reconnect_delay = 5

                # Start position checking in the background
                position_check_task = asyncio.create_task(
                    self.continuous_position_check()
                )

                # Start buy order monitoring in the background
                buy_order_monitor_task = asyncio.create_task(self.monitor_buy_orders())

                # Initialize database tables if needed
                self.init_database()

                # Main WebSocket loop
                while self.ws and self.connected:
                    try:
                        message = await asyncio.wait_for(self.ws.recv(), timeout=30)
                        # Process valid message
                        if message:
                            try:
                                data = json.loads(message)
                                if "T" in data and data["T"] == "trade":
                                    await self.handle_trade(data)
                                # Process other message types
                                await self.process_message(message)
                            except json.JSONDecodeError:
                                logging.error(
                                    f"Received invalid JSON: {message[:100]}..."
                                )

                    except asyncio.TimeoutError:
                        # Send ping to keep connection alive
                        logging.debug(
                            "No messages for 30 seconds, checking connection..."
                        )
                        try:
                            pong_waiter = await self.ws.ping()
                            await asyncio.wait_for(pong_waiter, timeout=10)
                            logging.debug("Ping successful, connection still alive")
                        except (
                            asyncio.TimeoutError,
                            websockets.exceptions.ConnectionClosed,
                        ):
                            logging.warning(
                                "Ping failed or connection closed, reconnecting..."
                            )
                            self.connected = False
                            break
                    except websockets.exceptions.ConnectionClosed as e:
                        logging.warning(f"WebSocket connection closed: {e}")
                        self.connected = False
                        break
                    except Exception as e:
                        logging.error(f"Error in WebSocket loop: {e}")
                        # Don't break immediately for non-connection errors
                        await asyncio.sleep(1)

                # Cancel tasks if connection is lost
                for task in [position_check_task, buy_order_monitor_task]:
                    if task and not task.done():
                        task.cancel()
                        try:
                            await task
                        except asyncio.CancelledError:
                            pass

                # Ensure WebSocket is properly closed
                await self.close_websocket()

                # Wait before reconnecting
                logging.info(f"Reconnecting in {reconnect_delay} seconds...")
                await asyncio.sleep(reconnect_delay)

            except Exception as e:
                logging.error(f"Fatal error in stream: {e}")
                # Ensure WebSocket is properly closed even after fatal error
                await self.close_websocket()
                await asyncio.sleep(reconnect_delay)
                # Exponential backoff for reconnection after errors
                reconnect_delay = min(reconnect_delay * 1.5, max_reconnect_delay)

    async def continuous_position_check(self):
        """Continuously check positions and orders regardless of price updates"""
        last_buy_attempt_time = datetime.now() - timedelta(
            minutes=10
        )  # Initialize with past time

        while True:
            try:
                # Get current positions and orders
                position = await self.get_position()
                orders = self.trading_client.get_orders()
                current_price = self.get_last_price()

                # Log all positions and orders status
                logging.info("\n=== Detailed Position & Order Status ===")

                # Position information
                if position:
                    logging.info(f"\nCurrent Position:")
                    logging.info(f"Quantity: {float(position.qty):.2f} XRP")
                    logging.info(f"Entry Price: ${float(position.avg_entry_price):.4f}")
                    logging.info(f"Current Value: ${float(position.market_value):.2f}")
                    logging.info(
                        f"Unrealized P/L: ${float(position.unrealized_pl):.2f} ({float(position.unrealized_plpc):.2%})"
                    )
                else:
                    logging.info("\nNo Active Position")

                # Order information
                logging.info("\nActive Orders:")
                if orders:
                    for order in orders:
                        order_type = "Buy" if order.side == "buy" else "Sell"
                        order_status = order.status
                        order_price = (
                            float(order.limit_price)
                            if order.limit_price
                            else float(order.stop_price)
                        )
                        order_qty = float(order.qty)
                        filled_qty = float(order.filled_qty) if order.filled_qty else 0

                        logging.info(
                            f"{order_type} Order:\n"
                            f"  Status: {order_status}\n"
                            f"  Price: ${order_price:.4f}\n"
                            f"  Quantity: {order_qty} XRP\n"
                            f"  Filled: {filled_qty} XRP"
                        )
                else:
                    logging.info("No Active Orders")

                # Account information
                account = self.trading_client.get_account()
                buying_power = float(account.buying_power)
                cash = float(account.cash)
                logging.info(f"\nAccount Status:")
                logging.info(f"Cash Balance: ${cash:.2f}")
                logging.info(f"Buying Power: ${buying_power:.2f}")
                logging.info("=" * 40 + "\n")

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
        """Continuously monitor buy orders with improved validation"""
        logging.info("Starting buy order monitoring...")

        while True:
            try:
                # Get all orders
                orders = self.trading_client.get_orders()

                # Check for active buy orders
                buy_orders = [
                    o
                    for o in orders
                    if o.symbol == "XRP/USD" and o.side == OrderSide.BUY
                ]

                if buy_orders:
                    for buy_order in buy_orders:
                        order_id = buy_order.id
                        status = buy_order.status
                        filled_qty = float(buy_order.filled_qty or 0)
                        total_qty = float(buy_order.qty)

                        # Only log detailed information for non-zero filled orders
                        if filled_qty > 0:
                            logging.info(
                                f"Monitoring buy order {order_id}: Status = {status}, "
                                f"Filled: {filled_qty}/{total_qty} XRP "
                                f"({(filled_qty/total_qty*100):.1f}%)"
                            )

                        # If order is filled, place sell orders - with extra verification
                        if status == OrderStatus.FILLED and filled_qty > 0:
                            filled_price = float(buy_order.filled_avg_price)

                            # Double check this order isn't already handled
                            if await self._is_newly_filled_order(order_id):
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

                        # Check if a partially filled order is almost complete - with verification
                        elif status == OrderStatus.PARTIALLY_FILLED and filled_qty > 0:
                            filled_price = float(buy_order.filled_avg_price)
                            fill_percentage = (filled_qty / total_qty) * 100

                            # Only log partial fills that changed since last check
                            logging.info(
                                f"Buy order {order_id} is partially filled: {fill_percentage:.2f}% complete\n"
                                f"Filled: {filled_qty:.2f} of {total_qty:.2f} XRP @ ${filled_price:.4f}"
                            )

                            # If the order is more than 95% filled, consider it as good as filled
                            if (
                                fill_percentage > 95
                                and await self._is_newly_filled_order(
                                    order_id, filled_qty
                                )
                            ):
                                logging.info(
                                    f"Buy order {order_id} is more than 95% filled, treating as complete\n"
                                    f"Setting up exit orders for the filled portion..."
                                )

                                await self.place_exit_orders(
                                    filled_price, filled_qty, order_id
                                )
                                break

                # Wait before checking again
                await asyncio.sleep(5)

            except Exception as e:
                logging.error(f"Error in monitor_buy_orders: {e}")
                await asyncio.sleep(5)

    async def place_exit_orders(self, filled_price, filled_qty, order_id):
        """Place take profit and trailing stop orders after a buy order is filled"""
        try:
            # Wait briefly for the position to be reflected in the system
            await asyncio.sleep(2)

            # Get current position to verify available quantity
            max_retries = 5  # Increased from 3
            retry_delay = 2  # seconds - increased from 1
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
                        retry_delay *= 1.5  # Exponential backoff with slower increase

            # If we haven't found a position after all retries, use the filled quantity directly
            if not position_found:
                logging.warning(
                    "Position not found after maximum retries. Checking account balance for XRP..."
                )
                # Try to get actual XRP balance from account
                try:
                    account_info = self.trading_client.get_account()
                    # Check if there's a method to get cryptocurrency balances
                    all_positions = self.trading_client.get_all_positions()

                    # Log what we found
                    logging.info(f"Account status: {account_info.status}")
                    logging.info(f"Found {len(all_positions)} positions")

                    # Use a smaller quantity than the filled quantity to account for potential fees
                    available_qty = filled_qty * 0.99  # Use 99% of filled quantity
                    logging.info(f"Using 99% of filled quantity: {available_qty} XRP")
                except Exception as e:
                    logging.error(f"Error getting account balance: {e}")
                    # Use a more conservative estimate
                    available_qty = filled_qty * 0.98  # Use 98% of filled quantity
                    logging.warning(
                        f"Using 98% of filled quantity as fallback: {available_qty} XRP"
                    )

            # Ensure we have a valid quantity and round appropriately
            available_qty = round(float(available_qty), 1)  # Round to 1 decimal place

            # Now that we have either position or fallback quantity, place the orders
            if available_qty > 0:
                # Calculate dynamic thresholds
                thresholds = await self.calculate_dynamic_thresholds()
                take_profit_threshold = thresholds["take_profit"]
                stop_loss_threshold = thresholds["stop_loss"]

                # Place take profit order
                take_profit_price = round(filled_price * (1 + take_profit_threshold), 4)
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
                        f"Take profit order placed: {available_qty} XRP @ ${take_profit_price:.4f} (+{take_profit_threshold*100:.2f}%)"
                    )
                except Exception as e:
                    logging.error(f"Error placing take profit order: {e}")
                    if "insufficient balance" in str(e).lower():
                        # Try with a smaller quantity
                        reduced_qty = round(available_qty * 0.98, 1)  # Try with 98%
                        logging.info(
                            f"Retrying with reduced quantity: {reduced_qty} XRP"
                        )
                        try:
                            tp_order = self.trading_client.submit_order(
                                LimitOrderRequest(
                                    symbol="XRP/USD",
                                    qty=str(reduced_qty),
                                    side=OrderSide.SELL,
                                    time_in_force=TimeInForce.GTC,
                                    limit_price=str(take_profit_price),
                                )
                            )
                            self.active_orders["take_profit"] = tp_order.id
                            self.store_order(tp_order)
                            logging.info(
                                f"Take profit order placed with reduced quantity: {reduced_qty} XRP @ ${take_profit_price:.4f}"
                            )
                            available_qty = reduced_qty  # Update available quantity for stop loss order
                        except Exception as e2:
                            logging.error(
                                f"Error placing take profit order with reduced quantity: {e2}"
                            )

                # Place stop order instead of trailing stop (trailing stop not supported for crypto)
                stop_loss_price = round(filled_price * (1 - stop_loss_threshold), 4)
                try:
                    sl_order = self.trading_client.submit_order(
                        StopOrderRequest(  # Use StopOrderRequest instead of TrailingStopOrderRequest
                            symbol="XRP/USD",
                            qty=str(available_qty),
                            side=OrderSide.SELL,
                            time_in_force=TimeInForce.GTC,
                            stop_price=str(stop_loss_price),
                        )
                    )
                    self.active_orders["stop_loss"] = sl_order.id
                    self.store_order(sl_order)
                    logging.info(
                        f"Stop loss order placed: {available_qty} XRP @ ${stop_loss_price:.4f} (-{stop_loss_threshold*100:.2f}%)"
                    )
                except Exception as e:
                    logging.error(f"Error placing stop order: {e}")
                    # Fallback to regular stop limit if stop order fails
                    self._place_fallback_stop_loss(
                        filled_price, available_qty, stop_loss_threshold
                    )
            else:
                logging.error(
                    f"Unable to place exit orders: No quantity available to sell"
                )

        except Exception as e:
            logging.error(f"Error in place_exit_orders: {e}")

    async def update_order_status(self):
        """Track and update order status with improved validation and retry logic"""
        try:
            # Get active and completed orders
            try:
                active_orders = self.trading_client.get_orders()
                active_ids = {o.id for o in active_orders}

                closed_orders_request = GetOrdersRequest(
                    status=QueryOrderStatus.CLOSED, limit=100
                )
                completed_orders = self.trading_client.get_orders(
                    filter=closed_orders_request
                )
            except Exception as e:
                logging.error(f"Error getting orders: {e}")
                return

            # Log active buy and sell orders separately for clarity
            active_buy_orders = [o for o in active_orders if o.side == "buy"]
            active_sell_orders = [o for o in active_orders if o.side == "sell"]

            if active_buy_orders:
                logging.info(f"Active buy orders: {len(active_buy_orders)}")
                for order in active_buy_orders:
                    logging.info(
                        f"Buy: {order.id} | Status: {order.status} | Qty: {order.qty} | Filled: {order.filled_qty or 0}"
                    )

            if active_sell_orders:
                logging.info(f"Active sell orders: {len(active_sell_orders)}")
                for order in active_sell_orders:
                    logging.info(
                        f"Sell: {order.id} | Status: {order.status} | Qty: {order.qty} | Filled: {order.filled_qty or 0}"
                    )

            # Check for filled sell orders (exit positions)
            for order in completed_orders:
                if (
                    order.symbol == "XRP/USD"
                    and order.side == "sell"
                    and order.status == OrderStatus.FILLED
                    and order.filled_qty
                    and await self._is_newly_filled_order(order.id)
                ):

                    filled_price = float(order.filled_avg_price)
                    filled_qty = float(order.filled_qty)
                    total_value = filled_price * filled_qty

                    # Calculate profit/loss if we can find the entry
                    try:
                        # Get buy orders to find entry price
                        buy_orders = [
                            o
                            for o in completed_orders
                            if o.side == "buy" and o.status == OrderStatus.FILLED
                        ]
                        if buy_orders:
                            # Use the most recent buy order as reference
                            buy_order = sorted(buy_orders, key=lambda x: x.filled_at)[
                                -1
                            ]
                            entry_price = float(buy_order.filled_avg_price)
                            profit_loss = (filled_price - entry_price) * filled_qty
                            profit_pct = (
                                (filled_price - entry_price) / entry_price * 100
                            )

                            logging.info(
                                f"\n=== POSITION CLOSED ===\n"
                                f"Sold: {filled_qty:.2f} XRP @ ${filled_price:.4f}\n"
                                f"Entry: ${entry_price:.4f}\n"
                                f"Exit: ${filled_price:.4f}\n"
                                f"P/L: ${profit_loss:.2f} ({profit_pct:.2f}%)\n"
                                f"Total Value: ${total_value:.2f}\n"
                                f"====================\n"
                            )
                        else:
                            logging.info(
                                f"\n=== POSITION CLOSED ===\n"
                                f"Sold: {filled_qty:.2f} XRP @ ${filled_price:.4f}\n"
                                f"Total Value: ${total_value:.2f}\n"
                                f"====================\n"
                            )
                    except Exception as e:
                        logging.error(f"Error calculating profit/loss: {e}")
                        logging.info(
                            f"\n=== POSITION CLOSED ===\n"
                            f"Sold: {filled_qty:.2f} XRP @ ${filled_price:.4f}\n"
                            f"Total Value: ${total_value:.2f}\n"
                            f"====================\n"
                        )

                    # Reset active order tracking
                    self.active_orders = {
                        "buy": None,
                        "take_profit": None,
                        "stop_loss": None,
                    }

                    # Update account balance and place new buy order
                    self.update_account_balance()
                    current_price = self.get_last_price()

                    # Wait a moment before placing new orders
                    await asyncio.sleep(5)

                    if current_price and self.cash_balance >= self.min_position_value:
                        await self.place_buy_order(current_price)
                    break

            # Clear stale order tracking
            for order_type, order_id in self.active_orders.items():
                if order_id and order_id not in active_ids:
                    old_id = order_id
                    self.active_orders[order_type] = None
                    logging.info(f"Cleared stale {order_type} order tracking: {old_id}")

        except Exception as e:
            logging.error(f"Error updating order status: {e}", exc_info=True)

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
        """Process a single trade data point"""
        try:
            # Extract trade information
            symbol = trade.get("S")
            price = float(trade.get("p", 0))
            timestamp_str = trade.get("t")

            if not all([symbol, price, timestamp_str]):
                return

            # Convert timestamp to datetime
            timestamp = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))

            # Store price in memory
            self.last_price = price

            # Store price in database
            self.store_price(symbol, price, timestamp)

            # Log trade (only occasionally to avoid too much output)
            if timestamp.second % 10 == 0:  # Log only every 10 seconds
                logging.info(
                    f"{timestamp.strftime('%Y-%m-%d %H:%M:%S')} | {symbol} | ${price:.4f}"
                )

        except Exception as e:
            logging.error(f"Error processing trade data: {e}")

    def view_price_history(self):
        """View all stored reference prices"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    self.price_history.select().order_by(
                        self.price_history.c.timestamp.desc()
                    )
                ).fetchall()

                if not result:
                    logging.info("No price history found")
                    return

                logging.info("\nPrice History:")
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
        """Display the trading plan for a specific buy order"""
        try:
            order_id = buy_order.id
            order_price = (
                float(buy_order.limit_price)
                if buy_order.limit_price
                else float(buy_order.filled_avg_price)
            )
            order_qty = float(buy_order.qty)
            filled_qty = float(buy_order.filled_qty) if buy_order.filled_qty else 0
            remaining_qty = order_qty - filled_qty

            # Calculate the planned exit prices
            take_profit_price = round(order_price * (1 + self.take_profit_threshold), 4)
            stop_loss_price = round(order_price * (1 - self.stop_loss_threshold), 4)

            # Calculate potential profit/loss
            potential_profit = (take_profit_price - order_price) * order_qty
            potential_loss = (order_price - stop_loss_price) * order_qty

            # Calculate risk-reward ratio
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

    def _log_trading_status(self, position=None, orders=None):
        """Log detailed trading status"""
        try:
            logging.info("\n=== Detailed Position & Order Status ===")

            # Get current position if not provided
            if position is None:
                positions = self.trading_client.get_all_positions()
                position = None
                for pos in positions:
                    if pos.symbol == "XRP/USD":
                        position = pos
                        break

            # Get all orders if not provided
            if orders is None:
                orders = self.trading_client.get_orders()

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
                        f"{order_type}: "
                        f"Status: {order.status}, "
                        f"Price: ${price:.4f}, "
                        f"Quantity: {qty} XRP, "
                        f"Filled: {filled} XRP"
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

    async def _is_newly_filled_order(self, order_id, filled_qty=None):
        """
        Check if an order is newly filled and hasn't been processed yet
        Returns True if the order should be processed, False otherwise
        """
        try:
            # Check if we've processed this order before
            if not hasattr(self, "_processed_orders"):
                self._processed_orders = {}

            # If order is in processed list with same/greater fill amount, skip it
            if order_id in self._processed_orders:
                if filled_qty is None or filled_qty <= self._processed_orders[order_id]:
                    logging.info(f"Order {order_id} already processed, skipping")
                    return False

            # Record this order as processed
            if filled_qty:
                self._processed_orders[order_id] = filled_qty
            else:
                self._processed_orders[order_id] = True

            # Clean up processed orders list occasionally (keep last 50)
            if len(self._processed_orders) > 50:
                oldest_keys = sorted(self._processed_orders.keys())[
                    : len(self._processed_orders) - 50
                ]
                for key in oldest_keys:
                    del self._processed_orders[key]

            return True
        except Exception as e:
            logging.error(f"Error checking order processing status: {e}")
            return True  # Default to processing on error


async def main():
    streamer = XRPStreamer()
    await streamer.stream()


if __name__ == "__main__":
    asyncio.run(main())
