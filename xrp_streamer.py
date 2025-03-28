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
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from dotenv import load_dotenv
import os
import math  # Add math import for floor function
import time  # Add time module for timestamp operations

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

        # Add WebSocket configuration
        self.ws_heartbeat_interval = 30  # Seconds between WebSocket heartbeats
        self.ws_last_heartbeat = None
        self.ws_reconnect_delay = 5  # Initial reconnect delay in seconds
        self.ws_max_reconnect_delay = 60  # Maximum reconnect delay
        self.ws_reconnect_attempts = 0  # Track reconnection attempts

        # Add position checking parameters
        self.position_check_retries = 10  # Increased number of retries
        self.position_check_delay = 2  # Initial delay between retries in seconds

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

    def store_price(self, symbol, price, timestamp=None):
        """Store a new price"""
        try:
            with self.engine.connect() as conn:
                if timestamp is None:
                    timestamp = datetime.now()
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

    async def stream(self):
        """Main streaming loop"""
        try:
            # Connect to WebSocket
            connected = await self.connect()
            if not connected:
                logging.error("Failed to connect to WebSocket. Retrying...")
                await asyncio.sleep(self.ws_reconnect_delay)
                self.ws_reconnect_delay = min(
                    self.ws_reconnect_delay * 2, self.ws_max_reconnect_delay
                )
                self.ws_reconnect_attempts += 1
                await self.stream()
                return

            # Reset reconnect parameters on successful connection
            self.ws_reconnect_delay = 5
            self.ws_reconnect_attempts = 0
            self.ws_last_heartbeat = datetime.now()

            # Start position checking in the background
            position_check_task = asyncio.create_task(self.continuous_position_check())

            # Start buy order monitoring in the background
            buy_order_monitor_task = asyncio.create_task(self.monitor_buy_orders())

            # Start WebSocket heartbeat task
            heartbeat_task = asyncio.create_task(self.send_heartbeat())

            # Initialize database tables if needed
            self.init_database()

            # Main WebSocket loop
            while self.ws and self.connected:
                try:
                    message = await asyncio.wait_for(self.ws.recv(), timeout=30)
                    await self.process_message(json.loads(message))
                    self.ws_last_heartbeat = (
                        datetime.now()
                    )  # Update heartbeat timestamp on successful message

                except asyncio.TimeoutError:
                    # Send ping to keep connection alive
                    try:
                        logging.info("Connection idle, sending ping...")
                        pong = await self.ws.ping()
                        await asyncio.wait_for(pong, timeout=10)
                        logging.debug("Ping successful, connection still alive")
                        self.ws_last_heartbeat = datetime.now()
                    except Exception as e:
                        logging.warning(f"Ping failed: {e}, reconnecting...")
                        self.connected = False
                        break

                except websockets.exceptions.ConnectionClosed as e:
                    logging.error(f"WebSocket connection closed: {e}")
                    self.connected = False
                    break

                except Exception as e:
                    logging.error(f"Error in WebSocket loop: {e}")
                    if "no close frame received or sent" in str(e):
                        logging.info(
                            "Detected WebSocket close frame issue, reconnecting..."
                        )
                        self.connected = False
                        break
                    await asyncio.sleep(1)

            # Cancel tasks if connection is lost
            for task in [position_check_task, buy_order_monitor_task, heartbeat_task]:
                if task and not task.done():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

        except Exception as e:
            logging.error(f"Critical error in stream: {e}")

        finally:
            # Try to reconnect if connection was lost
            if not self.connected:
                logging.info(
                    f"Reconnecting in {self.ws_reconnect_delay} seconds... (Attempt {self.ws_reconnect_attempts + 1})"
                )
                await asyncio.sleep(self.ws_reconnect_delay)
                self.ws_reconnect_delay = min(
                    self.ws_reconnect_delay * 2, self.ws_max_reconnect_delay
                )
                self.ws_reconnect_attempts += 1
                await self.stream()

    async def connect(self):
        """Connect to the WebSocket and authenticate"""
        try:
            # Close existing connection if any
            if self.ws:
                try:
                    await self.ws.close()
                except Exception as e:
                    logging.warning(f"Error closing existing connection: {e}")
                self.ws = None
                await asyncio.sleep(1)  # Wait for connection to fully close

            logging.info("Connecting to WebSocket...")
            self.ws = await websockets.connect(
                self.ws_url, ping_interval=20, ping_timeout=10
            )
            auth_data = {
                "action": "auth",
                "key": os.getenv("API_KEY"),
                "secret": os.getenv("SECRET_KEY"),
            }
            await self.ws.send(json.dumps(auth_data))
            response = await asyncio.wait_for(self.ws.recv(), timeout=10)
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

                # Subscribe to trades, quotes, and minute bars
                subscribe_data = {
                    "action": "subscribe",
                    "trades": [self.symbol],
                    "quotes": [self.symbol],
                    "bars": [self.symbol],
                }
                await self.ws.send(json.dumps(subscribe_data))
                response = await asyncio.wait_for(self.ws.recv(), timeout=10)
                logging.info(f"Subscription response: {response}")
                logging.info(f"Starting to stream {self.symbol} data...")
                self.connected = True
                self.ws_last_heartbeat = datetime.now()
                return True
            else:
                logging.error("Authentication failed")
                return False

        except Exception as e:
            logging.error(f"Error connecting to WebSocket: {e}")
            return False

    async def send_heartbeat(self):
        """Periodically send heartbeats to keep the connection alive"""
        while self.connected:
            try:
                await asyncio.sleep(self.ws_heartbeat_interval)
                if self.ws and self.connected:
                    # Check if we've received messages recently
                    time_since_heartbeat = (
                        datetime.now() - self.ws_last_heartbeat
                    ).total_seconds()
                    if time_since_heartbeat > self.ws_heartbeat_interval * 2:
                        logging.warning(
                            f"No messages received for {time_since_heartbeat:.1f}s, sending ping..."
                        )
                        try:
                            pong = await asyncio.wait_for(self.ws.ping(), timeout=5)
                            self.ws_last_heartbeat = datetime.now()
                            logging.info("Heartbeat ping successful")
                        except Exception as e:
                            logging.error(f"Heartbeat ping failed: {e}, will reconnect")
                            self.connected = False
                            break
            except asyncio.CancelledError:
                break
            except Exception as e:
                logging.error(f"Error in heartbeat: {e}")
                await asyncio.sleep(5)

    async def place_exit_orders(self, filled_price, filled_qty, order_id):
        """Place take profit and stop loss orders after a buy order is filled"""
        # Get current position to verify available quantity
        try:
            # Add retry mechanism with exponential backoff to handle timing issues
            max_retries = self.position_check_retries
            base_retry_delay = self.position_check_delay
            available_qty = 0
            position_found = False
            position = None

            logging.info(
                f"Looking for position after buy order fill - order_id: {order_id}"
            )

            for retry in range(max_retries):
                try:
                    positions = self.trading_client.get_all_positions()
                    position = None

                    # Log all positions for debugging
                    if positions:
                        logging.info(f"Found {len(positions)} total positions:")
                        for pos in positions:
                            logging.info(f"Position: {pos.symbol}, Qty: {pos.qty}")
                    else:
                        logging.info("No positions returned from API")

                    # Look for XRP position
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
                        # Check if the order was actually filled
                        order_status = None
                        try:
                            order = self.trading_client.get_order(order_id)
                            order_status = order.status
                            logging.info(f"Order {order_id} status: {order_status}")

                            if order_status != OrderStatus.FILLED:
                                logging.warning(
                                    f"Order {order_id} not actually filled (status: {order_status})"
                                )
                                return  # Exit if order not filled
                        except Exception as e:
                            logging.error(f"Error checking order status: {e}")

                        retry_delay = base_retry_delay * (
                            2**retry
                        )  # Exponential backoff
                        if retry < max_retries - 1:
                            logging.info(
                                f"Position not found or zero quantity on attempt {retry+1}, retrying in {retry_delay} seconds..."
                            )
                            await asyncio.sleep(retry_delay)
                except Exception as e:
                    logging.error(f"Error during position check retry {retry+1}: {e}")
                    await asyncio.sleep(base_retry_delay)

            # If position still not found after retries, use the filled quantity as fallback
            if not position_found or available_qty <= 0:
                logging.warning(
                    f"Position not found after {max_retries} retries. Using filled quantity from order ({filled_qty} XRP) as fallback"
                )
                available_qty = filled_qty

                # Additional check - try to get account positions again with direct API call
                try:
                    logging.info(
                        "Attempting a direct position query as a last resort..."
                    )
                    # Small delay to ensure any pending updates have time to process
                    await asyncio.sleep(2)
                    account_info = self.trading_client.get_account()
                    logging.info(f"Account status: {account_info.status}")

                    # Try once more with specific API call
                    positions = self.trading_client.get_all_positions()
                    for pos in positions:
                        if pos.symbol == "XRP/USD":
                            available_qty = float(pos.qty)
                            position_found = True
                            logging.info(
                                f"Position found on final check: {available_qty} XRP"
                            )
                            break
                except Exception as e:
                    logging.error(f"Error in final position check: {e}")

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
            stop_loss_price = round(filled_price * (1 - self.stop_loss_threshold), 4)
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
        except Exception as e:
            logging.error(f"Error retrieving position after buy order fill: {e}")

    async def monitor_buy_orders(self):
        """Continuously monitor buy orders until they're filled, then place sell orders"""
        logging.info("Starting buy order monitoring...")

        while True:
            try:
                # Get all orders with error handling
                orders = []
                for attempt in range(3):  # Try up to 3 times
                    try:
                        orders = self.trading_client.get_orders()
                        break  # Break if successful
                    except Exception as e:
                        if attempt < 2:  # Don't log on last attempt
                            logging.warning(
                                f"Error getting orders (attempt {attempt+1}): {e}"
                            )
                            await asyncio.sleep(2)  # Wait before retrying

                if not orders:
                    logging.error("Failed to retrieve orders after multiple attempts")
                    await asyncio.sleep(5)
                    continue

                logging.info(f"Retrieved {len(orders)} total orders")

                # Debug log all orders
                for order in orders:
                    logging.info(
                        f"Order: {order.id}, Symbol: {order.symbol}, Side: {order.side}, Status: {order.status}"
                    )

                # Check for active buy orders
                buy_orders = [
                    o
                    for o in orders
                    if o.symbol == "XRP/USD" and o.side == OrderSide.BUY
                ]
                logging.info(f"Filtered to {len(buy_orders)} buy orders for XRP/USD")

                if buy_orders:
                    logging.info(f"Monitoring {len(buy_orders)} active buy orders")
                    for buy_order in buy_orders:
                        order_id = buy_order.id
                        status = buy_order.status

                        logging.info(
                            f"Monitoring buy order {order_id}: Status = {status}"
                        )

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

                            # Update account balance before placing exit orders
                            self.update_account_balance()

                            # Add a brief delay before placing exit orders to ensure position is registered
                            logging.info("Waiting for position to be registered...")
                            await asyncio.sleep(2)

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

                                # Add a brief delay before placing exit orders to ensure position is registered
                                logging.info("Waiting for position to be registered...")
                                await asyncio.sleep(2)

                                await self.place_exit_orders(
                                    filled_price, filled_qty, order_id
                                )
                                break
                        else:
                            # Display the trading plan for this order
                            await self.display_order_trading_plan(buy_order)
                else:
                    logging.info("No buy orders found to monitor")

                # Check for recently filled orders that may have been missed
                try:
                    filled_orders_request = GetOrdersRequest(
                        status=QueryOrderStatus.CLOSED,
                        limit=10,
                        after=datetime.now() - timedelta(hours=1),
                    )
                    filled_orders = self.trading_client.get_orders(
                        filter=filled_orders_request
                    )

                    recent_filled_buys = [
                        o
                        for o in filled_orders
                        if o.symbol == "XRP/USD"
                        and o.side == "buy"
                        and o.status == "filled"
                    ]

                    if recent_filled_buys:
                        logging.info(
                            f"Found {len(recent_filled_buys)} recently filled buy orders"
                        )

                        # Check if any of these don't have matching sell orders
                        position = await self.get_position()
                        if position and not any(o.side == "sell" for o in orders):
                            logging.info(
                                "Found position without exit orders, setting up exit orders..."
                            )
                            entry_price = float(position.avg_entry_price)
                            qty = float(position.qty)
                            await self.place_exit_orders(
                                entry_price, qty, recent_filled_buys[0].id
                            )
                except Exception as e:
                    logging.error(f"Error checking recently filled orders: {e}")

                # Wait before checking again
                await asyncio.sleep(5)

            except asyncio.CancelledError:
                logging.info("Buy order monitor task cancelled")
                break

            except Exception as e:
                logging.error(f"Error in monitor_buy_orders: {e}")
                await asyncio.sleep(5)

    async def process_message(self, message):
        """Enhanced message processing with frequent position checks"""
        try:
            if not isinstance(message, list):
                return

            for msg in message:
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
                        self.store_price(self.symbol, current_price)
                        # Check position and orders every 5 seconds
                        current_time = datetime.now()
                        if (
                            not self.last_check_time
                            or (current_time - self.last_check_time).total_seconds()
                            >= 5
                        ):
                            await self.update_position()  # This will now handle everything
                            self.last_check_time = current_time

                            # Update last heartbeat time to indicate activity
                            self.ws_last_heartbeat = datetime.now()

        except Exception as e:
            logging.error(f"Error processing message: {e}")

    async def initialize_orders(self):
        """Initialize orders on startup without canceling positions"""
        try:
            position = await self.get_position()
            current_price = self.get_last_price()

            if not current_price:
                logging.error("No price data available")
                return

            logging.info("Initializing orders for existing positions...")

            # Get existing orders WITHOUT canceling them
            orders = self.trading_client.get_orders()
            has_buy_order = any(o.side == "buy" for o in orders)
            has_tp_order = any(o.side == "sell" and o.type == "limit" for o in orders)
            has_sl_order = any(
                o.side == "sell" and o.type == "stop_limit" for o in orders
            )

            logging.info(f"Found {len(orders)} existing orders")
            logging.info(f"Buy orders: {'Yes' if has_buy_order else 'No'}")
            logging.info(f"Take profit orders: {'Yes' if has_tp_order else 'No'}")
            logging.info(f"Stop loss orders: {'Yes' if has_sl_order else 'No'}")

            if position:
                logging.info(f"Found existing position: {position.qty} XRP")
                entry_price = float(position.avg_entry_price)
                available_qty = float(position.qty_available)

                # Only add missing orders, don't cancel existing ones
                if available_qty > 0:
                    take_profit_price = round(entry_price * (1 + self.take_profit_threshold), 4)
                    stop_loss_price = round(entry_price * (1 - self.stop_loss_threshold), 4)

                    if not has_tp_order:
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
                                    stop_price=str(stop_loss_price),
                                    limit_price=str(round(stop_loss_price * 0.995, 4)),
                                )
                            )
                            self.active_orders["stop_loss"] = sl_order.id
                            self.store_order(sl_order)
                            logging.info(
                                f"Added stop loss order at ${stop_loss_price:.4f} (-1.5%)"
                            )
                        except Exception as e:
                            logging.error(f"Error placing stop loss order: {e}")
                else:
                    logging.info(f"Position found but no available quantity to place orders")
            else:
                # Only place buy order if no position and no existing buy order
                logging.info("No existing position found")
                if not has_buy_order and self.cash_balance >= self.min_position_value:
                    logging.info("Placing initial buy order...")
                    await self.place_buy_order(current_price)
                elif self.cash_balance < self.min_position_value:
                    logging.warning(f"Insufficient funds for initial position. Required: ${self.min_position_value}, Available: ${self.cash_balance}")

            # Log current status
            await self._log_trading_status()
        except Exception as e:
            logging.error(f"Error initializing orders: {e}")
            # Don't raise the exception to allow the program to continue

    async def _log_trading_status(self):
        """Log detailed trading status"""
        try:
            logging.info("\n=== Detailed Position & Order Status ===")

            # Get current position
            positions = self.trading_client.get_all_positions()
            position = None
            for pos in positions:
                if pos.symbol == "XRP/USD":
                    position = pos
                    break

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

            # Get all orders
            orders = self.trading_client.get_orders()

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
                        f"{order_type}:\n"
                        f"  Status: {order.status}\n"
                        f"  Price: ${price:.4f}\n"
                        f"  Quantity: {qty} XRP\n"
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

    async def ensure_exit_orders(self, position, current_price):
        """Ensure that exit orders (take profit and stop loss) exist for a position"""
        try:
            # Get existing orders
            orders = self.trading_client.get_orders()
            has_tp_order = any(
                o.symbol == "XRP/USD" and o.side == "sell" and o.type == "limit"
                for o in orders
            )
            has_sl_order = any(
                o.symbol == "XRP/USD" and o.side == "sell" and o.type == "stop_limit"
                for o in orders
            )
            
            # Skip if both orders already exist
            if has_tp_order and has_sl_order:
                return
                
            # Get position details
            available_qty = float(position.qty_available)
            entry_price = float(position.avg_entry_price)
            
            if available_qty <= 0:
                logging.info("No available quantity to place exit orders")
                return
                
            # Place take profit order if missing
            if not has_tp_order:
                take_profit_price = round(entry_price * (1 + self.take_profit_threshold), 4)
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
                    logging.info(f"Added take profit order: {available_qty} XRP @ ${take_profit_price:.4f}")
                except Exception as e:
                    logging.error(f"Error placing take profit order: {e}")
                    
            # Place stop loss order if missing
            if not has_sl_order:
                stop_loss_price = round(entry_price * (1 - self.stop_loss_threshold), 4)
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
                    logging.info(f"Added stop loss order: {available_qty} XRP @ ${stop_loss_price:.4f}")
                except Exception as e:
                    logging.error(f"Error placing stop loss order: {e}")
                    
        except Exception as e:
            logging.error(f"Error ensuring exit orders: {e}")

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


async def main():
    streamer = XRPStreamer()
    await streamer.stream()


if __name__ == "__main__":
    asyncio.run(main())
