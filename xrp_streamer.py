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

    def view_price_history(self):
        """View all stored reference prices"""
        try:
            logging.info("Retrieving price history from database...")
            with self.engine.connect() as conn:
                # Get most recent prices first, limit to 10 entries to avoid excessive logging
                result = conn.execute(
                    self.price_history.select()
                    .order_by(self.price_history.c.timestamp.desc())
                    .limit(10)
                ).fetchall()

                if not result:
                    logging.info("No price history found in database")
                    return

                logging.info("\nRecent Price History:")
                logging.info("Timestamp | Symbol | Price")
                logging.info("-" * 50)

                for row in result:
                    dt = row.timestamp
                    formatted_time = dt.strftime("%Y-%m-%d %H:%M:%S")
                    logging.info(f"{formatted_time} | {row.symbol} | ${row.price:.4f}")

                # Get average, min, max prices
                stats_query = f"""
                SELECT 
                    AVG(price) as avg_price,
                    MIN(price) as min_price,
                    MAX(price) as max_price,
                    COUNT(*) as total_records
                FROM price_history 
                WHERE symbol = '{self.symbol}'
                """

                try:
                    stats = conn.execute(text(stats_query)).fetchone()
                    if stats and stats.total_records > 0:
                        logging.info("\nPrice Statistics:")
                        logging.info(f"Total Records: {stats.total_records}")
                        logging.info(f"Average Price: ${stats.avg_price:.4f}")
                        logging.info(f"Minimum Price: ${stats.min_price:.4f}")
                        logging.info(f"Maximum Price: ${stats.max_price:.4f}")
                        logging.info(
                            f"Price Range: ${stats.max_price - stats.min_price:.4f}"
                        )
                except Exception as e:
                    logging.warning(f"Could not calculate price statistics: {e}")

                # Get the most recent price for reference
                most_recent = result[0] if result else None
                if most_recent:
                    self.reference_price = most_recent.price
                    logging.info(
                        f"\nMost recent price (${most_recent.price:.4f}) set as reference price"
                    )

        except Exception as e:
            logging.error(f"Error viewing price history: {e}")
            # Continue execution even if this fails
            pass

    async def display_order_trading_plan(self, buy_order):
        """Display the trading plan for a specific buy order"""
        try:
            order_id = buy_order.id
            order_price = float(
                buy_order.limit_price
                if buy_order.limit_price
                else (buy_order.filled_avg_price if buy_order.filled_avg_price else 0)
            )
            order_qty = float(buy_order.qty)
            filled_qty = float(buy_order.filled_qty) if buy_order.filled_qty else 0
            remaining_qty = order_qty - filled_qty

            if order_price <= 0:
                logging.warning(
                    f"Invalid order price for order {order_id}: {order_price}"
                )
                return

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

            # Calculate fees
            estimated_fees = await self.calculate_fees(
                order_price * order_qty, order_price
            )

            logging.info(
                f"\n=== Trading Plan for Order {order_id} ===\n"
                f"Status: {buy_order.status}\n"
                f"Buy Price: ${order_price:.4f}\n"
                f"Quantity: {order_qty:.2f} XRP\n"
                f"Filled: {filled_qty:.2f} XRP ({(filled_qty/order_qty*100) if order_qty > 0 else 0:.2f}%)\n"
                f"Remaining: {remaining_qty:.2f} XRP\n"
                f"Estimated Fees: ${estimated_fees:.2f}\n"
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

                # View price history before starting - handle potential errors
                try:
                    self.view_price_history()
                except Exception as e:
                    logging.warning(
                        f"Failed to view price history: {e}, but continuing..."
                    )

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

    def update_account_balance(self):
        """Update the current cash balance"""
        try:
            account = self.trading_client.get_account()
            self.cash_balance = min(float(account.cash), float(account.buying_power))
            return self.cash_balance
        except Exception as e:
            logging.error(f"Error updating account balance: {e}")
            self.cash_balance = 0.0
            return 0.0

    async def calculate_fees(self, trade_amount_usd, current_price):
        """Calculate total fees for a trade"""
        trading_fee = trade_amount_usd * self.trading_fee_pct  # 1.6% trading fee
        network_fee_usd = self.network_fee_xrp * current_price  # XRP network fee
        total_fees = trading_fee + network_fee_usd
        return total_fees

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


async def main():
    streamer = XRPStreamer()
    await streamer.stream()


if __name__ == "__main__":
    asyncio.run(main())
