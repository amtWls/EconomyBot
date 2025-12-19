import discord
import aiosqlite
import os
from discord.ext import commands

from dotenv import load_dotenv

# -----------------------------------------------------------
# 設定 (Configuration)
# -----------------------------------------------------------
load_dotenv()

TOKEN = os.getenv("DISCORD_TOKEN")
HF_TOKEN = os.getenv("HF_TOKEN")
DB_NAME = "economy.db"

# -----------------------------------------------------------
# Bank システム (Bank System)
# -----------------------------------------------------------
class BankSystem:
    def __init__(self, db_path):
        self.db_path = db_path

    async def initialize(self):
        async with aiosqlite.connect(self.db_path, timeout=60.0) as db:
            await db.execute("PRAGMA journal_mode=WAL")
            # Bank table
            await db.execute("""
                CREATE TABLE IF NOT EXISTS bank (
                    user_id INTEGER,
                    guild_id INTEGER,
                    balance INTEGER DEFAULT 0,
                    PRIMARY KEY (user_id, guild_id)
                )
            """)
            # Market Items table
            await db.execute("""
                CREATE TABLE IF NOT EXISTS market_items (
                    item_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    seller_id INTEGER NOT NULL,
                    image_url TEXT NOT NULL,
                    image_hash TEXT,
                    aesthetic_score REAL NOT NULL,
                    price INTEGER NOT NULL,
                    status TEXT DEFAULT 'on_sale',
                    tags TEXT,
                    grade TEXT,
                    thread_id INTEGER,
                    message_id INTEGER,
                    buyer_id INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Indices for market_items
            await db.execute("CREATE INDEX IF NOT EXISTS idx_market_status ON market_items(status)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_market_buyer ON market_items(buyer_id)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_market_hash ON market_items(image_hash)")

            # Market Trends table
            await db.execute("""
                CREATE TABLE IF NOT EXISTS market_trends (
                    tag_name TEXT PRIMARY KEY,
                    current_price INTEGER DEFAULT 100,
                    saturation INTEGER DEFAULT 0,
                    trend_bonus INTEGER DEFAULT 0
                )
            """)
            
            await db.execute("""
                CREATE TABLE IF NOT EXISTS user_galleries (
                    user_id INTEGER PRIMARY KEY,
                    thread_id INTEGER
                )
            """)
            
            # Daily Trends table
            await db.execute("""
                CREATE TABLE IF NOT EXISTS daily_trends (
                    date_key TEXT PRIMARY KEY,
                    pose TEXT,
                    costume TEXT,
                    body TEXT
                )
            """)
            
            # Tag Metadata table (Danbooru Cache)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS tag_metadata (
                    tag_name TEXT PRIMARY KEY,
                    post_count INTEGER,
                    last_updated TIMESTAMP
                )
            """)
            
            # Tag Stock Market table
            await db.execute("""
                CREATE TABLE IF NOT EXISTS tag_stocks (
                    tag_name TEXT PRIMARY KEY,
                    current_price REAL DEFAULT 100.0,
                    total_volume INTEGER DEFAULT 0
                )
            """)

            # User Stocks table
            await db.execute("""
                CREATE TABLE IF NOT EXISTS user_stocks (
                    user_id INTEGER,
                    tag_name TEXT,
                    amount INTEGER DEFAULT 0,
                    average_cost REAL DEFAULT 0,
                    PRIMARY KEY (user_id, tag_name)
                )
            """)
            
            # Migration check
            try:
                await db.execute("ALTER TABLE market_items ADD COLUMN image_hash TEXT")
            except Exception:
                pass 
            try:
                await db.execute("ALTER TABLE market_items ADD COLUMN tags TEXT")
            except Exception:
                pass
            try:
                await db.execute("ALTER TABLE market_items ADD COLUMN grade TEXT")
            except Exception:
                pass
            try:
                await db.execute("ALTER TABLE market_items ADD COLUMN thread_id INTEGER")
            except Exception:
                pass
            try:
                await db.execute("ALTER TABLE market_items ADD COLUMN message_id INTEGER")
            except Exception:
                pass
            try:
                await db.execute("ALTER TABLE market_items ADD COLUMN buyer_id INTEGER")
            except Exception:
                pass
            
            # Auction Columns
            try:
                await db.execute("ALTER TABLE market_items ADD COLUMN auction_end_time TEXT")
            except Exception: pass
            try:
                await db.execute("ALTER TABLE market_items ADD COLUMN current_bid INTEGER DEFAULT 0")
            except Exception: pass
            try:
                await db.execute("ALTER TABLE market_items ADD COLUMN top_bidder_id INTEGER")
            except Exception: pass
            
            await db.commit()

    async def get_balance(self, user: discord.Member) -> int:
        async with aiosqlite.connect(self.db_path, timeout=60.0) as db:
            cursor = await db.execute(
                "SELECT balance FROM bank WHERE user_id = ? AND guild_id = ?",
                (user.id, user.guild.id)
            )
            row = await cursor.fetchone()
            return row[0] if row else 0

    async def set_balance(self, user: discord.Member, amount: int):
        if amount < 0:
            raise ValueError("残高は負の値にはできません。")
        async with aiosqlite.connect(self.db_path, timeout=60.0) as db:
            await db.execute(
                """
                INSERT INTO bank (user_id, guild_id, balance) 
                VALUES (?, ?, ?)
                ON CONFLICT(user_id, guild_id) DO UPDATE SET balance = ?
                """,
                (user.id, user.guild.id, amount, amount)
            )
            await db.commit()

    async def deposit_credits(self, user: discord.Member, amount: int):
        if amount <= 0:
            raise ValueError("支給額は0より大きくなければなりません。")
        async with aiosqlite.connect(self.db_path, timeout=60.0) as db:
            await db.execute(
                """
                INSERT INTO bank (user_id, guild_id, balance) 
                VALUES (?, ?, ?)
                ON CONFLICT(user_id, guild_id) DO UPDATE SET balance = balance + ?
                """,
                (user.id, user.guild.id, amount, amount)
            )
            await db.commit()

    async def withdraw_credits(self, user: discord.Member, amount: int):
        if amount <= 0:
            raise ValueError("引き落とし額は0より大きくなければなりません。")
        async with aiosqlite.connect(self.db_path, timeout=60.0) as db:
            current_bal = await self.get_balance(user)
            if current_bal < amount:
                raise ValueError("残高不足です。")
            await db.execute(
                "UPDATE bank SET balance = balance - ? WHERE user_id = ? AND guild_id = ?",
                (amount, user.id, user.guild.id)
            )
            await db.commit()

    async def transfer_credits(self, sender: discord.Member, receiver: discord.Member, amount: int):
        if amount <= 0:
            raise ValueError("送金額は0より大きくなければなりません。")
        if sender.id == receiver.id:
            raise ValueError("自分自身に送金することはできません。")

        async with aiosqlite.connect(self.db_path, timeout=60.0) as db:
            try:
                await db.execute("BEGIN TRANSACTION")
                
                cursor = await db.execute(
                    "SELECT balance FROM bank WHERE user_id = ? AND guild_id = ?",
                    (sender.id, sender.guild.id)
                )
                sender_row = await cursor.fetchone()
                sender_bal = sender_row[0] if sender_row else 0

                if sender_bal < amount:
                    raise ValueError("残高不足です。")

                await db.execute(
                    "UPDATE bank SET balance = balance - ? WHERE user_id = ? AND guild_id = ?",
                    (amount, sender.id, sender.guild.id)
                )

                await db.execute(
                    """
                    INSERT INTO bank (user_id, guild_id, balance) 
                    VALUES (?, ?, ?)
                    ON CONFLICT(user_id, guild_id) DO UPDATE SET balance = balance + ?
                    """,
                    (receiver.id, receiver.guild.id, amount, amount)
                )
                await db.commit()
            except Exception as e:
                await db.rollback()
                raise e

# -----------------------------------------------------------
# Bot クラス (Bot Class)
# -----------------------------------------------------------
class EconomyBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.members = True
        super().__init__(command_prefix="!", intents=intents)
        self.bank = BankSystem(DB_NAME)
        self.hf_token = HF_TOKEN

    async def setup_hook(self):
        await self.bank.initialize()
        
        self.initial_extensions = [
            "cogs.bank",
            "cogs.market",
            "cogs.broker",
            "cogs.stocks",
            "cogs.setup",
        ]
        for extension in self.initial_extensions:
            try:
                await self.load_extension(extension)
                print(f"ロード成功: {extension}")
            except Exception as e:
                print(f"ロード失敗 {extension}: {e}")

if __name__ == "__main__":
    bot = EconomyBot()
    
    @bot.event
    async def on_ready():
        print(f'{bot.user} 準備完了！')
        print(f'データベース: {DB_NAME}')

    bot.run(TOKEN)
