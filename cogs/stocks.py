import discord
from discord.ext import commands, tasks
import aiosqlite
import math
import random

class StockView(discord.ui.View):
    def __init__(self, bot, tag_name):
        super().__init__(timeout=60)
        self.bot = bot
        self.tag_name = tag_name

    @discord.ui.button(label="📈 買う (Buy)", style=discord.ButtonStyle.green)
    async def buy_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(StockOrderModal(self.bot, self.tag_name, "buy"))

    @discord.ui.button(label="📉 売る (Sell)", style=discord.ButtonStyle.red)
    async def sell_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(StockOrderModal(self.bot, self.tag_name, "sell"))

class StockOrderModal(discord.ui.Modal, title="注文入力"):
    def __init__(self, bot, tag_name, order_type):
        super().__init__()
        self.bot = bot
        self.tag_name = tag_name
        self.order_type = order_type
        
        self.amount_input = discord.ui.TextInput(
            label="数量 (株)",
            placeholder="1以上の整数",
            min_length=1,
            max_length=5
        )
        self.add_item(self.amount_input)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            amount = int(self.amount_input.value)
            if amount <= 0: raise ValueError
        except:
             await interaction.response.send_message(f"❌ 正しい数量を入力してください。", ephemeral=True)
             return

        # Delegate to Cog
        cog = self.bot.get_cog("StocksCog")
        if cog:
            if self.order_type == "buy":
                await cog.process_buy(interaction, self.tag_name, amount)
            else:
                await cog.process_sell(interaction, self.tag_name, amount)
        else:
            await interaction.response.send_message("❌ エラー: StocksCogが見つかりません。", ephemeral=True)

class StocksCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.volatility_loop.start()

    def cog_unload(self):
        self.volatility_loop.cancel()

    @tasks.loop(hours=1.0)
    async def volatility_loop(self):
        """Applies random market volatility every hour (-5% to +5%)."""
        async with aiosqlite.connect(self.bot.bank.db_path) as db:
            cursor = await db.execute("SELECT tag_name, current_price FROM tag_stocks")
            rows = await cursor.fetchall()
            
            for tag, price in rows:
                # Random fluctuations: -5% to +5%
                change_rate = random.uniform(-0.05, 0.05)
                multiplier = 1.0 + change_rate
                
                # Apply update
                new_price = max(1.0, price * multiplier)
                
                await db.execute("UPDATE tag_stocks SET current_price = ? WHERE tag_name = ?", (new_price, tag))
                
            await db.commit()
        # print("📉 Market Volatility Applied.")

    async def get_stock_price(self, tag_name):
        async with aiosqlite.connect(self.bot.bank.db_path) as db:
             cursor = await db.execute("SELECT current_price FROM tag_stocks WHERE tag_name = ?", (tag_name,))
             row = await cursor.fetchone()
             if row: return row[0]
             
             # If not exists, init it
             await db.execute("INSERT OR IGNORE INTO tag_stocks (tag_name) VALUES (?)", (tag_name,))
             await db.commit()
             return 100.0

    async def update_stock_price(self, tag_name, multiplier):
        """Called by other Cogs to influence price. 
        Multiplier example: 1.05 for +5%, 0.99 for -1%."""
        async with aiosqlite.connect(self.bot.bank.db_path) as db:
            await db.execute("""
                INSERT INTO tag_stocks (tag_name, current_price) VALUES (?, 100)
                ON CONFLICT(tag_name) DO UPDATE SET current_price = max(1.0, current_price * ?)
            """, (tag_name, multiplier))
            await db.commit()

    async def process_buy(self, interaction, tag, amount):
        async with aiosqlite.connect(self.bot.bank.db_path) as db:
            current_price = await self.get_stock_price(tag)
            cost = int(current_price * amount)
            
            try:
                await self.bot.bank.withdraw_credits(interaction.user, cost)
            except ValueError:
                 await interaction.response.send_message(f"❌ 資金不足: {cost:,} Cr 必要", ephemeral=True)
                 return

            # Update Portfolio
            # Select first to calc average
            cursor = await db.execute("SELECT amount, average_cost FROM user_stocks WHERE user_id = ? AND tag_name = ?", (interaction.user.id, tag))
            row = await cursor.fetchone()
            
            if row:
                old_amt, old_avg = row
                new_amt = old_amt + amount
                new_avg = ((old_avg * old_amt) + (current_price * amount)) / new_amt
                await db.execute("UPDATE user_stocks SET amount = ?, average_cost = ? WHERE user_id = ? AND tag_name = ?", (new_amt, new_avg, interaction.user.id, tag))
            else:
                await db.execute("INSERT INTO user_stocks (user_id, tag_name, amount, average_cost) VALUES (?, ?, ?, ?)", (interaction.user.id, tag, amount, current_price))
            
            await db.commit()
            
            # Influence Price (Buying raises price slightly: +0.01% per share?)
            # Limit impact to avoid exploits
            impact = 1.0 + (min(amount, 100) * 0.0001) 
            await self.update_stock_price(tag, impact)
            
            await interaction.response.send_message(f"📈 **購入完了:** `{tag}` x{amount}株 (取得単価: {current_price:.1f})")

    async def process_sell(self, interaction, tag, amount):
        async with aiosqlite.connect(self.bot.bank.db_path) as db:
            cursor = await db.execute("SELECT amount, average_cost FROM user_stocks WHERE user_id = ? AND tag_name = ?", (interaction.user.id, tag))
            row = await cursor.fetchone()
            
            if not row or row[0] < amount:
                 await interaction.response.send_message(f"❌ 保有株式が不足しています。", ephemeral=True)
                 return
            
            current_price = await self.get_stock_price(tag)
            payout = int(current_price * amount)
            profit = payout - (row[1] * amount)
            
            new_amt = row[0] - amount
            if new_amt == 0:
                await db.execute("DELETE FROM user_stocks WHERE user_id = ? AND tag_name = ?", (interaction.user.id, tag))
            else:
                await db.execute("UPDATE user_stocks SET amount = ? WHERE user_id = ? AND tag_name = ?", (new_amt, interaction.user.id, tag))
            
            await self.bot.bank.deposit_credits(interaction.user, payout)
            await db.commit()
            
            # Selling lowers price
            impact = 1.0 - (min(amount, 100) * 0.0001)
            await self.update_stock_price(tag, impact)
            
            profit_str = f"利益: +{int(profit):,}" if profit >= 0 else f"損失: {int(profit):,}"
            await interaction.response.send_message(f"📉 **売却完了:** `{tag}` x{amount}株 ({profit_str}) -> `{payout:,} Cr` 受取")

    @commands.command(name="stock", aliases=["kabuka"])
    async def stock(self, ctx, tag_name: str):
        """特定のタグの株価情報を確認します。"""
        price = await self.get_stock_price(tag_name)
        async with aiosqlite.connect(self.bot.bank.db_path) as db:
            cursor = await db.execute("SELECT amount, average_cost FROM user_stocks WHERE user_id = ? AND tag_name = ?", (ctx.author.id, tag_name))
            row = await cursor.fetchone()
            
        embed = discord.Embed(title=f"📊 株価情報: {tag_name}", color=discord.Color.blue())
        embed.add_field(name="現在値", value=f"**{price:.2f} Cr**", inline=False)
        
        if row:
            amount, avg_cost = row
            val = price * amount
            pl = val - (avg_cost * amount)
            sign = "+" if pl >= 0 else ""
            embed.add_field(name="保有状況", value=f"保有数: `{amount}株`\n取得単価: `{avg_cost:.1f}`\n評価損益: `{sign}{int(pl):,}`", inline=False)
        else:
             embed.add_field(name="保有状況", value="なし", inline=False)
             
        view = StockView(self.bot, tag_name)
        await ctx.send(embed=embed, view=view)

    @commands.command(name="portfolio")
    async def portfolio(self, ctx):
        """保有株式一覧を表示します。"""
        async with aiosqlite.connect(self.bot.bank.db_path) as db:
            cursor = await db.execute("SELECT tag_name, amount, average_cost FROM user_stocks WHERE user_id = ? ORDER BY amount DESC", (ctx.author.id,))
            rows = await cursor.fetchall()
            
        if not rows:
            await ctx.send("💼 **ポートフォリオ:** 株式を保有していません。")
            return
            
        embed = discord.Embed(title=f"💼 {ctx.author.display_name}のポートフォリオ", color=discord.Color.blue())
        desc = ""
        total_val = 0
        total_pl = 0
        
        for tag, amt, cost in rows:
            # Note: Fetching current price for EACH tag in loop is N+1 query.
            # But simple enough for now. optimized would be "SELECT ... FROM tag_stocks WHERE tag_name IN (...)"
            curr = await self.get_stock_price(tag) 
            val = curr * amt
            pl = val - (cost * amt)
            
            total_val += val
            total_pl += pl
            
            sign = "+" if pl >= 0 else ""
            desc += f"**{tag}**: {amt}株 (現在: {curr:.1f} / 取得: {cost:.1f}) -> `{sign}{int(pl):,}`\n"
            
        embed.description = desc
        sign_total = "+" if total_pl >= 0 else ""
        embed.set_footer(text=f"総評価額: {int(total_val):,} Cr (損益: {sign_total}{int(total_pl):,})")
        await ctx.send(embed=embed)

async def setup(bot):
    await bot.add_cog(StocksCog(bot))
