import discord
from discord.ext import commands, tasks
from gradio_client import Client, handle_file
import asyncio
import aiosqlite
import os
import aiohttp
import uuid
import traceback
import imagehash
from PIL import Image
from datetime import datetime, timedelta

class BuyView(discord.ui.View):
    def __init__(self, bot):
        super().__init__(timeout=None)
        self.bot = bot

    @discord.ui.button(label="💸 今すぐ購入", style=discord.ButtonStyle.green, custom_id="shadow_broker:buy_btn")
    async def buy_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        # 1. Identify Item by Thread ID
        thread_id = interaction.channel_id
        buyer = interaction.user
        
        async with aiosqlite.connect(self.bot.bank.db_path, timeout=60.0) as db:
            cursor = await db.execute("SELECT item_id, price, seller_id, status, image_url, tags FROM market_items WHERE thread_id = ?", (thread_id,))
            row = await cursor.fetchone()
            
            if not row:
                await interaction.response.send_message("❌ データが見つかりません。", ephemeral=True)
                return
            
            item_id, price, seller_id, status, img_url, tags_str = row
            img_url = img_url or ""
            tags_str = tags_str or ""
            
            if status != 'on_sale':
                await interaction.response.send_message("❌ 売り切れです。", ephemeral=True)
                return
            
            if buyer.id == seller_id:
                await interaction.response.send_message("❌ 自分の商品は購入できません。", ephemeral=True)
                return

            # 2. Check Balance & Process Transaction (ATOMIC)
            try:
                # Pass 'db' to withdraw_credits so it uses the SAME transaction
                await self.bot.bank.withdraw_credits(buyer, price, db_conn=db)
                
                # Update DB to SOLD
                await db.execute("UPDATE market_items SET status = 'owned', buyer_id = ?, seller_id = ?, price = 0 WHERE item_id = ?", (buyer.id, buyer.id, item_id))
                
                # Pay Seller (With Tax Logic)
                seller = interaction.guild.get_member(seller_id)
                payout_msg = ""
                
                if seller_id == self.bot.user.id:
                    # Bot Sale
                    pass
                elif seller:
                    # User Resale: 20% Tax
                    tax_rate = 0.2
                    tax_amount = int(price * tax_rate)
                    payout = int(price - tax_amount)
                    # Pass 'db' to deposit
                    await self.bot.bank.deposit_credits(seller, payout, db_conn=db)
                    payout_msg = f" (販売者へ `{payout:,}` 円送金)"
                
                await db.commit() # Commit EVERYTHING together
                
                await interaction.response.send_message(f"✅ **取引成立！**\n`{price:,}` 円支払いました。{payout_msg}", ephemeral=True)

            except ValueError:
                await interaction.response.send_message(f"❌ 残高不足です！ ({price:,} クレジット必要)", ephemeral=True)
                return
            except Exception as e:
                await interaction.response.send_message(f"❌ エラーが発生しました: {e}", ephemeral=True)
                return
            
            # --- Visual Transfer & Logging ---
            try:
                # 1. Log to shadow-logs
                log_channel = discord.utils.get(interaction.guild.text_channels, name="shadow-logs")
                if log_channel:

                    log_embed = discord.Embed(title="💸 Transaction Log", color=discord.Color.green())
                    log_embed.add_field(name="Item ID", value=f"#{item_id}", inline=True)
                    log_embed.add_field(name="Buyer", value=buyer.mention, inline=True)
                    log_embed.add_field(name="Seller", value=f"<@{seller_id}>" if seller_id else "Unknown", inline=True)
                    log_embed.add_field(name="Price", value=f"{price:,}", inline=True)
                    if img_url: log_embed.set_thumbnail(url=img_url)
                    await log_channel.send(embed=log_embed)

                # 2. Cleanup Seller Message
                # We know thread_id is interaction.channel_id
                # But message_id? Interaction.message.id!
                try:
                    await interaction.message.delete()
                except:
                    # Could not delete, maybe edit
                    await interaction.message.edit(content=f"❌ **完売 (Sold)**", view=None, embed=None)

                # 3. Post to Buyer's Gallery
                async with aiosqlite.connect(self.bot.bank.db_path, timeout=60.0) as db_gal:
                    cursor = await db_gal.execute("SELECT thread_id FROM user_galleries WHERE user_id = ?", (buyer.id,))
                    row = await cursor.fetchone()
                
                new_thread_id = 0
                new_msg_id = 0
                
                if row:
                    buyer_thread = interaction.guild.get_thread(row[0])
                    if not buyer_thread:
                         try: buyer_thread = await interaction.guild.fetch_channel(row[0])
                         except: pass
                    
                    if buyer_thread:
                         # Reconstruct Embed for Gallery
                         # Need to fetch details again or use what we have? 
                         # We have img_url from logging step
                         gallery_embed = discord.Embed(title=f"🖼️ 所持品 (ID: #{item_id})", color=discord.Color.gold())
                         if img_url: gallery_embed.set_image(url=img_url)
                         gallery_embed.add_field(name="Tags", value=tags_str, inline=False)
                         
                         new_msg = await buyer_thread.send(content=f"**獲得:** {buyer.mention}", embed=gallery_embed)
                         new_thread_id = buyer_thread.id
                         new_msg_id = new_msg.id
                    else:
                         await interaction.followup.send("⚠️ あなたのギャラリーが見つかりませんでした。`!join` で作成してください。", ephemeral=True)
                else:
                     await interaction.followup.send("⚠️ ギャラリー未登録のため、アイテムは倉庫(DB)に保管されました。`!join` してください。", ephemeral=True)
                
                # Update DB with new location
                if new_thread_id:
                     async with aiosqlite.connect(self.bot.bank.db_path, timeout=60.0) as db_upd:
                        await db_upd.execute("UPDATE market_items SET thread_id = ?, message_id = ? WHERE item_id = ?", (new_thread_id, new_msg_id, item_id))
                        await db_upd.commit()

            except Exception as e:
                print(f"Failed transfer logic: {e}")
                import traceback
                traceback.print_exc()

class MarketCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.ai_client = None

    async def cog_load(self):
        # Register Persistent View
        self.bot.add_view(BuyView(self.bot))
        # No persistent view for AuctionView needed? 
        # Actually yes, if we want buttons to work after restart.
        # But AuctionView takes item_id. 
        # Standard pattern: Use dynamic custom_id e.g. "auction:bid:item_id" OR generic callback that checks DB.
        # The Implementation above used a generic "auction_bid_btn" which looks up by Thread ID.
        # So we can register a generic instance.
        self.bot.add_view(AuctionView(self.bot, 0))
        self.auction_check_loop.start()

    def setup_client(self):
        try:
            token = getattr(self.bot, 'hf_token', None)
            if token and token != "YOUR_HUGGINGFACE_TOKEN_HERE":
                print(f"HF Token 検知: {token[:4]}****")
                self.ai_client = Client("Eugeoter/waifu-scorer-v3", token=token)
            else:
                print("HF Tokenが設定されていません。(匿名モードを試行)")
                self.ai_client = Client("Eugeoter/waifu-scorer-v3")
        except Exception as e:
            print(f"AI Client 初期化失敗: {e}")
            traceback.print_exc()
            self.ai_client = None

    def calculate_phash(self, image_path):
        """画像のPerceptual Hashを計算します。"""
        with Image.open(image_path) as img:
            return str(imagehash.phash(img))

    async def check_duplicate(self, current_hash):
        """DBから全ハッシュを取得し、ハミング距離を比較します。"""
        if not current_hash:
            return False

        async with aiosqlite.connect(self.bot.bank.db_path) as db:
            cursor = await db.execute("SELECT image_hash FROM market_items WHERE image_hash IS NOT NULL")
            rows = await cursor.fetchall()
        
        current_hash_obj = imagehash.hex_to_hash(current_hash)
        
        for (db_hash_str,) in rows:
            try:
                db_hash_obj = imagehash.hex_to_hash(db_hash_str)
                distance = current_hash_obj - db_hash_obj
                if distance <= 5: # 閾値 5
                    return True
            except:
                continue
        return False


    @commands.command(name="market", aliases=["gallery", "shop"])
    async def market(self, ctx):
        """現在販売中の美術品リストを見ます。"""
        async with aiosqlite.connect(self.bot.bank.db_path) as db:
            cursor = await db.execute(
                "SELECT item_id, price, aesthetic_score, image_url FROM market_items WHERE status = 'on_sale' ORDER BY item_id DESC LIMIT 10"
            )
            items = await cursor.fetchall()
            
        if not items:
            await ctx.send("🏪 現在販売中の作品がありません。先に絵を鑑定してもらって売ってみましょう！")
            return

        embed = discord.Embed(title="🏰 AIアートギャラリー (Market)", color=discord.Color.purple())
        for item_id, price, score, url in items:
            embed.add_field(
                name=f"🖼️ No.{item_id} (スコア: {score:.2f})",
                value=f"価格: `{price:,} 円`\n[画像を見る]({url})",
                inline=False
            )
        embed.set_footer(text="購入するには '!購入 [番号]' を入力してください。")
        await ctx.send(embed=embed)

    @commands.command(name="buy")
    async def buy(self, ctx, item_id: int):
        """ギャラリーにある絵を購入します。"""
        async with aiosqlite.connect(self.bot.bank.db_path) as db:
            cursor = await db.execute(
                "SELECT price, image_url, status FROM market_items WHERE item_id = ?",
                (item_id,)
            )
            row = await cursor.fetchone()
            
            if not row:
                await ctx.send("❌ その番号の作品は見つかりませんでした。")
                return
            
            price, image_url, status = row
            
            if status != 'on_sale':
                await ctx.send("❌ すでに販売された作品です。")
                return
            
            # Check balance
            buyer_balance = await self.bot.bank.get_balance(ctx.author)
            if buyer_balance < price:
                await ctx.send(f"❌ 残高が不足しています。(必要: {price:,} 円, 保有: {buyer_balance:,} 円)")
                return
            
            # Process Transaction
            try:
                await self.bot.bank.withdraw_credits(ctx.author, price)
                
                await db.execute(
                    "UPDATE market_items SET status = 'sold', buyer_id = ? WHERE item_id = ?",
                    (ctx.author.id, item_id,)
                )
                await db.commit()

                # --- Stock Market Influence (Demand) ---
                # Buying increases stock price by +1.0%
                if tags_str:
                    stocks_cog = self.bot.get_cog("StocksCog")
                    if stocks_cog:
                         tag_list = tags_str.split(",")
                         for tag in tag_list:
                             t_clean = tag.strip()
                             if t_clean:
                                 self.bot.loop.create_task(stocks_cog.update_stock_price(t_clean, 1.01))
                
                embed = discord.Embed(title="🎉 購入成功！", description=f"素晴らしい作品を所持することになりました。\n`{price:,} 円`を支払いました。", color=discord.Color.green())
                embed.set_image(url=image_url)
                await ctx.send(embed=embed)
                
            except ValueError as e:
                 await ctx.send(f"❌ 取引失敗: {e}")

    async def cog_unload(self):
        self.auction_check_loop.cancel()

    @tasks.loop(minutes=1.0)
    async def auction_check_loop(self):
        """Checks for expired auctions every minute."""
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        async with aiosqlite.connect(self.bot.bank.db_path, timeout=60.0) as db:
            # Select expired auctions that are still 'on_auction'
            cursor = await db.execute("""
                SELECT item_id, image_url, current_bid, top_bidder_id, seller_id, thread_id, message_id
                FROM market_items 
                WHERE status = 'on_auction' AND auction_end_time <= ?
            """, (now_str,))
            expired_items = await cursor.fetchall()
            
            notifications = []
            
            for item in expired_items:
                item_id, img_url, bid, bidder_id, seller_id, thread_id, msg_id = item
                status_msg = ""
                final_owner_id = None
                
                # If no bids, return to owner
                if not bidder_id or bid == 0:
                    await db.execute("UPDATE market_items SET status = 'owned', auction_end_time = NULL WHERE item_id = ?", (item_id,))
                    status_msg = "🚫 **流札 (Unsold)**: 入札者がいませんでした。所有権は出品者に戻ります。"
                    final_owner_id = seller_id
                else:
                    # Winner!
                    # 1. Pay Seller (Auction Tax 10%)
                    tax = int(bid * 0.1) 
                    payout = int(bid - tax)
                    seller = self.bot.get_user(seller_id) 
                    
                    if seller:
                        await self.bot.bank.deposit_credits(seller, payout, db_conn=db)
                    else:
                        # Fallback deposit via DB (Atomic Upsert)
                         await db.execute("""
                            INSERT INTO bank (user_id, guild_id, balance) VALUES (?, ?, ?)
                            ON CONFLICT(user_id, guild_id) DO UPDATE SET balance = balance + ?
                         """, (seller_id, 0, payout, payout))

                    # 2. Transfer Item
                    await db.execute("""
                        UPDATE market_items 
                        SET status = 'owned', buyer_id = ?, seller_id = ?, price = 0, auction_end_time = NULL
                        WHERE item_id = ?
                    """, (bidder_id, bidder_id, item_id))
                    
                    status_msg = f"🔨 **落札 (SOLD)!**\n落札者: <@{bidder_id}>\n落札額: `{bid:,}` Credits"
                    final_owner_id = bidder_id
                
                # Store Notification Data
                notifications.append({
                    'thread_id': thread_id,
                    'msg_id': msg_id,
                    'item_id': item_id,
                    'status_msg': status_msg,
                    'img_url': img_url,
                    'final_owner_id': final_owner_id
                })
            
            await db.commit()
            
        # Send Notifications (Outside DB Transaction to prevent locking)
        for n in notifications:
            if n['thread_id']:
                 channel = self.bot.get_channel(n['thread_id'])
                 if channel:
                     try:
                         # Update Original Message
                         if n['msg_id']:
                             try:
                                msg = await channel.fetch_message(n['msg_id'])
                                await msg.edit(content=f"🏁 **オークション終了**: (ID: #{n['item_id']})", view=None)
                             except: pass
                         
                         embed = discord.Embed(title="🏁 オークション結果", description=n['status_msg'], color=discord.Color.gold())
                         if n['img_url']: embed.set_image(url=n['img_url'])
                         await channel.send(content=f"<@{n['final_owner_id']}>", embed=embed)
                     except: pass

    @commands.command(name="auction")
    async def auction(self, ctx, item_id: int, start_price: int, duration_minutes: int):
        """所持品をオークションに出品します。 Usage: !auction [ID] [開始価格] [時間(分)]"""
        if duration_minutes < 1 or duration_minutes > 1440:
             await ctx.send("❌ 時間は 1分 〜 1440分(24時間) の間で指定してください。")
             return
        if start_price < 100:
             await ctx.send("❌ 開始価格は 100 Credits 以上で設定してください。")
             return

        async with aiosqlite.connect(self.bot.bank.db_path) as db:
            # Check ownership
            cursor = await db.execute("""
                SELECT tags, aesthetic_score, image_url, image_hash 
                FROM market_items 
                WHERE item_id = ? AND buyer_id = ? AND status IN ('owned', 'on_sale')
            """, (item_id, ctx.author.id))
            row = await cursor.fetchone()
            
            if not row:
                await ctx.send("❌ そのアイテムを所有していないか、すでに出品中です。")
                return
            
            # Start Auction
            end_time = datetime.now() + timedelta(minutes=duration_minutes)
            end_time_str = end_time.strftime("%Y-%m-%d %H:%M:%S")
            
            tags, score, img_url, img_hash = row
            
            # Create Thread/Post
            forum = discord.utils.get(ctx.guild.forums, name="闇市ギャラリー")
            if not forum:
                await ctx.send("❌ 闇市ギャラリーが見つかりません。")
                return

            embed = discord.Embed(title=f"🔨 オークション開催 (ID: #{item_id})", color=discord.Color.red())
            embed.set_image(url=img_url)
            embed.add_field(name="出品者", value=ctx.author.mention, inline=True)
            embed.add_field(name="開始価格", value=f"💰 {start_price:,}", inline=True)
            embed.add_field(name="終了時刻", value=f"<t:{int(end_time.timestamp())}:R>", inline=True)
            embed.add_field(name="スコア", value=f"{score:.2f}", inline=True)
            embed.add_field(name="Tags", value=tags[:100], inline=False)
            
            view = AuctionView(self.bot, item_id)
            
            thread_with_message = await forum.create_thread(
                name=f"[Auction] ID:{item_id} | Price: {start_price}",
                content=f"🔨 **オークション開始!** (ID: #{item_id})",
                embed=embed,
                view=view
            )
            thread = thread_with_message.thread if hasattr(thread_with_message, 'thread') else thread_with_message
            msg = thread_with_message.message 
            if not msg and hasattr(thread, 'starter_message'): msg = thread.starter_message

            # Update DB
            await db.execute("""
                UPDATE market_items 
                SET status = 'on_auction', 
                    price = ?, 
                    current_bid = ?, 
                    auction_end_time = ?, 
                    thread_id = ?, 
                    message_id = ?,
                    top_bidder_id = NULL
                WHERE item_id = ?
            """, (start_price, start_price, end_time_str, thread.id, msg.id if msg else 0, item_id))
            await db.commit()
            
            await ctx.send(f"✅ **オークションを開始しました！**\n会場: {thread.mention}")

class AuctionView(discord.ui.View):
    def __init__(self, bot, item_id):
        super().__init__(timeout=None)
        self.bot = bot
        self.item_id = item_id

    @discord.ui.button(label="✋ 入札する", style=discord.ButtonStyle.primary, custom_id="auction_bid_btn")
    async def bid_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        # We need to find the item_id from context if generic, but here we passed it.
        # Wait, Custom ID persistent views need dynamic handling if we don't store state.
        # For persistent views, we usually encode ID in custom_id or look up by channel.
        # Let's Look up by Channel (Thread) ID as per `BuyView` logic, safer for persistence.
        
        thread_id = interaction.channel_id
        async with aiosqlite.connect(self.bot.bank.db_path) as db:
            cursor = await db.execute("SELECT item_id, current_bid, top_bidder_id, auction_end_time, seller_id FROM market_items WHERE thread_id = ? AND status = 'on_auction'", (thread_id,))
            row = await cursor.fetchone()
            
            if not row:
                 await interaction.response.send_message("❌ オークションが見つかりません(終了している可能性があります)。", ephemeral=True)
                 return

            item_id_db, current_bid, top_bidder, end_time_str, seller_id = row
            
            if interaction.user.id == seller_id:
                 await interaction.response.send_message("❌ 自分の商品には入札できません。", ephemeral=True)
                 return

            if interaction.user.id == top_bidder:
                 await interaction.response.send_message("⚠️ あなたは現在の最高入札者です。", ephemeral=True)
                 return

            # Ask for Bid Amount via Modal
            await interaction.response.send_modal(BidModal(self.bot, item_id_db, current_bid))

class BidModal(discord.ui.Modal, title="入札金額を入力"):
    def __init__(self, bot, item_id, current_bid):
        super().__init__()
        self.bot = bot
        self.item_id = item_id
        self.current_bid = current_bid
        
        self.bid_input = discord.ui.TextInput(
            label=f"現在の価格: {current_bid:,}",
            placeholder=f"{int(current_bid * 1.1)} 以上の金額を入力",
            min_length=1,
            max_length=10,
        )
        self.add_item(self.bid_input)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            bid_amount = int(self.bid_input.value)
        except ValueError:
            await interaction.response.send_message("❌ 数字を入力してください。", ephemeral=True)
            return
            
        min_bid = int(self.current_bid * 1.1)
        if min_bid < self.current_bid + 100: min_bid = self.current_bid + 100
        
        if bid_amount < min_bid:
             await interaction.response.send_message(f"❌ 入札額が低すぎます。(最低: {min_bid:,})", ephemeral=True)
             return

        # Check Balance
        buyer = interaction.user
        
        extended = False
        async with aiosqlite.connect(self.bot.bank.db_path, timeout=60.0) as db:
            # 1. Check Previous Bidder (Read first to prepare refund)
            cursor = await db.execute("SELECT top_bidder_id, current_bid, auction_end_time FROM market_items WHERE item_id = ?", (self.item_id,))
            row = await cursor.fetchone()
            
            # 2. Withdraw from New Bidder (Atomic)
            try:
                await self.bot.bank.withdraw_credits(buyer, bid_amount, db_conn=db)
            except ValueError:
                await interaction.response.send_message(f"❌ 残高不足です！ ({bid_amount:,} 必要)", ephemeral=True)
                return
            except Exception as e:
                await interaction.response.send_message(f"❌ エラー: {e}", ephemeral=True)
                return

            # 3. Refund Previous Bidder (Atomic)
            if row:
                prev_bidder_id, prev_bid_val, end_time_str = row
                if prev_bidder_id and prev_bid_val > 0:
                     prev_bidder = interaction.guild.get_member(prev_bidder_id)
                     if prev_bidder:
                         await self.bot.bank.deposit_credits(prev_bidder, prev_bid_val, db_conn=db)
                         try: await prev_bidder.send(f"↩️ **返金通知:** あなたの入札が更新されました (+{prev_bid_val:,} Credits)")
                         except: pass
                     else:
                         # Manual Deposit if user left (Using same DB conn)
                         await db.execute("INSERT OR IGNORE INTO bank (user_id, guild_id, balance) VALUES (?, ?, 0)", (prev_bidder_id, interaction.guild.id))
                         await db.execute("UPDATE bank SET balance = balance + ? WHERE user_id = ? AND guild_id = ?", (prev_bid_val, prev_bidder_id, interaction.guild.id))

                # Update Auction State
                end_time = datetime.strptime(end_time_str, "%Y-%m-%d %H:%M:%S")
                now = datetime.now()
                new_end_time = end_time
                
                if (end_time - now).total_seconds() < 120:
                     new_end_time = now + timedelta(minutes=2)
                     extended = True
                
                new_end_str = new_end_time.strftime("%Y-%m-%d %H:%M:%S")
                
                await db.execute("""
                    UPDATE market_items 
                    SET current_bid = ?, top_bidder_id = ?, auction_end_time = ?
                    WHERE item_id = ?
                """, (bid_amount, buyer.id, new_end_str, self.item_id))
            
            # 4. Commit All
            await db.commit()
                
        msg = f"✅ **入札成功！**\n現在の最高額: `{bid_amount:,}` Credits"
        if extended: msg += "\n⏳ 終了時間が2分延長されました！"
        await interaction.response.send_message(msg)
        
        # Update Thread Title/Embed (Optional polish)
        try:
                thread = interaction.channel
                await thread.edit(name=f"[Auction] ID:{self.item_id} | Price: {bid_amount:,}")
                await thread.send(f"⚡ **新規入札:** {buyer.mention} が `{bid_amount:,}` Credits で入札しました！")
        except: pass

async def setup(bot):
    await bot.add_cog(MarketCog(bot))

