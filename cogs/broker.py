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
import random
import csv
import json
import math
from datetime import datetime, time, timedelta
from utils.bloom_filter import BloomFilter

class BrokerCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.ai_client_score = None
        self.ai_client_tag = None
        self.setup_clients()
        self.tag_data = {} # category: [tags]
        self.load_tag_data()
        
        # AI Queue System
        self.ai_queue = asyncio.Queue()
        self.ai_worker_task = self.bot.loop.create_task(self.ai_worker())
        
        # Initialize Bloom Filter (Capacity 10000, 0.1% error)
        self.bloom = BloomFilter(capacity=10000, error_rate=0.001)
        self.bot.loop.create_task(self.initialize_bloom_filter())
        
        self.daily_task_loop.start()

    def cog_unload(self):
        self.daily_task_loop.cancel()
        self.ai_worker_task.cancel()
        # Save Bloom Filter on unload
        self.bloom.save_to_file("bloom_filter.bin")

    def setup_clients(self):
        try:
            token = getattr(self.bot, 'hf_token', None)
            self.ai_client_score = Client("Eugeoter/waifu-scorer-v3", token=token)
            self.ai_client_tag = Client("SmilingWolf/wd-tagger", token=token)
            print("Broker AI Clients Loaded.")
        except Exception as e:
            print(f"Broker AI Clients Error: {e}")
            self.ai_client_score = None
            self.ai_client_tag = None

    def load_tag_data(self):
        try:
            with open('tags.json', 'r', encoding='utf-8') as f:
                self.tag_data = json.load(f)
            print(f"Loaded Tags: {len(self.tag_data)} categories.")
        except Exception as e:
            print(f"Failed to load tag data: {e}")
            self.tag_data = {}

    @tasks.loop(hours=24)
    async def daily_task_loop(self):
        """Runs daily to update trends (approximated)"""
        await self.update_daily_trends()
        await self.decay_saturation()

    async def initialize_bloom_filter(self):
        """Loads valid image hashes. Tries file first, then DB."""
        await self.bot.wait_until_ready()
        print("Initializing Bloom Filter...")
        
        # Try loading from file
        loaded_bloom = BloomFilter.load_from_file("bloom_filter.bin")
        if loaded_bloom:
            self.bloom = loaded_bloom
            print(f"Bloom Filter loaded from file. Size eq: {len(self.bloom)}")
            # Optional: We could load *new* items from DB here if we tracked last_id.
            # For now, we assume the file is reasonably fresh or we just accept the gap until next save.
            return

        count = 0
        async with aiosqlite.connect(self.bot.bank.db_path) as db:
            # 1. Load URLs (to prevent re-downloading known links)
            cursor = await db.execute("SELECT image_url, image_hash FROM market_items")
            rows = await cursor.fetchall()
            
            for url, img_hash in rows:
                if url: self.bloom.add(url)
                if img_hash: self.bloom.add(img_hash)
                count += 1
                
        print(f"Bloom Filter Rebuilt with {count} items.")
        self.bloom.save_to_file("bloom_filter.bin")

    async def ai_worker(self):
        """Worker to process AI requests sequentially."""
        print("AI Worker Started.")
        while True:
            try:
                # task_type: 'tag' or 'score'
                # future: asyncio.Future to set result
                task_type, file_path, future = await self.ai_queue.get()
                
                try:
                    res = None
                    if task_type == 'tag':
                        res = await asyncio.to_thread(self._run_predict_sync, self.ai_client_tag, file_path)
                    elif task_type == 'score':
                        res = await asyncio.to_thread(self._run_predict_sync, self.ai_client_score, file_path)
                    
                    if not future.done():
                        future.set_result(res)
                except Exception as e:
                    if not future.done():
                        future.set_exception(e)
                finally:
                    self.ai_queue.task_done()
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"AI Worker Error: {e}")

    @daily_task_loop.before_loop
    async def before_daily_task(self):
        await self.bot.wait_until_ready()
        # Sleep until 6 AM
        now = datetime.now()
        target = now.replace(hour=6, minute=0, second=0, microsecond=0)
        if now >= target:
            target += timedelta(days=1)
        # For testing, we might want to run immediately if DB is empty, but let's just log.
        print(f"Next Daily Trend Update: {target}")
        await asyncio.sleep((target - now).total_seconds())

    async def update_daily_trends(self):
        if not self.tag_data: return
        
        # Pick 1 from each category
        today_trends = {}
        for category, tags in self.tag_data.items():
            if tags:
                today_trends[category] = random.choice(tags)
        
        date_key = datetime.now().strftime("%Y-%m-%d")
        
        async with aiosqlite.connect(self.bot.bank.db_path) as db:
            # Clear old trends or just overwrite for the day
            # We store by date_key just in case
            await db.execute("""
                INSERT OR REPLACE INTO daily_trends (date_key, pose, costume, body)
                VALUES (?, ?, ?, ?)
            """, (date_key, today_trends.get('pose'), today_trends.get('costume'), today_trends.get('body')))
            await db.commit()
        
        print(f"Updated Daily Trends for {date_key}: {today_trends}")
        
        # Notify "トレンド" channel in all guilds
        embed = discord.Embed(title=f"📅 本日のトレンド ({date_key})", color=discord.Color.gold())
        embed.description = "市場調査の結果、以下の属性が高騰しています！\nこれらの要素を含む画像を密輸するとボーナスがつきます。"
        embed.add_field(name="🤸 ポーズ", value=f"`{today_trends.get('pose')}`", inline=True)
        embed.add_field(name="👗 衣装", value=f"`{today_trends.get('costume')}`", inline=True)
        embed.add_field(name="👀 特徴", value=f"`{today_trends.get('body')}`", inline=True)
        embed.set_footer(text="毎日AM6:00更新 | 闇市運営委員会")

        for guild in self.bot.guilds:
            channel = discord.utils.get(guild.text_channels, name="トレンド")
            if channel:
                try:
                    await channel.send(embed=embed)
                except Exception as e:
                    print(f"Failed to send trend update to guild {guild.name}: {e}")

    async def get_current_trends(self):
        date_key = datetime.now().strftime("%Y-%m-%d")
        async with aiosqlite.connect(self.bot.bank.db_path) as db:
            cursor = await db.execute("SELECT pose, costume, body FROM daily_trends WHERE date_key = ?", (date_key,))
            row = await cursor.fetchone()
            if row:
                return {'pose': row[0], 'costume': row[1], 'body': row[2]}
            else:
                # Force update if missing
                await self.update_daily_trends()
                return await self.get_current_trends()

    def _run_predict_sync(self, client, file_path):
        """Run prediction in a separate thread"""
        print(f"DEBUG: Thread Running for {file_path}")
        try:
             # Try passing path directly first?
             # Some Gradio apps accept path strings.
             # If this fails, we catch it.
             return client.predict(handle_file(file_path), api_name="/predict")
        except Exception as e:
             print(f"DEBUG: Prediction Thread Error: {e}")
             raise e

    def calculate_phash(self, image_path):
        with Image.open(image_path) as img:
            return str(imagehash.phash(img))

    async def get_risk_factor(self, current_hash):
        if not current_hash:
            return 10, "Unknown Error", 0
        
        async with aiosqlite.connect(self.bot.bank.db_path) as db:
            cursor = await db.execute("SELECT image_hash FROM market_items WHERE image_hash IS NOT NULL")
            rows = await cursor.fetchall()

        current_hash_obj = imagehash.hex_to_hash(current_hash)
        min_dist = 100
        
        for (db_hash_str,) in rows:
            try:
                db_hash_obj = imagehash.hex_to_hash(db_hash_str)
                dist = current_hash_obj - db_hash_obj
                if dist < min_dist:
                    min_dist = dist
            except:
                continue

        if min_dist <= 5:
            return 100, f"⛔ **重複警告** (類似度: {min_dist})", min_dist
        else:
            return 0, f"✅ **確認完了** (新規アイテム)", min_dist

    async def update_market_trends(self, tags):
        """Update saturation for tags on new upload."""
        async with aiosqlite.connect(self.bot.bank.db_path) as db:
            for tag in tags:
                await db.execute("INSERT OR IGNORE INTO market_trends (tag_name) VALUES (?)", (tag,))
                await db.execute("""
                    UPDATE market_trends 
                    SET saturation = saturation + 1
                    WHERE tag_name = ?
                """, (tag,))
            await db.commit()

    async def decay_saturation(self):
        """Called daily to reduce saturation."""
        async with aiosqlite.connect(self.bot.bank.db_path) as db:
            # Decay by 10% or at least 1
            await db.execute("""
                UPDATE market_trends 
                SET saturation = CAST(saturation * 0.9 AS INTEGER) 
                WHERE saturation > 0
            """)
            await db.commit()
        print("Daily Saturation Decay Applied.")



    async def get_tag_value_modifier(self, tags):
        # Logarithmic Saturation Decay
        # Multiplier = 1 / log10(saturation + 2)
        # Base saturation starts at 0.
        # If saturation is 100 -> log10(102) ~ 2.0 -> Mult ~ 0.5
        # If saturation is 500 -> log10(502) ~ 2.7 -> Mult ~ 0.37
        
        multiplier = 1.0
        async with aiosqlite.connect(self.bot.bank.db_path) as db:
            for tag in tags:
                cursor = await db.execute("SELECT current_price, saturation FROM market_trends WHERE tag_name = ?", (tag,))
                row = await cursor.fetchone()
                
                if row:
                    price, sat = row
                    # Apply saturation penalty
                    # Use a weighted average or minimum multiplier?
                    # Let's use the WORST modifier (the most saturated tag pulls down the whole value)
                    # Or average? Average feels fairer.
                    
                    sat_mult = 1.0 / math.log10(max(sat, 0) + 2)
                    
                    # Accumulate? Let's average the multipliers of known tags
                    # But we need to handle "no record" tags as 1.0
                    # This logic is complex. Simplified:
                    # Modify the aggregate multiplier by the impact of this tag.
                    # Let's take the Minimum modifier found.
                    if sat_mult < multiplier:
                         multiplier = sat_mult

        return max(multiplier, 0.1)

    @commands.command(name="trends")
    async def trends(self, ctx):
        """(Beta) 今日の流行トレンドを表示します。"""
        trends = await self.get_current_trends()
        if not trends:
            await ctx.send("📅 今日のトレンドはまだ発表されていません。")
            return
            
        embed = discord.Embed(title="📈 本日の闇市トレンド (Daily Trends)", description="以下の要素を含む品は高値で取引されます。", color=discord.Color.magenta())
        embed.add_field(name="💃 姿勢 (Pose)", value=f"`{trends.get('pose', 'None')}`", inline=True)
        embed.add_field(name="👗 衣装 (Costume)", value=f"`{trends.get('costume', 'None')}`", inline=True)
        embed.add_field(name="👀 特徴 (Body)", value=f"`{trends.get('body', 'None')}`", inline=True)
        embed.set_footer(text="毎日 朝6:00 更新")
        await ctx.send(embed=embed)

    async def _download_and_hash(self, url):
        """Downloads image from URL and calculates pHash."""
        temp_path = f"temp_{uuid.uuid4()}.png"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    if resp.status != 200: return None, None
                    data = await resp.read()
            with open(temp_path, "wb") as f: f.write(data)
            
            img_hash = await asyncio.to_thread(self.calculate_phash, temp_path)
            return temp_path, img_hash
        except Exception as e:
            print(f"Download Error: {e}")
            if os.path.exists(temp_path): os.remove(temp_path)
            return None, None

    async def _run_tagger(self, file_path):
        """Runs the tagger AI via Queue. Returns (tag_list, tags_str, character_list)."""
        if not self.ai_client_tag: return [], "", []
        
        future = self.bot.loop.create_future()
        await self.ai_queue.put(('tag', file_path, future))
        
        try:
            # Enforce 20s timeout
            res = await asyncio.wait_for(future, timeout=30.0) # Slightly longer to account for queue wait

            # Debug output for verification
            # print(f"DEBUG: Tagger Raw Output Type: {type(res)}")
            
            # Helper to parse Gradio Label output
            def parse_gradio_label(data):
                if isinstance(data, dict) and 'confidences' in data:
                    return {item['label']: item['confidence'] for item in data['confidences']}
                return data if isinstance(data, dict) else {}

            # Initialize containers
            confidences = {}
            character_confidences = {}
            
            # Handle Tuple Output (New Tagger Model: [comb_tags_str, rating_dict, char_dict, gen_dict])
            if isinstance(res, (list, tuple)) and len(res) >= 3:
                # index 2 is character tags
                # index 3 is general tags
                
                if len(res) > 3:
                    confidences = parse_gradio_label(res[3])
                elif len(res) > 0 and isinstance(res[0], dict):
                     # Fallback if structure is different
                    confidences = parse_gradio_label(res[0])

                if isinstance(res[2], dict) or (isinstance(res[2], dict) and 'confidences' in res[2]):
                    character_confidences = parse_gradio_label(res[2])

            elif isinstance(res, dict):
                confidences = parse_gradio_label(res)

            # Fallback for file path output
            if isinstance(res, str) and os.path.exists(res):
                 # This path is legacy/fallback, unlikely to happen with this model
                 pass

            tag_list = []
            character_list = []

            # Process General Tags
            if confidences:
                # Ensure values are floats
                clean_confidences = {}
                for k, v in confidences.items():
                    try:
                        clean_confidences[k] = float(v)
                    except:
                        continue
                        
                sorted_tags = sorted(clean_confidences.items(), key=lambda x: x[1], reverse=True)
                tag_list = [t[0] for t in sorted_tags if t[1] > 0.35][:20]

            # Process Character Tags
            if character_confidences:
                clean_chars = {}
                for k, v in character_confidences.items():
                     try:
                        clean_chars[k] = float(v)
                     except:
                        continue

                sorted_chars = sorted(clean_chars.items(), key=lambda x: x[1], reverse=True)
                character_list = [c[0] for c in sorted_chars if c[1] > 0.5] # Higher threshold for chars

            return tag_list, ", ".join(tag_list), character_list
                
        except asyncio.TimeoutError:
            print("Tagging Timeout (Queue/Process limit reached)")
        except Exception as e:
            print(f"Tagging Error: {e}")
            traceback.print_exc()
            
        return [], "timeout_fallback", []

    async def _fetch_tag_count(self, tag_name):
        """Fetches post count for a tag from Danbooru (with 30-day DB Cache)."""
        # 1. Check DB Cache
        async with aiosqlite.connect(self.bot.bank.db_path) as db:
            cursor = await db.execute("SELECT post_count, last_updated FROM tag_metadata WHERE tag_name = ?", (tag_name,))
            row = await cursor.fetchone()
            
            if row:
                count, last_updated_str = row
                last_updated = datetime.strptime(last_updated_str, "%Y-%m-%d %H:%M:%S")
                if datetime.now() - last_updated < timedelta(days=30):
                    return count

        # 2. Fetch from API
        try:
            print(f"Fetching count for tag: {tag_name}")
            async with aiohttp.ClientSession() as session:
                # Danbooru API: tags.json?search[name]=tag_name
                url = f"https://danbooru.donmai.us/tags.json"
                params = {"search[name]": tag_name}
                async with session.get(url, params=params) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data and isinstance(data, list):
                            post_count = data[0].get('post_count', 0)
                            
                            # Update Cache
                            now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            async with aiosqlite.connect(self.bot.bank.db_path) as db:
                                await db.execute(
                                    "INSERT OR REPLACE INTO tag_metadata (tag_name, post_count, last_updated) VALUES (?, ?, ?)",
                                    (tag_name, post_count, now_str)
                                )
                                await db.commit()
                            
                            return post_count
        except Exception as e:
            print(f"Danbooru API Error ({tag_name}): {e}")
            
        return 9999999 # Return high count (low rarity) on failure

    async def _run_scorer(self, file_path):
        """Runs the aesthetic scorer AI via Queue."""
        if not self.ai_client_score: return random.uniform(2.0, 5.0)
        
        future = self.bot.loop.create_future()
        await self.ai_queue.put(('score', file_path, future))
        
        try:
            # Enforce 20s timeout
            res = await asyncio.wait_for(future, timeout=30.0)
            return float(res)
        except:
            return random.uniform(2.0, 5.0)

    async def _calculate_price(self, score, tag_list, character_list):
        """Calculates final price, trend bonus, and rarity multiplier."""
        tag_multiplier = await self.get_tag_value_modifier(tag_list)
        base_price = 1000
        
        trends = await self.get_current_trends()
        trend_bonus = 0
        matched_trends = []
        
        if trends:
            for cat, val in trends.items():
                if val and val in tag_list:
                    trend_bonus += 5000 
                    matched_trends.append(val)
        
        # Character Bonus
        char_bonus = 0
        if character_list:
            char_bonus = 2000 * len(character_list)

        # --- Rarity Bonus (Danbooru) ---
        rarity_multiplier = 1.0
        
        # Filter commonly used tags to avoid dilution
        ignored_tags = {'1girl', 'solo', 'long_hair', 'breasts', 'looking_at_viewer', 'smile', 'blush', 'short_hair', 'open_mouth'}
        candidate_tags = [t for t in tag_list if t not in ignored_tags and t not in character_list] # Chars have their own bonus
        
        # We need to fetch counts. This can be slow if not cached, so limit to top 5 candidates?
        # Let's take first 5 from "tag_list" which is sorted by confidence usually? 
        # Actually tag_list is sorted by confidence.
        # Let's check top 5 confident tags.
        check_limit = 5
        checked_tags = []
        rarity_scores = []
        
        for tag in candidate_tags[:check_limit]:
             count = await self._fetch_tag_count(tag)
             # Formula: Multiplier boost based on rarity.
             # < 1000: x3.0
             # < 5000: x2.0
             # < 20000: x1.5
             # < 50000: x1.2
             # Else: x1.0
             
             mult = 1.0
             if count < 1000: mult = 3.0
             elif count < 5000: mult = 2.0
             elif count < 20000: mult = 1.5
             elif count < 50000: mult = 1.2
             
             rarity_scores.append(mult)
             if mult > 1.0:
                 checked_tags.append(f"{tag}({count})")

        if rarity_scores:
            # Take the MAX rarity found (reward the rarest feature), or average?
            # Max is better for "Jackpot" feeling.
            rarity_multiplier = max(rarity_scores)

        # Ensure score is within bounds
        score = max(0.0, min(10.0, score))
        
        # Final Algo: (Base Exponential) * SaturationMult * RarityMult + Trend + Char
        # New Formula: 1000 * (score^2)
        base_value_exp = int(1000 * (score ** 2))
        
        value_part = int(base_value_exp * tag_multiplier * rarity_multiplier)
        
        final_price = value_part + trend_bonus + char_bonus

        # --- Stock Market Influence ---
        # Trigger async stock update
        stocks_cog = self.bot.get_cog("StocksCog")
        if stocks_cog:
            for tag in tag_list:
                # User Formula:
                # 1. Supply (Smuggle): -0.5%
                change_rate = -0.005
                
                # 2. Quality (S-Rank >= 9.0): +2.0% Bonus
                # (Net result: -0.5% + 2.0% = +1.5%)
                if score >= 9.0: 
                    change_rate += 0.02
                
                # 3. Trend Bonus (If Applicable - Placeholder for now)
                # if is_trending(tag): change_rate *= 2
                
                multiplier = 1.0 + change_rate
                
                self.bot.loop.create_task(stocks_cog.update_stock_price(tag, multiplier))

        return final_price, trend_bonus, matched_trends, char_bonus, rarity_multiplier, checked_tags

    @commands.command(name="smuggle")
    async def smuggle(self, ctx):
        """The main loop: Upload -> Risk -> Gamble -> Appraise -> Sell"""
        if not ctx.message.attachments:
            await ctx.send("📦 **密輸品(画像)を添付してください！**")
            return
            
        attachment = ctx.message.attachments[0]
        if not attachment.content_type.startswith('image/'):
            await ctx.send("❌ 画像ファイルのみ有効です。")
            return

        image_url = attachment.url
        await ctx.send("🕵️ **密輸作戦を開始します...**")

        # 1. Download & Hash
        temp_path, img_hash = await self._download_and_hash(image_url)
        if not temp_path:
            await ctx.send("❌ ダウンロードに失敗しました。")
            return

        try:
            # 2. Bloom Filter Check (Fast Fail)
            # We check the Bloom Filter FIRST to avoid expensive DB queries for known duplicates.
            # If check returns True, it's LIKELY a duplicate (proceed to DB to confirm).
            # If False, it is DEFINITELY unique.
            if self.bloom.check(img_hash):
                print(f"Bloom Filter Warning: Hash {img_hash} might exist.")
            
            # 3. DB Duplicate Check (Strict & Reliable)
            # Even if Bloom said "No", we still check DB for *similar* images (hamming distance),
            # which Bloom Filter cannot do.
            is_dup, dup_msg, _ = await self.get_risk_factor(img_hash)
            
            if is_dup >= 50:
                 await ctx.send(f"❌ **密輸失敗:** {dup_msg}\n(同じ画像が既に存在します)")
                 return

            await ctx.send(f"✅ **密輸成功!**\n闇市の鑑定人に連絡しています...")
            
            # 4. AI Valuation
            tag_list, tags_str, character_list = await self._run_tagger(temp_path)
            score = await self._run_scorer(temp_path)
            
            # Removed score rejection check (< 4.0) to accept all items.

            # 5. Pricing
            final_price, trend_bonus, matched_trends, char_bonus, rarity_mult, rare_tags = await self._calculate_price(score, tag_list, character_list)
            
            # 6. Grading
            grade = "B"
            if score >= 9.0: grade = "S"
            elif score >= 7.0: grade = "A"
            
            # 7. Post to Gallery & DB Insert
            item_id = None
            async with aiosqlite.connect(self.bot.bank.db_path) as db:
                cursor = await db.execute(
                    """
                    INSERT INTO market_items (seller_id, image_url, aesthetic_score, price, status, image_hash, tags, grade, thread_id, message_id)
                    VALUES (?, ?, ?, ?, 'on_sale', ?, ?, ?, 0, 0)
                    """,
                    (self.bot.user.id, image_url, score, int(final_price * 1.5), img_hash, str(tag_list), grade)
                )
                item_id = cursor.lastrowid
                await db.commit()

            # Create Embed
            embed = discord.Embed(title=f"📦 新規入荷 (ID: #{item_id})", color=discord.Color.purple())
            embed.set_image(url=image_url)
            embed.add_field(name="販売者", value=self.bot.user.mention, inline=True)
            embed.add_field(name="価格", value=f"💰 {int(final_price * 1.5):,}", inline=True)
            embed.add_field(name="グレード", value=f"**{grade}** ({score:.2f})", inline=True)
            
            if rarity_mult > 1.0:
                 embed.add_field(name="✨ レアリティボーナス", value=f"x{rarity_mult:.1f} ({', '.join(rare_tags[:3])})", inline=True)
                 
            if character_list:
                chars_str = ", ".join(character_list)
                embed.add_field(name="👤 キャラクター", value=f"{chars_str} (+{char_bonus:,})", inline=True)
            if matched_trends:
                embed.add_field(name="🔥 トレンドボーナス!", value=f"+{trend_bonus:,} ({', '.join(matched_trends)})", inline=False)
            embed.add_field(name="特徴 (Tags)", value=tags_str[:1000], inline=False)
            
            # Post Logic
            try:
                await self._post_to_gallery(ctx, embed, temp_path, tags_str, item_id, grade, final_price, tag_list, image_url, img_hash)
            except Exception as e:
                await ctx.send(f"❌ 投稿処理中にエラーが発生: {e}")
                traceback.print_exc()
                # Rollback DB? (Optional)

        except Exception as e:
            await ctx.send(f"❌ エラーが発生しました: {e}")
            traceback.print_exc()
        finally:
             if os.path.exists(temp_path): os.remove(temp_path)

    async def _post_to_gallery(self, ctx, embed, temp_path, tags_str, item_id, grade, final_price, tag_list, image_url, img_hash):
        """Handles posting to the appropriate thread or forum."""
        bot_thread = None
        async with aiosqlite.connect(self.bot.bank.db_path) as db:
            cursor = await db.execute("SELECT thread_id FROM user_galleries WHERE user_id = ?", (self.bot.user.id,))
            row = await cursor.fetchone()
            if row:
                bot_thread = ctx.guild.get_thread(row[0])
                if not bot_thread:
                     try: bot_thread = await ctx.guild.fetch_channel(row[0])
                     except: pass

        from cogs.market import BuyView
        view = BuyView(self.bot)
        
        message = None
        thread_ref = None

        if bot_thread:
            thread_ref = bot_thread
            message = await bot_thread.send(
                content=f"**販売中:** {tags_str[:50]}... (ID: #{item_id})",
                embed=embed,
                file=discord.File(temp_path, filename="artifact.png"),
                view=view
            )
            await ctx.send(f"✅ **密輸成功！(ID: {item_id})**\n公式ギャラリーに入荷しました: {message.jump_url}")
        else:
             forum = discord.utils.get(ctx.guild.forums, name="闇市ギャラリー")
             if forum:
                title = f"[{grade}] {tags_str[:30]}..." if len(tags_str) > 30 else f"[{grade}] {tags_str}"
                if not title: title = f"[{grade}] 謎の品"

                thread_with_message = await forum.create_thread(
                    name=title,
                    content=f"**販売中:** {tags_str[:50]}... (ID: #{item_id})",
                    embed=embed,
                    file=discord.File(temp_path, filename="artifact.png"),
                    view=view
                )
                thread_ref = thread_with_message.thread if hasattr(thread_with_message, 'thread') else thread_with_message
                message = thread_with_message.message 
                if not message and hasattr(thread_ref, 'starter_message'): message = thread_ref.starter_message
                
                await ctx.send(f"✅ **密輸成功！(ID: {item_id})**\n臨時スレッドが作成されました: {thread_ref.mention}")
             else:
                await ctx.send("❌ フォーラム「闇市ギャラリー」が見つかりません。`!init_server` を確認してください。")
                return

        # DB Updates & Payment
        await self.bot.bank.deposit_credits(ctx.author, final_price)
        await self.update_market_trends(tag_list)
        
        # Update Bloom Filter
        self.bloom.add(image_url)
        self.bloom.add(img_hash)
        
        async with aiosqlite.connect(self.bot.bank.db_path) as db:
            await db.execute(
                "UPDATE market_items SET thread_id = ?, message_id = ? WHERE item_id = ?",
                (thread_ref.id, message.id if message else 0, item_id)
            )
            await db.commit()
        
        await ctx.send(f"💰 **報酬受取:** `{final_price:,} Credits` を受け取りました。")

    @commands.command(name="join")
    async def join(self, ctx):
        """闇のブローカーとして登録し、個人用ギャラリーを開設します。"""
        # 1. Check if already joined
        async with aiosqlite.connect(self.bot.bank.db_path) as db:
            cursor = await db.execute("SELECT thread_id FROM user_galleries WHERE user_id = ?", (ctx.author.id,))
            row = await cursor.fetchone()
        
        if row:
            await ctx.send(f"⚠️ 既に登録済みです。ギャラリー: <#{row[0]}>")
            return

        # 2. Assign Role & Find Forum
        role = discord.utils.get(ctx.guild.roles, name="密輸業者")
        forum = discord.utils.get(ctx.guild.forums, name="闇市ギャラリー")
        
        if not forum:
            await ctx.send("❌ フォーラム `闇市ギャラリー` が見つかりません。管理者に連絡してください。")
            return

        if role:
            try:
                await ctx.author.add_roles(role)
            except discord.Forbidden:
                await ctx.send("⚠️ ロールの付与に失敗しました(権限不足)。")

        # 3. Create Gallery Thread
        try:
            thread_with_message = await forum.create_thread(
                name=f"[Gallery] {ctx.author.display_name}",
                content=f"{ctx.author.mention} の個人ギャラリーへようこそ。\nここで獲得した戦利品が展示されます。"
            )
            thread = thread_with_message.thread if hasattr(thread_with_message, 'thread') else thread_with_message
            
            # 4. Save to DB
            async with aiosqlite.connect(self.bot.bank.db_path) as db:
                await db.execute("INSERT INTO user_galleries (user_id, thread_id) VALUES (?, ?)", (ctx.author.id, thread.id))
                await db.commit()
                
            # 5. Give Starting Funds
            await self.bot.bank.deposit_credits(ctx.author, 3000)
            
            await ctx.send(f"🎉 **登録完了！** あなたのギャラリーが開設されました: {thread.mention}\n💰 **開業資金 3,000クレジット** が支給されました！")

        except Exception as e:
            await ctx.send(f"❌ ギャラリー作成に失敗しました: {e}")
            traceback.print_exc()

    @commands.command(name="reset_game")
    @commands.has_permissions(administrator=True)
    async def reset_game(self, ctx):
        """(Debug) データベースを全消去し、ゲームをリセットします。"""
        await ctx.send("⚠️ **警告:** すべてのユーザーデータ、アイテム、ギャラリー情報が削除されます。よろしいですか？(yes/no)")
        
        def check(m):
            return m.author == ctx.author and m.channel == ctx.channel and m.content.lower() == "yes"

        try:
            await self.bot.wait_for('message', check=check, timeout=30.0)
        except asyncio.TimeoutError:
            await ctx.send("❌ タイムアウトしました。リセットはキャンセルされました。")
            return

        async with aiosqlite.connect(self.bot.bank.db_path) as db:
            tables = ["bank", "market_items", "market_trends", "user_galleries"]
            for table in tables:
                try:
                    await db.execute(f"DELETE FROM {table}")
                except Exception as e:
                    print(f"Failed to clear {table}: {e}")
            await db.commit()
        
        await ctx.send("🔥 **リセット完了/WIPE COMPLETE**\n全てのデータが削除されました。`!init_server` からやり直してください。")

class InventoryView(discord.ui.View):
    def __init__(self, ctx, items, per_page=5):
        super().__init__(timeout=60)
        self.ctx = ctx
        self.items = items
        self.per_page = per_page
        self.current_page = 0
        self.max_page = max(0, (len(items) - 1) // per_page)
        self.update_buttons()

    def update_buttons(self):
        self.prev_btn.disabled = self.current_page == 0
        self.next_btn.disabled = self.current_page == self.max_page

    def get_embed(self):
        start = self.current_page * self.per_page
        end = start + self.per_page
        batch = self.items[start:end]
        
        embed = discord.Embed(title=f"🎒 {self.ctx.author.display_name}の持ち物 ({self.current_page + 1}/{self.max_page + 1})", color=discord.Color.gold())
        if not batch:
             embed.description = "表示するアイテムがありません。"
             return embed
             
        description = ""
        for item_id, tags, thread_id, score in batch:
            # Shorten tags
            tag_summary = tags.split(",")[0] if tags else "不明"
            # Link to thread
            thread_link = f"<#{thread_id}>" if thread_id else "不明"
            description += f"**ID: {item_id}** | {tag_summary} (Score: {score:.1f}) | {thread_link}\n"
        
        embed.description = description
        embed.set_footer(text=f"Total: {len(self.items)} items")
        return embed

    @discord.ui.button(label="◀️", style=discord.ButtonStyle.blurple)
    async def prev_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user != self.ctx.author:
            await interaction.response.send_message("自分以外のインベントリは操作できません。", ephemeral=True)
            return

        if self.current_page > 0:
            self.current_page -= 1
            self.update_buttons()
            await interaction.response.edit_message(embed=self.get_embed(), view=self)
        else:
            await interaction.response.defer()

    @discord.ui.button(label="▶️", style=discord.ButtonStyle.blurple)
    async def next_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user != self.ctx.author:
            await interaction.response.send_message("自分以外のインベントリは操作できません。", ephemeral=True)
            return

        if self.current_page < self.max_page:
            self.current_page += 1
            self.update_buttons()
            await interaction.response.edit_message(embed=self.get_embed(), view=self)
        else:
             await interaction.response.defer()

    @commands.command(name="inventory", aliases=["bag", "inv"])
    async def inventory(self, ctx):
        """自分が所有している(購入済み)アイテムを表示します。"""
        async with aiosqlite.connect(self.bot.bank.db_path) as db:
            cursor = await db.execute("""
                SELECT item_id, tags, thread_id, aesthetic_score 
                FROM market_items 
                WHERE buyer_id = ? AND status = 'sold'
            """, (ctx.author.id,))
            rows = await cursor.fetchall()
            
        if not rows:
            await ctx.send("🎒 **持ち物:** 何も持っていません。ギャラリーで購入するか、密輸してください。")
            return

        view = InventoryView(ctx, rows, per_page=5)
        await ctx.send(embed=view.get_embed(), view=view)

class ResellPriceModal(discord.ui.Modal, title="再販価格の設定"):
    def __init__(self, bot, item_id):
        super().__init__()
        self.bot = bot
        self.item_id = item_id
        self.price_input = discord.ui.TextInput(
            label="価格 (Credits)",
            placeholder="100以上の整数",
            min_length=3,
            max_length=10
        )
        self.add_item(self.price_input)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            price = int(self.price_input.value)
            if price < 100: raise ValueError
        except:
             await interaction.response.send_message("❌ 価格は100以上の整数で入力してください。", ephemeral=True)
             return

        async with aiosqlite.connect(self.bot.bank.db_path) as db:
            # Re-verify ownership
            cursor = await db.execute("""
                SELECT thread_id, message_id, tags, aesthetic_score FROM market_items 
                WHERE item_id = ? AND buyer_id = ? AND status = 'sold'
            """, (self.item_id, interaction.user.id))
            row = await cursor.fetchone()
            
            if not row:
                await interaction.response.send_message("❌ エラー: アイテムを所有していないか、既に販売中です。", ephemeral=True)
                return
            
            thread_id, message_id, tags, score = row
            
            # Update DB
            await db.execute("""
                UPDATE market_items 
                SET status = 'on_sale', price = ?, seller_id = ?, buyer_id = NULL 
                WHERE item_id = ?
            """, (price, interaction.user.id, self.item_id))
            await db.commit()
            
            # Update Gallery Message
            try:
                guild = interaction.guild
                thread = guild.get_thread(thread_id)
                if not thread:
                     try: thread = await guild.fetch_channel(thread_id)
                     except: pass
                
                if thread:
                     try:
                         msg = await thread.fetch_message(message_id)
                         
                         # Edit Embed
                         embed = msg.embeds[0]
                         embed.clear_fields()
                         embed.title = "🔄 再販中 (Resale)"
                         embed.color = discord.Color.orange()
                         
                         tags_str = tags if tags else "None"
                         grade = "B"
                         if score >= 9.0: grade = "S"
                         elif score >= 7.0: grade = "A"
                         
                         embed.add_field(name="ID", value=f"**#{self.item_id}**", inline=True)
                         embed.add_field(name="販売者", value=interaction.user.mention, inline=True)
                         embed.add_field(name="価格", value=f"💰 {price:,}", inline=True)
                         embed.add_field(name="グレード", value=f"**{grade}** ({score:.2f})", inline=True)
                         embed.add_field(name="特徴 (Tags)", value=tags_str, inline=False)
                         
                         from cogs.market import BuyView
                         await msg.edit(content=f"📢 **再販中!** (ID: {self.item_id})", embed=embed, view=BuyView(self.bot))
                         
                         await interaction.response.send_message(f"✅ **再販設定完了！** (ID: {self.item_id}, Price: {price:,})\n🔗 {msg.jump_url}")
                         return
                     except Exception as e:
                         print(f"Failed to edit msg: {e}")
            except Exception as e:
                print(f"Resell Error: {e}")
            
            await interaction.response.send_message(f"✅ **再販設定完了(DBのみ)**: 元のメッセージが見つかりませんでしたが、販売リストには追加されました。")

class ResellSelect(discord.ui.Select):
    def __init__(self, bot, items):
        options = []
        for item_id, tags, score in items[:25]: # Max 25 options
            tag_summary = tags.split(",")[0] if tags else "Unknown"
            options.append(discord.SelectOption(
                label=f"ID: {item_id}",
                description=f"Score: {score:.1f} | {tag_summary}",
                value=str(item_id)
            ))
        super().__init__(placeholder="再販するアイテムを選択してください...", min_values=1, max_values=1, options=options)
        self.bot = bot

    async def callback(self, interaction: discord.Interaction):
        item_id = int(self.values[0])
        await interaction.response.send_modal(ResellPriceModal(self.bot, item_id))

class ResellSelectView(discord.ui.View):
    def __init__(self, bot, items):
        super().__init__(timeout=60)
        self.add_item(ResellSelect(bot, items))

    @commands.command(name="resell")
    async def resell(self, ctx):
        """所有しているアイテムを選択して再販します。"""
        async with aiosqlite.connect(self.bot.bank.db_path) as db:
            cursor = await db.execute("""
                SELECT item_id, tags, aesthetic_score 
                FROM market_items 
                WHERE buyer_id = ? AND status = 'sold'
                ORDER BY item_id DESC
            """, (ctx.author.id,))
            rows = await cursor.fetchall()
            
        if not rows:
            await ctx.send("🎒 **持ち物:** 再販できるアイテムを持っていません。")
            return

        view = ResellSelectView(self.bot, rows)
        await ctx.send("🔄 **再販するアイテムを選択してください:**", view=view)

    @commands.command(name="reset_risk")
    async def reset_risk(self, ctx):
        """(Debug) Clears all image hashes from the database to reset pHash risk."""
        async with aiosqlite.connect(self.bot.bank.db_path) as db:
            await db.execute("UPDATE market_items SET image_hash = NULL")
            await db.commit()
        await ctx.send("🔄 **記憶消去完了。** 当局は押収品に関するデータを失いました。\nこれで再び低リスクで密輸できます！")

async def setup(bot):
    await bot.add_cog(BrokerCog(bot))
