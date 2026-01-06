import math
import asyncio
import aiohttp
import aiosqlite
from datetime import datetime, timedelta
import random

class ValuationLogic:
    def __init__(self, db_path):
        self.db_path = db_path

    async def get_current_trends(self):
        date_key = datetime.now().strftime("%Y-%m-%d")
        async with aiosqlite.connect(self.db_path, timeout=60.0) as db:
            cursor = await db.execute("SELECT pose, costume, body FROM daily_trends WHERE date_key = ?", (date_key,))
            row = await cursor.fetchone()
            if row:
                return {'pose': row[0], 'costume': row[1], 'body': row[2]}
            else:
                # If missing, we return empty or None.
                # The caller might handle updating trends if they are missing.
                # For calculation purposes, if no trend exists, no bonus.
                return None

    async def get_tag_value_modifier(self, tags):
        """Calculates market saturation modifier."""
        multiplier = 1.0
        async with aiosqlite.connect(self.db_path, timeout=60.0) as db:
            # We can optimize this by using IN clause if possible, but SQLite limit is high.
            # But constructing the query is safer with loop for now or a single SELECT with WHERE IN
            if not tags:
                return 1.0

            placeholders = ','.join('?' for _ in tags)
            query = f"SELECT tag_name, saturation FROM market_trends WHERE tag_name IN ({placeholders})"
            try:
                cursor = await db.execute(query, tuple(tags))
                rows = await cursor.fetchall()

                for tag_name, sat in rows:
                    sat_mult = 1.0 / math.log10(max(sat, 0) + 2)
                    if sat_mult < multiplier:
                         multiplier = sat_mult
            except Exception as e:
                print(f"Error in get_tag_value_modifier: {e}")

        return max(multiplier, 0.1)

    async def _fetch_tag_count(self, tag_name, session):
        """Fetches post count for a tag from Danbooru (with 30-day DB Cache)."""
        # 1. Check DB Cache
        async with aiosqlite.connect(self.db_path, timeout=60.0) as db:
            cursor = await db.execute("SELECT post_count, last_updated FROM tag_metadata WHERE tag_name = ?", (tag_name,))
            row = await cursor.fetchone()

            if row:
                count, last_updated_str = row
                try:
                    last_updated = datetime.strptime(last_updated_str, "%Y-%m-%d %H:%M:%S")
                    if datetime.now() - last_updated < timedelta(days=30):
                        return count
                except ValueError:
                    pass # Date format error, fetch again

        # 2. Fetch from API
        try:
            # print(f"Fetching count for tag: {tag_name}")
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
                        async with aiosqlite.connect(self.db_path, timeout=60.0) as db:
                            await db.execute(
                                "INSERT OR REPLACE INTO tag_metadata (tag_name, post_count, last_updated) VALUES (?, ?, ?)",
                                (tag_name, post_count, now_str)
                            )
                            await db.commit()

                        return post_count
        except Exception as e:
            print(f"Danbooru API Error ({tag_name}): {e}")

        return 9999999 # Return high count (low rarity) on failure

    async def calculate_price(self, score, tag_list, character_list):
        """
        Calculates final price, trend bonus, and rarity multiplier.
        Returns: (final_price, trend_bonus, matched_trends, char_bonus, rarity_multiplier, checked_tags)
        """
        tag_multiplier = await self.get_tag_value_modifier(tag_list)

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

        ignored_tags = {'1girl', 'solo', 'long_hair', 'breasts', 'looking_at_viewer', 'smile', 'blush', 'short_hair', 'open_mouth'}
        candidate_tags = [t for t in tag_list if t not in ignored_tags and t not in character_list]

        check_limit = 5
        checked_tags_info = []
        rarity_scores = []

        tags_to_check = candidate_tags[:check_limit]

        if tags_to_check:
            async with aiohttp.ClientSession() as session:
                # Parallel fetch
                tasks = [self._fetch_tag_count(tag, session) for tag in tags_to_check]
                counts = await asyncio.gather(*tasks)

            for tag, count in zip(tags_to_check, counts):
                 mult = 1.0
                 if count < 1000: mult = 3.0
                 elif count < 5000: mult = 2.0
                 elif count < 20000: mult = 1.5
                 elif count < 50000: mult = 1.2

                 rarity_scores.append(mult)
                 if mult > 1.0:
                     checked_tags_info.append(f"{tag}({count})")

        if rarity_scores:
            rarity_multiplier = max(rarity_scores)

        # Ensure score is within bounds
        score = max(0.0, min(10.0, score))

        # New Formula: 1000 * (score^2)
        base_value_exp = int(1000 * (score ** 2))

        value_part = int(base_value_exp * tag_multiplier * rarity_multiplier)

        final_price = value_part + trend_bonus + char_bonus

        return final_price, trend_bonus, matched_trends, char_bonus, rarity_multiplier, checked_tags_info
