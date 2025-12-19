import discord
from discord.ext import commands
import asyncio
import aiosqlite

class SetupCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @commands.command(name="init_server")
    @commands.has_permissions(administrator=True)
    async def init_server(self, ctx):
        """
        자동으로 서버를 설정합니다. (관리자 전용)
        - 카테고리: 闇市 (Shadow Market)
        - 채널: 密輸現場 (Smuggling Spot)
        - 포럼: 闇市ギャラリー (Shadow Gallery)
        - 역할: 密輸業者 (Smuggler)
        """
        guild = ctx.guild
        
    @commands.command(name="init_server")
    @commands.has_permissions(administrator=True)
    async def init_server(self, ctx):
        """
        サーバーの構成を自動セットアップします。
        - ロール: 密輸業者
        - カテゴリ: 🏢 ロビー (Lobby), 🌑 闇市 (Shadow Market)
        - チャンネル: ルール, 参加受付, 雑談, 密輸現場, 賭博場, 番付, ギャラリー
        """
        guild = ctx.guild
        
        try:
            # 1. Create Role
            role_name = "密輸業者"
            role = discord.utils.get(guild.roles, name=role_name)
            if not role:
                try:
                    role = await guild.create_role(name=role_name, color=discord.Color.dark_grey(), hoist=True)
                    await ctx.send(f"✅ ロール作成完了: {role.mention}")
                except discord.Forbidden:
                    await ctx.send("❌ **エラー:** ロール作成権限がありません。")
                    return
            else:
                await ctx.send(f"ℹ️ ロールは既に存在します: {role.mention}")

            # ---------------------------------------------------------
            # Category 1: Lobby (Public)
            # ---------------------------------------------------------
            lobby_cat_name = "ロビー (Lobby)"
            lobby_cat = discord.utils.get(guild.categories, name=lobby_cat_name)
            
            # Permissions: Everyone can see
            lobby_overwrites = {
                guild.default_role: discord.PermissionOverwrite(read_messages=True, send_messages=False),
                guild.me: discord.PermissionOverwrite(read_messages=True, send_messages=True)
            }
            
            if not lobby_cat:
                lobby_cat = await guild.create_category(lobby_cat_name, overwrites=lobby_overwrites)
                await ctx.send(f"✅ カテゴリ作成: **{lobby_cat_name}**")
            
            # Channel: rules (Read Only)
            rules_ch_name = "ルール"
            rules_ch = discord.utils.get(guild.text_channels, name=rules_ch_name, category=lobby_cat)
            if not rules_ch:
                rules_ch = await guild.create_text_channel(rules_ch_name, category=lobby_cat)
                await ctx.send(f"✅ チャンネル作成: {rules_ch.mention}")
                
                # Post Rules
                embed = discord.Embed(title="🎮 ゲームの仕組み (How to Play)", color=discord.Color.red())
                embed.description = (
                    "**💰 目的**\n"
                    "画像を密輸（アップロード）してクレジットを稼ぎ、闇市のランキング上位を目指しましょう。\n\n"
                    "**🔄 ゲームの流れ**\n"
                    "1. **参加**: `!join` で闇市へのアクセス権を獲得。\n"
                    "2. **密輸**: `密輸現場` チャンネルで `!smuggle` コマンドと共に画像をアップロード。\n"
                    "3. **査定**: AIが以下の基準で画像を即座に査定・買取します。\n\n"
                    "**📊 査定基準**\n"
                    "- **美学スコア (Aesthetic)**: AIが画像の美しさを1-10点で採点。高得点ほど価格が **指数関数的** に跳ね上がります。\n"
                    "- **希少性 (Rarity)**: Danbooruで投稿数の少ない「レアなタグ」が含まれていると **最大3倍** のボーナス。\n"
                    "- **トレンド (Trends)**: 毎日変わるトレンド（ポーズ・衣装・特徴）に合致すると追加ボーナス。\n"
                    "- **キャラクター**: キャラクター名が特定されると +2,000 クレジット。\n\n"
                    "**💻 主なコマンド**\n"
                    "- `!join`: ゲームに参加する。\n"
                    "- `!smuggle`: 画像を添付して実行。密輸を行う。\n"
                    "- `!balance`: 現在の所持金を確認。\n"
                    "- `!pay @user [金額]`: 他のプレイヤーに送金。\n"
                )
                embed.set_footer(text="Economy Bot System")
                await rules_ch.send(embed=embed)
            
            # Channel: entry (Join Command)
            entry_ch_name = "参加受付"
            entry_overwrites = {
                guild.default_role: discord.PermissionOverwrite(read_messages=True, send_messages=True), # Allow typing !join
            }
            entry_ch = discord.utils.get(guild.text_channels, name=entry_ch_name, category=lobby_cat)
            if not entry_ch:
                entry_ch = await guild.create_text_channel(entry_ch_name, category=lobby_cat, overwrites=entry_overwrites)
                await ctx.send(f"✅ チャンネル作成: {entry_ch.mention}")
                
                # Post Welcome
                embed = discord.Embed(title="🚪 闇市への入り口", color=discord.Color.dark_blue())
                embed.description = (
                    "ようこそ、闇の世界へ。\n"
                    "取引に参加するには、以下のコマンドを入力して登録を済ませてください。\n\n"
                    "**コマンド:**\n"
                    "`!join`\n\n"
                    "※登録すると、奥のエリア（取引所、広場など）へのアクセス権が付与されます。"
                )
                await entry_ch.send(embed=embed)


            # ---------------------------------------------------------
            # Category 2: Shadow Market (Restricted)
            # ---------------------------------------------------------
            shadow_cat_name = "闇市 (Shadow Market)"
            shadow_cat = discord.utils.get(guild.categories, name=shadow_cat_name)
            
            # Permissions: Everyone FALSE, Role TRUE
            shadow_overwrites = {
                guild.default_role: discord.PermissionOverwrite(read_messages=False),
                role: discord.PermissionOverwrite(read_messages=True, send_messages=True),
                guild.me: discord.PermissionOverwrite(read_messages=True, send_messages=True, manage_channels=True)
            }

            if not shadow_cat:
                shadow_cat = await guild.create_category(shadow_cat_name, overwrites=shadow_overwrites)
                await ctx.send(f"✅ カテゴリ作成: **{shadow_cat_name}**")
            else:
                # Update permissions if exists
                await shadow_cat.edit(overwrites=shadow_overwrites)
                await ctx.send(f"♻️ カテゴリ権限更新: **{shadow_cat_name}**")

            # Create Channels
            # (Display Name, Code Name (unused here but good for logic), Topic)
            channels_to_create = [
                ("雑談", "general", "裏社会の社交場。"),
                ("トレンド", "trends", "本日の流行情報 (AM 6:00更新)。"),
                ("密輸現場", "smuggling-spot", "ここで `!smuggle` コマンドを使用します。"),
                ("賭博場", "casino", "金と運の使い道。"),
                ("番付", "leaderboard", "実力者たちのランキング。"),
                ("ログ", "shadow-logs", "取引履歴。")
            ]

            for ch_display, ch_name, topic in channels_to_create:
                ch = discord.utils.get(guild.text_channels, name=ch_display, category=shadow_cat)
                if not ch:
                    ch = await guild.create_text_channel(ch_display, category=shadow_cat, topic=topic)
                    await ctx.send(f"✅ チャンネル作成: {ch.mention}")
            
            # Forum: Gallery
            forum_name = "闇市ギャラリー"
            forum = discord.utils.get(guild.forums, name=forum_name, category=shadow_cat)
            if not forum:
                tags = [
                    discord.ForumTag(name="販売中", emoji="🟢"),
                    discord.ForumTag(name="完売", emoji="🔴"),
                    discord.ForumTag(name="S級", emoji="💎"),
                    discord.ForumTag(name="偽物", emoji="💩"),
                    discord.ForumTag(name="注目", emoji="🔥")
                ]
                forum = await guild.create_forum(name=forum_name, category=shadow_cat, topic="密輸品展示場", available_tags=tags)
                await ctx.send(f"✅ フォーラム作成: {forum.mention}")
            
            # Bot Gallery Setup (Same as before)
            if forum:
                async with aiosqlite.connect(self.bot.bank.db_path, timeout=60.0) as db:
                     cursor = await db.execute("SELECT thread_id FROM user_galleries WHERE user_id = ?", (self.bot.user.id,))
                     row = await cursor.fetchone()
                     if not row:
                         thread = await forum.create_thread(name="[Official] 闇のブローカー", content="公式取引所")
                         t = thread.thread if hasattr(thread, 'thread') else thread
                         await db.execute("INSERT OR REPLACE INTO user_galleries (user_id, thread_id) VALUES (?, ?)", (self.bot.user.id, t.id))
                         await db.commit()
                         await ctx.send("✅ 公式ギャラリー設立完了")

            await ctx.send("🎉 **サーバー構成の再構築が完了しました！**")

        except Exception as e:
            await ctx.send(f"❌ エラーが発生しました: {e}")
            import traceback
            traceback.print_exc()

async def setup(bot):
    await bot.add_cog(SetupCog(bot))
