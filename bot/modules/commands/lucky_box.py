#! /usr/bin/python3
# -*- coding: utf-8 -*-
import asyncio
import html
import json
import random
import re
import shutil
import unicodedata
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Optional

from pyrogram import filters, enums
from pyrogram.enums import ChatType, ChatMemberStatus, ParseMode
from pyrogram.errors import FloodWait, BadRequest
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message

from bot import bot, LOGGER, group, prefixes, bot_name, bot_photo, owner, admins, ranks
from bot.func_helper.filters import user_in_group_on_filter
from bot.func_helper.msg_utils import sendMessage, deleteMessage, callAnswer, editMessage
from bot.func_helper.utils import pwd_create
from bot.sql_helper import Session
from bot.sql_helper.sql_code import Code, sql_add_code
from bot.sql_helper.sql_emby import Emby, sql_get_emby, sql_update_emby


class LuckyBoxConfig:
    GAME_VERSION = "魔改版"
    DISABLED_LUCKY_BOX_CHATS = []
    DISABLED_FISHING_CHATS = []
    LUCKY_BOX_CHANCE = 0.005
    LUCKY_BOX_DAILY_LIMIT = 1
    LUCKY_BOX_EXPIRE_TIME = 600
    AUTO_DELETE_RESULTS_DELAY_SECONDS = 0
    BOX_STOCK_REPORT_DELETE_DELAY = 30
    ENABLE_SPECIAL_REWARDS = True
    USE_LEGACY_PRIZE_POOL = True
    PIN_WHITELIST_MESSAGE = True
    PIN_SILENTLY = True
    PINNED_MESSAGE_DELETE_DELAY = 900
    PINNED_MESSAGE_NOTIFY_MEMBERS = False

    STATUS_MAP = {
        "a": "⚪️ 白名单 (不受限)",
        "b": "✅ 普通用户",
        "c": "🚫 已禁用 (禁用账户)",
        "f": "🟢 绿名单 (普通用户)",
        "g": "🟣 紫名单 (普通用户)",
        "h": "🟠 橙名单 (普通用户)",
        "i": "🔵 蓝名单 (普通用户)",
        "j": "🟡 黄名单 (普通用户)",
        "k": "🔴 赤名单 (普通用户)",
        "l": "🌸 樱之守护 (普通用户)",
        "m": "🔰 见习骑士 (普通用户)",
        "n": "⚔️ 白银骑士 (普通用户)",
        "q": "⚜️ 黄金骑士 (普通用户)",
        "e": "⚫️ 黑名单 (禁用账户)",
        "o": "💀 堕入虚空 (禁用账户)",
        "p": "👻 幽灵部员 (禁用账户)",
    }

    SPECIAL_USER_CODES = ["f", "g", "h", "i", "j", "k", "l", "m", "n", "q"]
    DISABLED_STATUS_CODES = {"c", "e", "o", "p"}

    FISHABLE_ITEMS = [
        {"name": "鱼 骨", "icon": "🦴", "value": 0, "weight": 50},
        {"name": "破 鞋", "icon": "👟", "value": 0, "weight": 45},
        {"name": "生锈的铁罐", "icon": "🥫", "value": 0, "weight": 40},
        {"name": "漂流瓶", "icon": "🍾", "value": 0, "weight": 35},
        {"name": "海 草", "icon": "🌿", "value": 1, "weight": 30},
        {"name": "海 螺", "icon": "🐚", "value": 1, "weight": 25},
        {"name": "小 虾", "icon": "🦐", "value": 2, "weight": 20},
        {"name": "海 胆", "icon": "🌵", "value": 2, "weight": 15},
        {"name": "海 盐", "icon": "🧂", "value": 3, "weight": 12},
        {"name": "热带鱼", "icon": "🐠", "value": 3, "weight": 10},
        {"name": "海 蜇", "icon": "🦑", "value": 4, "weight": 8},
        {"name": "河 豚", "icon": "🐡", "value": 4, "weight": 6},
        {"name": "螃 蟹", "icon": "🦀", "value": 5, "weight": 5},
        {"name": "金 鱼", "icon": "🐟", "value": 5, "weight": 3},
        {"name": "一瓶朗姆酒", "icon": "🥃", "value": 6, "weight": 2},
        {"name": "锦 鲤", "icon": "🎏", "value": 6, "weight": 1},
        {"name": "章 鱼", "icon": "🐙", "value": 8, "weight": 0.5},
        {"name": "旗 鱼", "icon": "⚔️", "value": 8, "weight": 0.3},
        {"name": "海 星", "icon": "⭐️", "value": 10, "weight": 0.2},
        {"name": "大龙虾", "icon": "🦞", "value": 10, "weight": 0.1},
        {"name": "古老的金币", "icon": "🪙", "value": 15, "weight": 0.05},
        {"name": "美人鱼的鳞片", "icon": "✨", "value": 15, "weight": 0.03},
        {"name": "宝 箱", "icon": "📦", "value": 25, "weight": 0.02},
        {"name": "克苏鲁的触手", "icon": "🌀", "value": 25, "weight": 0.01},
        {"name": "海王的三叉戟", "icon": "🔱", "value": 50, "weight": 0.005},
        {"name": "注册码", "icon": "🎁", "value": 0, "weight": 0.000005, "special_item": "registration_code", "days": 30},
        {"name": "30天续期码", "icon": "🎁", "value": 0, "weight": 0, "special_item": "renewal_code", "days": 30},
        {"name": "90天续期码", "icon": "🎁", "value": 0, "weight": 0.0005, "special_item": "renewal_code", "days": 90},
        {"name": "180天续期码", "icon": "🎁", "value": 0, "weight": 0.00009, "special_item": "renewal_code", "days": 180},
        {"name": "365天续期码", "icon": "🎁", "value": 0, "weight": 0.00005, "special_item": "renewal_code", "days": 365},
        {"name": "白名单", "icon": "🌟", "value": 0, "weight": 0.0000005, "special_item": "whitelist", "days": 0},
    ]

    SPECIAL_REWARDS_LIMIT = 1
    REGISTRATION_CODE_LIMIT = 1
    WHITELIST_LIMIT = 1

    LUCKY_BOX_PRIZES = [
        {"name": "30天续期码", "type": "renewal_code", "value": 30, "weight": 0.5, "stock_key": "renewal_30"},
        {"name": "随机身份", "type": "random_status", "value": 0, "weight": 99.4, "stock_key": "random_status"},
        {"name": "90天续期码", "type": "renewal_code", "value": 90, "weight": 0.1, "stock_key": "renewal_90"},
        {"name": "180天续期码", "type": "renewal_code", "value": 180, "weight": 0, "stock_key": "renewal_180"},
        {"name": "365天续期码", "type": "renewal_code", "value": 365, "weight": 0, "stock_key": "renewal_365"},
        {"name": "白名单", "type": "whitelist", "value": 0, "weight": 0, "stock_key": "whitelist"},
    ]

    LIMIT_RENEWAL_30 = 1
    LIMIT_RENEWAL_90 = 0
    LIMIT_RENEWAL_180 = 0
    LIMIT_RENEWAL_365 = 0
    LIMIT_REGISTRATION_30 = 1
    LIMIT_WHITELIST = 1
    LIMIT_RANDOM_STATUS = 100000000000

    LIMIT_MAPPING = {
        "renewal_30": LIMIT_RENEWAL_30,
        "renewal_90": LIMIT_RENEWAL_90,
        "renewal_180": LIMIT_RENEWAL_180,
        "renewal_365": LIMIT_RENEWAL_365,
        "registration_30": LIMIT_REGISTRATION_30,
        "whitelist": LIMIT_WHITELIST,
        "random_status": LIMIT_RANDOM_STATUS,
    }

    GAME_COMMANDS = ["fish", "blackjack", "bull", "battle", "mario"]


class GamePersistence:
    def __init__(self, file_path: Path, default_data: Optional[Dict[str, Any]] = None):
        self.file_path = file_path
        self.default_data = default_data or {}
        self.backup_path = file_path.with_suffix(".json.bak")

    def load(self) -> Dict[str, Any]:
        try:
            if self.file_path.exists():
                with open(self.file_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    return data
        except json.JSONDecodeError as exc:
            LOGGER.error(f"lucky box json decode failed: {self.file_path} err={exc}")
            return self._try_restore_backup()
        except Exception as exc:
            LOGGER.warning(f"lucky box load failed: {self.file_path} err={exc}")
        return self.default_data.copy()

    def save(self, data: Dict[str, Any]) -> bool:
        try:
            if self.file_path.exists():
                self.backup_path.write_bytes(self.file_path.read_bytes())
            self.file_path.write_text(json.dumps(data, indent=4, ensure_ascii=False), encoding="utf-8")
            return True
        except Exception as exc:
            LOGGER.error(f"lucky box save failed: {self.file_path} err={exc}")
            return False

    def _try_restore_backup(self) -> Dict[str, Any]:
        try:
            if self.backup_path.exists():
                with open(self.backup_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                shutil.copy2(self.backup_path, self.file_path)
                return data
        except Exception as exc:
            LOGGER.error(f"lucky box backup restore failed: {self.file_path} err={exc}")
        return self.default_data.copy()

    def update(self, key: str, value: Any) -> bool:
        try:
            data = self.load()
            data[key] = value
            return self.save(data)
        except Exception as exc:
            LOGGER.error(f"lucky box update failed: key={key} err={exc}")
            return False

    def get(self, key: str, default: Any = None) -> Any:
        try:
            data = self.load()
            return data.get(key, default)
        except Exception as exc:
            LOGGER.error(f"lucky box get failed: key={key} err={exc}")
            return default

    def delete_backup(self):
        try:
            if self.backup_path.exists():
                self.backup_path.unlink()
        except Exception as exc:
            LOGGER.error(f"lucky box delete backup failed: {self.backup_path} err={exc}")


def create_persistence(file_name: str, default_data: Optional[Dict[str, Any]] = None) -> GamePersistence:
    return GamePersistence(Path(__file__).with_name(file_name), default_data)


def _escape_html_text(value: str) -> str:
    return html.escape(str(value or ""), quote=False)


def _mention_html(name: str, tg_id: int) -> str:
    return f'<a href="tg://user?id={tg_id}">{_escape_html_text(name or tg_id)}</a>'


def _normalize_group_chat_id(raw):
    if isinstance(raw, int):
        return raw
    text = unicodedata.normalize("NFKC", str(raw or "")).strip()
    if not text:
        return None
    text = text.replace("−", "-").replace("—", "-").replace("–", "-").replace("－", "-")
    for prefix in ("https://t.me/", "http://t.me/", "t.me/"):
        if text.startswith(prefix):
            text = text[len(prefix):]
            break
    text = text.strip().strip("/")
    if re.fullmatch(r"-?\d+", text):
        return int(text)
    if text.startswith("@"):
        return text
    if re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{3,}", text):
        return f"@{text}"
    return None


def _allowed_group_ids():
    ids = []
    for item in group or []:
        gid = _normalize_group_chat_id(item)
        if gid is not None:
            ids.append(gid)
    return ids


def _chat_matches_targets(chat, targets) -> bool:
    if not chat or not targets:
        return False
    numeric_ids = set()
    usernames = set()
    for item in targets:
        normalized = _normalize_group_chat_id(item)
        if isinstance(normalized, int):
            numeric_ids.add(normalized)
        elif isinstance(normalized, str) and normalized.startswith("@"):
            usernames.add(normalized.lstrip("@").lower())
    if chat.id in numeric_ids:
        return True
    chat_username = getattr(chat, "username", None)
    if chat_username and chat_username.lower() in usernames:
        return True
    return False


async def _is_group_admin(chat_id: int, user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(chat_id, user_id)
        return member.status in {ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER}
    except Exception:
        return False


async def _send_html(chat_id: int, text: str, reply_markup=None):
    return await bot.send_message(
        chat_id=chat_id,
        text=text,
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
        reply_markup=reply_markup,
    )


async def _edit_html(chat_id: int, message_id: int, text: str, reply_markup=None):
    return await bot.edit_message_text(
        chat_id=chat_id,
        message_id=message_id,
        text=text,
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
        reply_markup=reply_markup,
    )


async def _delete_after_delay(chat_id: int, message_id: int, delay: int):
    if delay <= 0:
        return
    await asyncio.sleep(delay)
    try:
        await bot.delete_messages(chat_id, message_id)
    except Exception as exc:
        LOGGER.warning(f"lucky box delete failed: chat={chat_id} msg={message_id} err={exc}")


async def _unpin_and_delete_after_delay(chat_id: int, message_id: int, delay: int):
    if delay <= 0:
        return
    await asyncio.sleep(delay)
    try:
        await bot.unpin_chat_message(chat_id=chat_id, message_id=message_id)
    except Exception as exc:
        LOGGER.warning(f"lucky box unpin failed: chat={chat_id} msg={message_id} err={exc}")
    try:
        await bot.delete_messages(chat_id, message_id)
    except Exception as exc:
        LOGGER.warning(f"lucky box pinned delete failed: chat={chat_id} msg={message_id} err={exc}")


stock_persistence = create_persistence(
    "lucky_box_stock.json",
    {key: limit for key, limit in LuckyBoxConfig.LIMIT_MAPPING.items()},
)
user_tracker_persistence = create_persistence("lucky_box_user_tracker.json", {})

CURRENT_PRIZE_STOCK = stock_persistence.load()
USER_TRIGGER_DATA = user_tracker_persistence.load()
DATA_LOCK = asyncio.Lock()


def _build_whitelist_code(days: int, prefix: str = "RegisterWL") -> str:
    return f"{ranks.logo}-{days}-{prefix}_{random.randint(1000000000, 9999999999)}"


async def _notify_private(user_id: int, text: str) -> bool:
    try:
        await _send_html(user_id, text)
        return True
    except FloodWait as exc:
        await asyncio.sleep(exc.value * 1.2)
        try:
            await _send_html(user_id, text)
            return True
        except Exception as fallback_exc:
            LOGGER.warning(f"lucky box private notify fallback failed: {fallback_exc}")
            return False
    except Exception as exc:
        LOGGER.warning(f"lucky box private notify failed: {exc}")
        return False


def _delete_code(code: str) -> bool:
    try:
        with Session() as session:
            deleted = session.query(Code).filter(Code.code == code).delete(synchronize_session=False)
            session.commit()
            return bool(deleted)
    except Exception as exc:
        LOGGER.error(f"lucky box delete code failed: code={code} err={exc}")
        return False


async def expire_lucky_box_message(message: Message, owner_id: int):
    await asyncio.sleep(LuckyBoxConfig.LUCKY_BOX_EXPIRE_TIME)
    try:
        latest = await bot.get_messages(message.chat.id, message.id)
        if not latest or not latest.reply_markup:
            return
        if owner_id:
            try:
                user = await bot.get_users(owner_id)
                user_name = user.first_name or str(owner_id)
                expire_text = (
                    f"🎁 <b>盲盒已自动失效</b>\n\n"
                    f"这个由 {_mention_html(user_name, owner_id)} 触发的盲盒已经过期了，下次要快点哦！"
                )
            except Exception:
                expire_text = "🎁 <b>盲盒已自动失效</b>\n\n这个盲盒已经过期了，下次要快点哦！"
        else:
            expire_text = "🎁 <b>盲盒已自动失效</b>\n\n这个由管理员派发的盲盒已经过期了，下次要快点哦！"
        await _edit_html(latest.chat.id, latest.id, expire_text, reply_markup=None)
    except Exception as exc:
        LOGGER.warning(f"lucky box expire failed: msg={message.id} err={exc}")


def _is_renew_code(input_string: str) -> bool:
    return "Renew" in input_string


def _pick_prize() -> Optional[Dict[str, Any]]:
    items = []
    weights = []

    prize_pool = LuckyBoxConfig.LUCKY_BOX_PRIZES if LuckyBoxConfig.USE_LEGACY_PRIZE_POOL else LuckyBoxConfig.FISHABLE_ITEMS

    for item in prize_pool:
        stock_key = item.get("stock_key")
        if stock_key:
            if CURRENT_PRIZE_STOCK.get(stock_key, 0) > 0:
                items.append(item)
                weights.append(item["weight"])
            continue

        special = item.get("special_item")
        if special and not LuckyBoxConfig.ENABLE_SPECIAL_REWARDS:
            continue
        items.append(item)
        weights.append(item["weight"])

    if not items:
        return None
    return random.choices(items, weights=weights, k=1)[0]


async def _handle_special_prize(item: Dict[str, Any], user_id: int, user_name: str, chat_id: int):
    item_type = item.get("special_item")
    if item_type in {"registration_code", "renewal_code"}:
        days = int(item["days"])
        code_prefix = "Register" if item_type == "registration_code" else "Renew"
        code_str = f"{ranks.logo}-{days}-{code_prefix}_{await pwd_create(10)}"
        if not sql_add_code([code_str], tg=user_id, us=days):
            raise RuntimeError("sql_add_code failed")
        start_link = f"https://t.me/{bot_name}?start={code_str}"
        private_msg_text = (
            f"恭喜您在幸运盲盒中获得了 <b>{_escape_html_text(item['name'])}</b>！\n\n"
            f"请点击下方链接来兑换：\n{_escape_html_text(start_link)}\n\n"
            f"您的专属码为：<code>{_escape_html_text(code_str)}</code>\n"
            f"有效期{days}天，请尽快使用！"
        )
        if not await _notify_private(user_id, private_msg_text):
            _delete_code(code_str)
            raise RuntimeError("lucky box private notify failed for code reward")
        return item["name"], code_str

    if item_type == "whitelist":
        user_emby_info = sql_get_emby(user_id)
        if user_emby_info and user_emby_info.embyid:
            if not sql_update_emby(Emby.tg == user_id, lv="a"):
                raise RuntimeError("sql_update_emby failed for whitelist reward")
            return item["name"], None

        reg_days = 30
        code_str = _build_whitelist_code(reg_days)
        if not sql_add_code([code_str], tg=user_id, us=reg_days):
            raise RuntimeError("sql_add_code failed")
        private_msg_text = (
            f"🎉 <b>恭喜您获得了传说级奖励：白名单资格！</b> 🎉\n\n"
            f"系统检测到您尚未绑定Emby账户，因此已为您自动生成一枚 <b>专属白名单注册码</b>。\n\n"
            f"请点击下方链接完成注册，注册成功后您的账户将 <b>自动激活为白名单用户</b>！\n"
            f"https://t.me/{bot_name}?start={_escape_html_text(code_str)}\n\n"
            f"您的专属码为：<code>{_escape_html_text(code_str)}</code>\n"
            f"请尽快使用！"
        )
        if not await _notify_private(user_id, private_msg_text):
            _delete_code(code_str)
            raise RuntimeError("lucky box private notify failed for whitelist reward")
        return item["name"], code_str

    if item_type == "random_status":
        new_lv_code = random.choice(LuckyBoxConfig.SPECIAL_USER_CODES)
        status_name = LuckyBoxConfig.STATUS_MAP.get(new_lv_code, "未知身份")
        with Session() as session:
            record = session.query(Emby).filter(Emby.tg == user_id).with_for_update().first()
            if not record:
                record = Emby(tg=user_id, lv=new_lv_code)
                session.add(record)
            else:
                if record.lv == "a":
                    status_name = "白名单用户"
                else:
                    record.lv = new_lv_code
            session.commit()
        return f"新身份: {status_name}", None

    return item.get("name", "未知奖励"), None


async def _claim_lucky_box(_, query):
    claimer_id = query.from_user.id
    data_parts = query.data.split("_")
    if len(data_parts) < 3:
        await query.answer("未知的盲盒类型，无法处理。", show_alert=True)
        return

    box_type = data_parts[1]
    if box_type == "personal":
        try:
            intended_owner_id = int(data_parts[2])
        except ValueError:
            await query.answer("未知的盲盒类型，无法处理。", show_alert=True)
            return
        if claimer_id != intended_owner_id:
            await query.answer("这不是你的盲盒哦！", show_alert=True)
            return
    elif box_type != "public":
        await query.answer("未知的盲盒类型，无法处理。", show_alert=True)
        return

    if (datetime.now() - query.message.date).total_seconds() > LuckyBoxConfig.LUCKY_BOX_EXPIRE_TIME:
        await query.answer("这个盲盒已经过期失效啦！", show_alert=True)
        if query.message.reply_markup:
            await _edit_html(
                query.message.chat.id,
                query.message.id,
                f"🎁 <b>盲盒已失效</b>\n\n这个由 {_mention_html(query.from_user.first_name, claimer_id)} 触发的盲盒已经过期了。",
                reply_markup=None,
            )
        return

    async with DATA_LOCK:
        if not query.message.reply_markup:
            await query.answer("这个盲盒已经被领取过了。", show_alert=True)
            return
        try:
            await query.message.edit_reply_markup(reply_markup=None)
        except BadRequest:
            await query.answer("这个盲盒已经被领取过了。", show_alert=True)
            return
        except Exception as exc:
            LOGGER.warning(f"lucky box remove markup failed: {exc}")
            await query.answer("这个盲盒已经被领取过了。", show_alert=True)
            return

        chosen_prize = None
        attempts = 0
        while attempts < 10:
            chosen_prize = _pick_prize()
            if not chosen_prize:
                break
            stock_key = chosen_prize.get("stock_key")
            if not stock_key:
                break
            limit = LuckyBoxConfig.LIMIT_MAPPING.get(stock_key, -1)
            if limit == -1 or CURRENT_PRIZE_STOCK.get(stock_key, 0) > 0:
                break
            attempts += 1
        else:
            await _edit_html(
                query.message.chat.id,
                query.message.id,
                "🎁 <b>盲盒领取失败</b>\n\n真不巧，奖品库正在补货中... 请稍后再试吧！",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🎁 点击领取", callback_data=query.data)]])
            )
            await query.answer("真不巧，奖品库正在补货中... 请稍后再试吧！", show_alert=True)
            return

        if not chosen_prize:
            try:
                await query.message.edit_reply_markup(
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🎁 点击领取", callback_data=query.data)]])
                )
            except Exception as exc:
                LOGGER.warning(f"lucky box restore markup when no prize selected failed: {exc}")
            await query.answer("真不巧，奖品库正在补货中... 请稍后再试吧！", show_alert=True)
            return

        prize_name = chosen_prize["name"]
        prize_type = chosen_prize.get("type") or chosen_prize.get("special_item") or "normal"
        prize_value = int(chosen_prize.get("days") or chosen_prize["value"] or 0)
        stock_key = chosen_prize.get("stock_key")

        if stock_key and LuckyBoxConfig.LIMIT_MAPPING.get(stock_key, -1) != -1:
            previous_stock = CURRENT_PRIZE_STOCK.copy()
            CURRENT_PRIZE_STOCK[stock_key] = max(CURRENT_PRIZE_STOCK.get(stock_key, 0) - 1, 0)
            if not stock_persistence.save(CURRENT_PRIZE_STOCK):
                CURRENT_PRIZE_STOCK.clear()
                CURRENT_PRIZE_STOCK.update(previous_stock)
                try:
                    await query.message.edit_reply_markup(
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🎁 点击领取", callback_data=query.data)]])
                    )
                except Exception as exc:
                    LOGGER.warning(f"lucky box restore markup after stock save failed: {exc}")
                await query.answer("奖品库存保存失败，请稍后再试。", show_alert=True)
                return

        try:
            private_msg = None
            if prize_type == "registration_code":
                code = f"{ranks.logo}-{prize_value}-Register_{await pwd_create(10)}"
                if not sql_add_code([code], tg=claimer_id, us=prize_value):
                    raise RuntimeError("sql_add_code failed")
                private_msg = (
                    f"恭喜您在幸运盲盒中获得了 <b>{_escape_html_text(prize_name)}</b>！\n\n"
                    f"请点击下方链接来兑换：\nhttps://t.me/{bot_name}?start={_escape_html_text(code)}\n\n"
                    f"您的专属码为：<code>{_escape_html_text(code)}</code>\n"
                    f"有效期{prize_value}天，请尽快使用！"
                )
                if not await _notify_private(claimer_id, private_msg):
                    _delete_code(code)
                    raise RuntimeError("private notify failed for registration code")

            elif prize_type == "renewal_code":
                code = f"{ranks.logo}-{prize_value}-Renew_{await pwd_create(10)}"
                if not sql_add_code([code], tg=claimer_id, us=prize_value):
                    raise RuntimeError("sql_add_code failed")
                private_msg = (
                    f"恭喜您在幸运盲盒中获得了 <b>{_escape_html_text(prize_name)}</b>！\n\n"
                    f"请点击下方链接来兑换：\nhttps://t.me/{bot_name}?start={_escape_html_text(code)}\n\n"
                    f"您的专属码为：<code>{_escape_html_text(code)}</code>\n"
                    f"有效期{prize_value}天，请尽快使用！"
                )
                if not await _notify_private(claimer_id, private_msg):
                    _delete_code(code)
                    raise RuntimeError("private notify failed for renewal code")

            elif prize_type == "whitelist":
                user_emby_info = sql_get_emby(claimer_id)
                if user_emby_info and user_emby_info.embyid:
                    if not sql_update_emby(Emby.tg == claimer_id, lv="a"):
                        raise RuntimeError("sql_update_emby failed for whitelist reward")
                    private_msg = "🎉 恭喜您在幸运盲盒中获得了白名单资格，您的账号已切换为白名单。"
                    await _notify_private(claimer_id, private_msg)
                else:
                    code = _build_whitelist_code(30)
                    if not sql_add_code([code], tg=claimer_id, us=30):
                        raise RuntimeError("sql_add_code failed")
                    private_msg = (
                        "🎉 <b>恭喜您获得了传说级奖励：白名单资格！</b> 🎉\n\n"
                        "系统检测到您尚未绑定Emby账户，因此已为您自动生成一枚 <b>专属白名单注册码</b>。\n\n"
                        f"请点击下方链接完成注册，注册成功后您的账户将 <b>自动激活为白名单用户</b>！\n"
                        f"https://t.me/{bot_name}?start={_escape_html_text(code)}\n\n"
                        f"您的专属码为：<code>{_escape_html_text(code)}</code>\n"
                        "请尽快使用！"
                    )
                    if not await _notify_private(claimer_id, private_msg):
                        _delete_code(code)
                        raise RuntimeError("private notify failed for whitelist code")

            elif prize_type == "random_status":
                new_lv_code = random.choice(LuckyBoxConfig.SPECIAL_USER_CODES)
                status_name = LuckyBoxConfig.STATUS_MAP.get(new_lv_code, "未知身份")
                with Session() as session:
                    record = session.query(Emby).filter(Emby.tg == claimer_id).with_for_update().first()
                    if not record:
                        record = Emby(tg=claimer_id, lv=new_lv_code)
                        session.add(record)
                    else:
                        if record.lv == "a":
                            status_name = "白名单用户"
                        else:
                            record.lv = new_lv_code
                    session.commit()
                    if record.lv != "a" and record.lv != new_lv_code:
                        raise RuntimeError("random status update failed")
                prize_name = f"新身份: {status_name}"
            else:
                private_msg = f"🎉 恭喜您在幸运盲盒中获得了 {prize_name}！"
                await _notify_private(claimer_id, private_msg)

        except Exception as exc:
            LOGGER.error(f"lucky box prize handling failed: {exc}")
            if stock_key and LuckyBoxConfig.LIMIT_MAPPING.get(stock_key, -1) != -1:
                CURRENT_PRIZE_STOCK[stock_key] = CURRENT_PRIZE_STOCK.get(stock_key, 0) + 1
                if not stock_persistence.save(CURRENT_PRIZE_STOCK):
                    LOGGER.warning(f"lucky box stock rollback save failed for key={stock_key}")
            try:
                await query.message.edit_reply_markup(
                    reply_markup=InlineKeyboardMarkup(
                        [[InlineKeyboardButton("🎁 点击领取", callback_data=query.data)]]
                    )
                )
            except Exception as markup_exc:
                LOGGER.warning(f"lucky box restore markup failed: {markup_exc}")
            await query.answer("领取失败，请稍后再试。", show_alert=True)
            return

    try:
        callback_answer_text = "领取成功！"
        if prize_type == "whitelist":
            callback_answer_text = "领取成功！你获得了白名单资格！"
        elif prize_type in {"registration_code", "renewal_code"}:
            callback_answer_text = f"领取成功！你获得了 {prize_name}，请查收私信！"
        elif prize_type == "random_status":
            callback_answer_text = f"领取成功！你的新身份是: {prize_name}"
        else:
            callback_answer_text = f"领取成功！你获得了 {prize_name}"
        await query.answer(callback_answer_text, show_alert=True)
    except Exception as exc:
        LOGGER.warning(f"lucky box callback answer failed: {exc}")

    final_text_content = (
        "<b>🎉 恭喜！幸运盲盒已被领取！</b>\n\n"
        f"🎁 <b>领奖人</b>: {_mention_html(query.from_user.first_name, claimer_id)}\n"
        f"✨ <b>奖品</b>: {prize_name if prize_type == 'random_status' else f'<b>{_escape_html_text(prize_name)}</b>'}\n\n"
        "期待下一个幸运儿！"
    )

    try:
        edited_msg = await _edit_html(query.message.chat.id, query.message.id, final_text_content, reply_markup=None)
    except Exception as exc:
        LOGGER.warning(f"lucky box edit final message failed: {exc}")
        edited_msg = None

    was_pinned = False
    if edited_msg and LuckyBoxConfig.PIN_WHITELIST_MESSAGE and prize_type == "whitelist":
        try:
            await bot.pin_chat_message(
                chat_id=edited_msg.chat.id,
                message_id=edited_msg.id,
                disable_notification=LuckyBoxConfig.PIN_SILENTLY,
            )
            was_pinned = True
        except Exception as exc:
            LOGGER.error(f"lucky box pin failed: chat={edited_msg.chat.id}, msg={edited_msg.id}, err={exc}")

    if edited_msg:
        if was_pinned and prize_type == "whitelist" and LuckyBoxConfig.PINNED_MESSAGE_DELETE_DELAY > 0:
            asyncio.create_task(
                _unpin_and_delete_after_delay(
                    edited_msg.chat.id,
                    edited_msg.id,
                    LuckyBoxConfig.PINNED_MESSAGE_DELETE_DELAY,
                )
            )
        elif LuckyBoxConfig.AUTO_DELETE_RESULTS_DELAY_SECONDS > 0:
            asyncio.create_task(
                _delete_after_delay(
                    edited_msg.chat.id,
                    edited_msg.id,
                    LuckyBoxConfig.AUTO_DELETE_RESULTS_DELAY_SECONDS,
                )
            )
        elif not was_pinned and LuckyBoxConfig.PINNED_MESSAGE_DELETE_DELAY > 0:
            asyncio.create_task(
                _delete_after_delay(
                    edited_msg.chat.id,
                    edited_msg.id,
                    LuckyBoxConfig.PINNED_MESSAGE_DELETE_DELAY,
                )
            )


@bot.on_message(filters.group & (filters.text | filters.command(LuckyBoxConfig.GAME_COMMANDS, prefixes=prefixes)), group=11)
async def trigger_lucky_box(_, message: Message):
    if not message.chat or message.chat.type not in {ChatType.GROUP, ChatType.SUPERGROUP}:
        return
    if _chat_matches_targets(message.chat, LuckyBoxConfig.DISABLED_LUCKY_BOX_CHATS):
        return
    allowed_ids = _allowed_group_ids()
    if not allowed_ids:
        return
    if not _chat_matches_targets(message.chat, group):
        return
    if not message.from_user or message.from_user.is_bot:
        return
    if _chat_matches_targets(message.chat, LuckyBoxConfig.DISABLED_FISHING_CHATS):
        return
    if message.text and message.text.startswith(tuple(prefixes)) and (not message.command or message.command[0] not in LuckyBoxConfig.GAME_COMMANDS):
        return
    if message.command and message.command[0] not in LuckyBoxConfig.GAME_COMMANDS:
        return

    user_id_str = str(message.from_user.id)
    today_str = date.today().isoformat()
    async with DATA_LOCK:
        user_data = USER_TRIGGER_DATA.get(user_id_str, {})
        if user_data.get("date") != today_str:
            user_data = {"date": today_str, "count": 0}
        if user_data.get("count", 0) >= LuckyBoxConfig.LUCKY_BOX_DAILY_LIMIT:
            return
        previous_user_data = user_data.copy()
        user_data["count"] += 1
        USER_TRIGGER_DATA[user_id_str] = user_data
        if not user_tracker_persistence.save(USER_TRIGGER_DATA):
            USER_TRIGGER_DATA[user_id_str] = previous_user_data
            LOGGER.warning(f"lucky box tracker save failed for user={message.from_user.id}")

        if random.random() >= LuckyBoxConfig.LUCKY_BOX_CHANCE:
            return

        user_mention = _mention_html(message.from_user.first_name, message.from_user.id)
        sent_message = await _send_html(
            message.chat.id,
            (
                "🎊 <b>从天而降的幸运！</b> 🎊\n\n"
                f"{user_mention} 的发言触发了一个神秘的幸运盲盒！\n\n"
                f"<b>只有你能领取哦！</b> ({LuckyBoxConfig.LUCKY_BOX_EXPIRE_TIME // 60}分钟内有效)"
            ),
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🎁 点击领取", callback_data=f"luckybox_personal_{message.from_user.id}")
            ]]),
        )
        if sent_message:
            asyncio.create_task(expire_lucky_box_message(sent_message, message.from_user.id))


@bot.on_callback_query(filters.regex(r"^luckybox_"))
async def claim_lucky_box(_, query):
    await _claim_lucky_box(_, query)


@bot.on_message(filters.command("sendbox", prefixes=prefixes))
async def send_lucky_box_manual(_, message: Message):
    admin_user = message.from_user
    if not admin_user:
        return

    chat_id = message.chat.id
    raw_parts = (message.text or "").split()
    is_admin = admin_user.id == owner or admin_user.id in admins
    if not is_admin and not (message.chat.type != ChatType.PRIVATE and await _is_group_admin(chat_id, admin_user.id)):
        await deleteMessage(message)
        await sendMessage(
            message,
            f"{_mention_html(admin_user.first_name, admin_user.id)} 您没有权限使用此命令。",
            parse_mode=ParseMode.HTML,
            send=True,
            chat_id=chat_id,
        )
        return

    await deleteMessage(message)

    quantity = 1
    if len(raw_parts) > 1:
        try:
            quantity = int(raw_parts[1])
            if not (1 <= quantity <= 10):
                await sendMessage(message, "派发失败：数量必须在 1 到 10 之间。", send=True, chat_id=chat_id)
                return
        except ValueError:
            await sendMessage(message, "派发失败：请输入有效的数字。例如 `/sendbox 5`。", send=True, chat_id=chat_id)
            return

    target_user = message.reply_to_message.from_user if message.reply_to_message and message.reply_to_message.from_user else None
    admin_name = admin_user.first_name

    for _ in range(quantity):
        if target_user:
            text_content = (
                f"🎊 <b>{_escape_html_text(admin_name)} 为 {_mention_html(target_user.first_name, target_user.id)} 派送的专属礼物！</b> 🎊\n\n"
                "一个神秘的幸运盲盒从天而降！\n\n"
                f"<b>只有你能领取哦！</b> ({LuckyBoxConfig.LUCKY_BOX_EXPIRE_TIME // 60}分钟内有效)"
            )
            callback_data = f"luckybox_personal_{target_user.id}"
            owner_id_for_expire = target_user.id
        else:
            text_content = (
                f"🎊 <b>{_escape_html_text(admin_name)} 派送的特别礼物！</b> 🎊\n\n"
                "一个神秘的幸运盲盒从天而降！\n\n"
                f"<b>这次是公共盲盒，先到先得！</b> ({LuckyBoxConfig.LUCKY_BOX_EXPIRE_TIME // 60}分钟内有效)"
            )
            callback_data = f"luckybox_public_{admin_user.id}"
            owner_id_for_expire = 0

        sent_message = await _send_html(
            chat_id,
            text_content,
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🎁 点击领取", callback_data=callback_data)
            ]]),
        )
        if sent_message:
            asyncio.create_task(expire_lucky_box_message(sent_message, owner_id_for_expire))
        if quantity > 1:
            await asyncio.sleep(0.5)

    if target_user:
        LOGGER.info(f"管理员 {admin_user.id} 在群组 {chat_id} 为用户 {target_user.id} 派发了 {quantity} 个专属幸运盲盒。")
    else:
        LOGGER.info(f"管理员 {admin_user.id} 在群组 {chat_id} 手动派发了 {quantity} 个公共幸运盲盒。")


@bot.on_message(filters.command("boxstock", prefixes=prefixes))
async def check_box_stock(_, message: Message):
    user_id = message.from_user.id if message.from_user else 0
    chat_id = message.chat.id
    is_admin = user_id == owner or user_id in admins
    if not is_admin and not (message.chat.type != ChatType.PRIVATE and await _is_group_admin(chat_id, user_id)):
        await deleteMessage(message)
        await sendMessage(
            message,
            f"{_mention_html(message.from_user.first_name, user_id)} 您没有权限使用此命令。",
            parse_mode=ParseMode.HTML,
            send=True,
            chat_id=chat_id,
        )
        return

    await deleteMessage(message)

    report_lines = ["<b>🎁 幸运盲盒奖品库存报告</b>\n"]
    async with DATA_LOCK:
        current_stock = CURRENT_PRIZE_STOCK.copy()

    active_pool = LuckyBoxConfig.LUCKY_BOX_PRIZES if LuckyBoxConfig.USE_LEGACY_PRIZE_POOL else LuckyBoxConfig.FISHABLE_ITEMS

    for prize in active_pool:
        stock_key = prize.get("stock_key")
        name = prize["name"]
        if stock_key:
            limit = LuckyBoxConfig.LIMIT_MAPPING.get(stock_key, -1)
            stock_now = current_stock.get(stock_key, -1)
            limit_str = "无限" if limit == -1 else str(limit)
            stock_now_str = "无限" if stock_now == -1 else str(stock_now)
        else:
            limit_str = "不受库存控制"
            stock_now_str = "不适用"
        report_lines.append(f"• <b>{_escape_html_text(name)}</b>:")
        report_lines.append(f"  - 总上限: {limit_str}")
        report_lines.append(f"  - 剩余库存: {stock_now_str}\n")

    report_lines.append("<pre>注意：此数据为持久化保存，重启不会重置。</pre>")
    report_text = "\n".join(report_lines)
    report_msg = await _send_html(chat_id, report_text)
    if report_msg and LuckyBoxConfig.BOX_STOCK_REPORT_DELETE_DELAY > 0:
        asyncio.create_task(
            _delete_after_delay(report_msg.chat.id, report_msg.id, LuckyBoxConfig.BOX_STOCK_REPORT_DELETE_DELAY)
        )
