"""轻量内存 TTL 缓存，具备容量上限控制、主动过期淘汰与线程安全防护。"""

import threading
import time
from datetime import datetime
from typing import Any, Dict, Optional

CACHE_TTL_SECONDS = 180
HISTORICAL_CACHE_TTL_SECONDS = 86400
MAX_CACHE_ENTRIES = 1000
CLEANUP_INTERVAL_WRITES = 50

CACHE_STORE: Dict[str, Dict[str, Any]] = {}
_CACHE_LOCK = threading.Lock()
_WRITE_COUNTER = 0


def cleanup_expired_cache(now: Optional[float] = None) -> int:
    """主动清理所有已过期的缓存条目，返回清理条目数。"""
    if now is None:
        now = time.time()
    with _CACHE_LOCK:
        expired_keys = [
            k for k, v in CACHE_STORE.items()
            if (now - v.get("time", 0.0)) >= v.get("ttl", CACHE_TTL_SECONDS)
        ]
        for k in expired_keys:
            CACHE_STORE.pop(k, None)
        return len(expired_keys)


def _evict_lru_entries_locked(target_size: int):
    """当缓存超出上限时淘汰最久未访问的条目（必须在持有 _CACHE_LOCK 时调用）。"""
    if len(CACHE_STORE) <= target_size:
        return
    # 按照 last_accessed 排序，若未记录则回退到 time
    sorted_items = sorted(
        CACHE_STORE.items(),
        key=lambda item: item[1].get("last_accessed", item[1].get("time", 0.0))
    )
    excess = len(CACHE_STORE) - target_size
    for k, _ in sorted_items[:excess]:
        CACHE_STORE.pop(k, None)


def get_cached(key: str) -> Optional[Any]:
    """读取缓存；若过期或不存在返回 None。"""
    now = time.time()
    with _CACHE_LOCK:
        record = CACHE_STORE.get(key)
        if not record:
            return None
        if now - record.get("time", 0.0) < record.get("ttl", CACHE_TTL_SECONDS):
            record["last_accessed"] = now
            return record.get("data")
        CACHE_STORE.pop(key, None)
        return None


def set_cached(key: str, data: Any, ttl: int = CACHE_TTL_SECONDS):
    """写入缓存；周期性主动清理过期条目，并执行最大容量 LRU 驱逐。"""
    global _WRITE_COUNTER
    now = time.time()
    with _CACHE_LOCK:
        _WRITE_COUNTER += 1
        # 达到写入周期或容量即将超标时，执行主动清扫
        if _WRITE_COUNTER >= CLEANUP_INTERVAL_WRITES or len(CACHE_STORE) >= MAX_CACHE_ENTRIES:
            _WRITE_COUNTER = 0
            expired_keys = [
                k for k, v in CACHE_STORE.items()
                if (now - v.get("time", 0.0)) >= v.get("ttl", CACHE_TTL_SECONDS)
            ]
            for k in expired_keys:
                CACHE_STORE.pop(k, None)

            # 清扫后若容量依然触碰或超过上限，则淘汰最老条目以腾出安全余量
            if len(CACHE_STORE) >= MAX_CACHE_ENTRIES:
                _evict_lru_entries_locked(MAX_CACHE_ENTRIES - 1)

        CACHE_STORE[key] = {
            "data": data,
            "time": now,
            "ttl": ttl,
            "last_accessed": now,
        }


def ttl_for_history(latest_date_str: Optional[str]) -> int:
    """根据数据日期确定 TTL：收盘历史定格数据采用长缓存，当日或未注明采用短缓存。"""
    try:
        if latest_date_str and str(latest_date_str)[:10] < datetime.now().strftime("%Y-%m-%d"):
            return HISTORICAL_CACHE_TTL_SECONDS
    except Exception:
        pass
    return CACHE_TTL_SECONDS
