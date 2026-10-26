"""受限 HTTPS 传输、频控和主机级断路器，支持抖动指数退避重试与半开探测自愈。"""

import random
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, Optional

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
HOST_MIN_INTERVAL_SECONDS = 0.5
BREAKER_FAILURE_THRESHOLD = 3
BREAKER_COOLDOWN_SECONDS = 60
BREAKER_MAX_COOLDOWN_SECONDS = 600
MAX_HTTP_RETRIES = 2
MAX_HTTP_RESPONSE_BYTES = 4 * 1024 * 1024

BREAKER_STATE_CLOSED = "closed"
BREAKER_STATE_OPEN = "open"
BREAKER_STATE_HALF_OPEN = "half_open"

ALLOWED_HTTP_HOSTS = {
    "smartbox.gtimg.cn", "qt.gtimg.cn", "web.ifzq.gtimg.cn",
    "push2ex.eastmoney.com", "push2.eastmoney.com", "push2his.eastmoney.com",
    "23.push2.eastmoney.com", "48.push2.eastmoney.com", "push2delay.eastmoney.com",
    "23.push2his.eastmoney.com", "48.push2his.eastmoney.com",
    "datacenter-web.eastmoney.com", "searchapi.eastmoney.com",
    "money.finance.sina.com.cn",
    "data.10jqka.com.cn", "d.10jqka.com.cn",
}

_HOST_LAST_REQUEST: Dict[str, float] = {}
_HOST_FAILURE_STATE: Dict[str, Dict[str, Any]] = {}
_TRANSPORT_LOCK = threading.Lock()


def _calculate_retry_backoff(attempt: int, base_delay: float = 0.5, max_delay: float = 1.2) -> float:
    """计算带随机抖动的指数退避时长，平滑请求峰值。"""
    delay = base_delay * (1.5 ** attempt)
    jitter = random.uniform(0.05, 0.25)
    return min(delay + jitter, max_delay)


def _throttle_host(hostname: str):
    """同主机频率限制。"""
    with _TRANSPORT_LOCK:
        last = _HOST_LAST_REQUEST.get(hostname, 0.0)
        wait = HOST_MIN_INTERVAL_SECONDS - (time.time() - last)
    if wait > 0:
        time.sleep(wait)
    with _TRANSPORT_LOCK:
        _HOST_LAST_REQUEST[hostname] = time.time()


def _breaker_check(hostname: str):
    """检查主机断路器状态：Open 时快速熔断，冷却期后进入 Half-Open 放行单次试探。"""
    with _TRANSPORT_LOCK:
        rec = _HOST_FAILURE_STATE.get(hostname)
        if not rec:
            return
        opened_at = rec.get("opened_at")
        state = rec.get("state", BREAKER_STATE_CLOSED)

        # 处于 Open 状态，判断是否已度过冷却期
        if opened_at is not None:
            cooldown = rec.get("cooldown", BREAKER_COOLDOWN_SECONDS)
            remaining = cooldown - (time.time() - opened_at)
            if remaining > 0:
                raise ConnectionError(f"上游 {hostname} 连续失败已熔断, 约{int(remaining)}秒后恢复探测")
            # 冷却到期，转入半开探测
            rec["state"] = BREAKER_STATE_HALF_OPEN
            rec["opened_at"] = None
            rec["half_open_in_flight"] = True
            return

        # 处于 Half-Open 状态，若已有探测在进行则拦截并发
        if state == BREAKER_STATE_HALF_OPEN:
            if rec.get("half_open_in_flight", False):
                raise ConnectionError(f"上游 {hostname} 处于半开试探状态, 正在等待健康检测结果")
            rec["half_open_in_flight"] = True


def _breaker_record_success(hostname: str):
    """调用成功：彻底重置该主机熔断状态。"""
    with _TRANSPORT_LOCK:
        _HOST_FAILURE_STATE.pop(hostname, None)


def _breaker_record_failure(hostname: str):
    """记录调用失败：支持半开试探失败重新熔断及连续失败触发熔断。"""
    with _TRANSPORT_LOCK:
        rec = _HOST_FAILURE_STATE.setdefault(hostname, {
            "count": 0,
            "opened_at": None,
            "state": BREAKER_STATE_CLOSED,
            "half_open_in_flight": False,
            "cooldown": BREAKER_COOLDOWN_SECONDS,
        })
        state = rec.get("state", BREAKER_STATE_CLOSED)

        # 半开探测失败：重新熔断，冷却期倍增（指数退避，最高至上限）
        if state == BREAKER_STATE_HALF_OPEN:
            current_cooldown = rec.get("cooldown", BREAKER_COOLDOWN_SECONDS)
            rec["cooldown"] = min(current_cooldown * 2, BREAKER_MAX_COOLDOWN_SECONDS)
            rec["opened_at"] = time.time()
            rec["state"] = BREAKER_STATE_OPEN
            rec["half_open_in_flight"] = False
            rec["count"] = BREAKER_FAILURE_THRESHOLD
            return

        if rec.get("opened_at") is not None:
            return

        rec["count"] = rec.get("count", 0) + 1
        if rec["count"] >= BREAKER_FAILURE_THRESHOLD:
            rec["opened_at"] = time.time()
            rec["state"] = BREAKER_STATE_OPEN
            rec["cooldown"] = BREAKER_COOLDOWN_SECONDS
            rec["half_open_in_flight"] = False


def _release_half_open_on_pass(hostname: str):
    """非 5xx HTTP 响应表明链路已连通，释放半开状态。"""
    with _TRANSPORT_LOCK:
        rec = _HOST_FAILURE_STATE.get(hostname)
        if rec and rec.get("state") == BREAKER_STATE_HALF_OPEN:
            _HOST_FAILURE_STATE.pop(hostname, None)


def http_get(url: str, timeout: int = 4, encoding: str = "utf-8") -> str:
    """受限安全 HTTP GET 请求：域名白名单、频控、断路器及带抖动指数退避重试。"""
    parsed = urllib.parse.urlparse(url)
    hostname = parsed.hostname
    if parsed.scheme != "https" or hostname not in ALLOWED_HTTP_HOSTS:
        raise ValueError("仅允许访问预设的 HTTPS 金融数据源")
    _breaker_check(hostname)
    _throttle_host(hostname)
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    last_exc: Optional[Exception] = None
    for attempt in range(MAX_HTTP_RETRIES):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                content = resp.read(MAX_HTTP_RESPONSE_BYTES + 1)
            if len(content) > MAX_HTTP_RESPONSE_BYTES:
                raise ValueError("上游响应超过大小限制")
            _breaker_record_success(hostname)
            try:
                return content.decode(encoding)
            except UnicodeDecodeError:
                return content.decode("gbk", errors="ignore")
        except urllib.error.HTTPError as exc:
            # 仅 5xx 计入熔断: 单端点 4xx (404/403 等) 不代表主机不可用, 不应殃及同主机其它工具
            if getattr(exc, "code", 0) >= 500:
                _breaker_record_failure(hostname)
            else:
                _release_half_open_on_pass(hostname)
            raise
        except Exception as exc:
            last_exc = exc
            if attempt < MAX_HTTP_RETRIES - 1:
                sleep_time = _calculate_retry_backoff(attempt)
                time.sleep(sleep_time)
    _breaker_record_failure(hostname)
    raise last_exc
