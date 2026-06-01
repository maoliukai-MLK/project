#!/usr/bin/env python3
"""
Stock Monitor v4 - 股票技术指标监控
数据源: 内置历史K线 + 新浪实时报价
指标: MACD / KDJ / RSI / BOLL / MA
信号: 金叉/死叉/超卖超买/布林触轨
"""

import json, os, time, sys, subprocess, urllib.request, urllib.error, pickle, signal
from datetime import datetime
from functools import wraps

ANSI = {"R":"\033[91m","G":"\033[92m","Y":"\033[93m","B":"\033[94m","C":"\033[96m",
        "BOLD":"\033[1m","Z":"\033[0m","GR":"\033[90m"}
BASE = os.path.dirname(os.path.abspath(__file__))
STATE = os.path.join(BASE, "state.json")
CONFIG = os.path.join(BASE, "config.json")
CACHE = os.path.join(BASE, "cache")

MAX_RETRIES = 3
RETRY_DELAY = 2
REQUEST_TIMEOUT = 15
API_INTERVAL = 0.5

_running = True

def _signal_handler(signum, frame):
    global _running
    _running = False
    print(f"\n{ANSI['Y']}收到退出信号，正在停止...{ANSI['Z']}")

signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)

def retry(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        last_err = None
        for attempt in range(1, MAX_RETRIES + 1):
            if not _running:
                raise SystemExit("被用户中断")
            try:
                return func(*args, **kwargs)
            except (urllib.error.URLError, urllib.error.HTTPError,
                    OSError, ValueError) as e:
                last_err = e
                if attempt < MAX_RETRIES:
                    delay = RETRY_DELAY * attempt
                    print(f" ⚡重试({attempt}/{MAX_RETRIES}) {delay}s", end="", flush=True)
                    time.sleep(delay)
        raise last_err
    return wrapper

# ═══════════════════ 内置历史K线 ═══════════════════
# 格式: {code: {"close":[...], "high":[...], "low":[...]}}
KLINE = {
    "002334": {
        "close": [10.01,9.59,9.59,9.85,9.91,9.80,10.02,9.87,9.98,9.91,10.09,9.95,9.80,9.31,9.42,9.67,10.13,9.99,
                  10.11,10.22,10.06,9.80,8.81,8.67,8.88,8.58,8.32,7.85,8.07,8.41,8.16,8.26,8.22,7.96,8.14,7.95,
                  7.84,7.83,8.23,8.12,8.27,8.37,8.64,8.51,8.95,8.33,8.37,8.04,8.10,7.90,7.79,7.85,7.65,7.71,7.74,
                  7.85,7.89,8.26,9.09,9.02,9.11,8.78,8.98,9.20,9.23,9.09,8.70,9.00,9.34,9.05,9.12,9.10,8.87],
        "high": [10.01,9.83,9.71,9.91,10.12,9.95,10.04,10.07,10.05,10.00,10.22,10.05,9.94,9.93,9.61,10.38,10.03,
                 10.18,10.29,10.26,10.06,9.06,9.61,9.00,8.89,8.78,8.65,8.31,8.08,8.48,8.39,8.27,8.22,8.21,8.16,8.16,
                 7.99,7.87,8.24,8.18,8.40,8.43,8.93,8.68,9.10,8.57,8.52,8.34,8.22,8.10,7.87,7.85,7.80,7.74,7.77,
                 7.92,7.90,8.54,9.09,9.21,9.30,9.16,9.10,9.33,9.32,9.18,9.16,9.06,9.51,9.25,9.39,9.24,9.19],
        "low":  [9.01,9.55,9.49,9.64,9.72,9.78,9.76,9.87,9.84,9.86,9.86,9.90,9.63,9.24,9.09,9.59,9.71,10.02,10.10,
                 10.00,9.77,8.81,8.81,8.63,8.59,8.54,8.31,7.78,7.83,8.12,8.12,8.01,8.02,7.95,8.07,7.88,7.78,7.77,
                 8.07,8.09,8.21,8.16,8.35,8.47,8.46,8.05,8.22,7.99,7.89,7.84,7.72,7.66,7.62,7.60,7.69,7.79,7.82,
                 7.82,8.78,8.92,8.76,8.78,8.58,8.79,9.05,8.93,8.64,8.59,8.93,8.91,8.91,8.96,8.81],
    },
    "301007": {
        "close": [37.02,38.50,38.09,37.63,37.38,37.85,38.17,37.99,38.33,37.60,37.91,39.59,39.69,38.58,36.70,34.31,
                  34.68,34.83,36.15,35.23,36.12,35.58,35.28,34.30,34.35,33.73,35.21,34.16,33.10,30.81,32.36,33.31,
                  32.44,33.30,33.43,33.61,34.53,33.80,34.03,33.84,35.65,35.72,36.56,35.53,35.53,35.21,35.15,35.23,
                  35.35,35.28,35.83,35.97,36.25,36.30,34.56,35.60,36.10,36.12,36.99,37.65,38.88,40.50,40.25,42.79,
                  44.50,43.26,43.13,41.81,41.17,41.50,41.06,39.39,37.33,37.73,35.50],
        "high": [38.09,38.80,38.58,38.43,38.01,38.09,38.50,38.88,39.66,38.38,38.38,39.74,39.94,39.71,38.29,37.08,
                 35.43,35.56,36.87,35.74,36.21,36.49,36.93,35.27,34.68,34.58,35.43,35.20,34.50,32.65,32.48,33.75,
                 33.39,33.79,33.80,34.45,36.00,35.10,35.00,34.30,35.70,35.98,37.35,36.28,36.10,36.45,35.74,35.40,
                 35.43,36.15,36.05,37.46,36.48,36.81,36.40,36.11,36.33,36.58,38.66,37.96,39.00,41.95,40.90,43.74,
                 45.99,44.50,44.88,43.13,43.97,42.22,42.33,41.88,39.47,38.19,37.76],
        "low":  [37.01,37.07,37.65,37.35,37.03,37.29,37.66,37.82,37.93,37.52,37.41,37.68,39.01,38.38,36.50,34.12,
                 33.80,34.58,34.60,34.24,35.45,35.50,35.10,34.23,33.53,33.64,33.52,33.98,33.00,30.55,30.85,32.37,
                 32.39,31.57,32.00,33.29,33.80,33.68,33.61,33.51,34.51,35.00,35.73,35.26,35.20,35.10,34.83,34.30,
                 34.56,34.90,34.75,35.09,35.18,35.80,34.51,34.40,35.41,35.73,36.12,37.02,37.20,38.93,39.50,39.64,
                 41.67,42.91,42.65,41.41,40.99,41.11,40.36,38.08,37.18,36.48,35.48],
    },
}

# ═══════════════════ 技术指标 ═══════════════════
def ema(d, p):
    r, k = [d[0]], 2/(p+1)
    for i in range(1, len(d)): r.append(d[i]*k + r[-1]*(1-k))
    return r

def calc_macd(c):
    e12, e26 = ema(c, 12), ema(c, 26)
    dif = [e12[i]-e26[i] for i in range(len(c))]
    dea = ema(dif, 9)
    return dif, dea, [(dif[i]-dea[i])*2 for i in range(len(c))]

def calc_kdj(h, l, c):
    n = len(c)
    l9 = [min(l[max(0,i-8):i+1]) for i in range(n)]
    h9 = [max(h[max(0,i-8):i+1]) for i in range(n)]
    rsv = [(c[i]-l9[i])/(h9[i]-l9[i])*100 if h9[i]!=l9[i] else 50 for i in range(n)]
    k, d, j = [50]*n, [50]*n, [50]*n
    for i in range(1, n):
        k[i]=2/3*k[i-1]+1/3*rsv[i]; d[i]=2/3*d[i-1]+1/3*k[i]; j[i]=3*k[i]-2*d[i]
    return k, d, j

def calc_rsi(c, p=14):
    n = len(c)
    r = [50]*n
    for i in range(p, n):
        g = l = 0
        for t in range(i-p+1, i+1):
            ch = c[t]-c[t-1]
            if ch>0: g+=ch
            else: l-=ch
        ag, al = g/p, l/p
        r[i] = 100-100/(1+ag/al) if al else 100
    return r

def calc_boll(c, p=20):
    n = len(c)
    mid, up, dn = [None]*n, [None]*n, [None]*n
    for i in range(p-1, n):
        m = sum(c[i-p+1:i+1])/p
        s = (sum((c[t]-m)**2 for t in range(i-p+1,i+1))/p)**0.5
        mid[i], up[i], dn[i] = m, m+2*s, m-2*s
    return mid, up, dn

def calc_ma(d, p):
    return [sum(d[max(0,i-p+1):i+1])/min(i+1,p) for i in range(len(d))]

# ═══════════════════ 数据获取 ═══════════════════
def _sina_req(url):
    req = urllib.request.Request(url, headers={"Referer": "https://finance.sina.com.cn"})
    with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as r:
        return r.read().decode("gbk")

@retry
def get_price(code):
    """新浪实时行情"""
    prefix = "sz" if code.startswith("00") or code.startswith("30") else "sh"
    text = _sina_req(f"https://hq.sinajs.cn/list={prefix}{code}")
    parts = text.split('"')[1].split(",")
    if len(parts) < 4:
        raise ValueError(f"API返回格式异常: {text[:80]}")
    return float(parts[3])

@retry
def get_us_prices(tickers):
    """新浪美股实时行情 - 按固定顺序匹配"""
    sym = ",".join(f"gb_{t.lower()}" for t in tickers)
    text = _sina_req(f"https://hq.sinajs.cn/list={sym}")
    prices = {}
    lines = text.strip().split("\n")
    for i, line in enumerate(lines):
        if i >= len(tickers): break
        try:
            parts = line.split('"')
            if len(parts) < 2: continue
            fields = parts[1].split(",")
            if len(fields) < 2: continue
            prices[tickers[i]] = float(fields[1])
        except (ValueError, IndexError):
            pass
    if not prices:
        raise ValueError("美股API返回数据为空")
    return prices

@retry
def get_hk_price(code):
    """新浪港股实时行情,字段[6]为当前价"""
    text = _sina_req(f"https://hq.sinajs.cn/list=rt_hk{code}")
    parts = text.split('"')[1].split(",")
    if len(parts) < 7:
        raise ValueError(f"港股API返回格式异常: {text[:80]}")
    name = parts[1]
    price = float(parts[6])
    return name, price

def _yf_kline(symbol, period="6mo"):
    """通过 yfinance 拉取历史K线,返回 (c,h,l) 三个 list"""
    try:
        import yfinance as yf
    except ImportError:
        raise ValueError("需要 yfinance 库: pip install yfinance")
    df = yf.Ticker(symbol).history(period=period, auto_adjust=True)
    if df.empty:
        raise ValueError(f"yfinance 无数据: {symbol}")
    n = len(df)
    return ([float(df["Close"].iloc[i]) for i in range(n)],
            [float(df["High"].iloc[i]) for i in range(n)],
            [float(df["Low"].iloc[i]) for i in range(n)])

def get_kline(market, code):
    """获取历史K线: A股优先用内置 KLINE dict,否则 yfinance 兜底"""
    if market == "hs":
        k = KLINE.get(code)
        if k:
            c = [k["close"][0]]*30 + list(k["close"])
            h = [k["high"][0]]*30 + list(k["high"])
            l = [k["low"][0]]*30 + list(k["low"])
            return c, h, l
        sym = (("sz" if code.startswith("00") or code.startswith("30") else "sh") + code)
    elif market == "us":
        sym = code
    elif market == "hk":
        sym = f"{int(code):04d}.HK"
    else:
        raise ValueError(f"暂不支持的市场: {market}")
    c, h, l = _yf_kline(sym)
    return ([c[0]]*30 + c, [h[0]]*30 + h, [l[0]]*30 + l)

def quote(market, code):
    """统一行情入口: market in {hs, us, hk},返回 {price, name, last, signals}"""
    prev = load_state()
    cfg = json.load(open(CONFIG)) if os.path.exists(CONFIG) else {}
    if market == "hs":
        info = cfg.get("a_shares", {}).get(code, {})
        name = info.get("name", code)
        c, h, l = get_kline("hs", code)
        p = get_price(code)
    elif market == "us":
        info = cfg.get("us_shares", {}).get(code, {})
        name = info.get("name", code)
        c, h, l = get_kline("us", code)
        p_dict = get_us_prices([code])
        p = p_dict.get(code, c[-1])
    elif market == "hk":
        name, p = get_hk_price(code)
        c, h, l = get_kline("hk", code)
    else:
        raise ValueError(f"未知市场: {market}")
    c[-1] = p
    sigs, last = detect(c, h, l, prev.get(code, {}))
    return {"market": market, "code": code, "name": name,
            "price": p, "last": last, "signals": sigs}

# ═══════════════════ 信号检测 ═══════════════════
def detect(c, h, l, prev):
    """计算所有指标并检测信号"""
    dif, dea, mh = calc_macd(c)
    k, d, j = calc_kdj(h, l, c)
    rsi = calc_rsi(c)
    mid, up, dn = calc_boll(c)
    ma5 = calc_ma(c, 5)
    ma20 = calc_ma(c, 20)
    price = round(c[-1], 2)

    last = dict(price=price, date=datetime.now().strftime("%Y-%m-%d"),
                dif=round(dif[-1],3), dea=round(dea[-1],3), macd=round(mh[-1],3),
                k=round(k[-1],1), d=round(d[-1],1), j=round(j[-1],1),
                rsi=round(rsi[-1],1), ma5=round(ma5[-1],2),
                ma20=round(ma20[-1],2) if ma20[-1] else None,
                boll_up=round(up[-1],2) if up[-1] else None,
                boll_mid=round(mid[-1],2) if mid[-1] else None,
                boll_dn=round(dn[-1],2) if dn[-1] else None)

    sigs = []
    if not prev or "dif" not in prev:
        return sigs, last

    # MACD金叉/死叉
    if dif[-2] <= dea[-2] and dif[-1] > dea[-1]:
        sigs.append(("MACD金叉 ↑", "BUY", f"DIF↑DEA"))
    if dif[-2] >= dea[-2] and dif[-1] < dea[-1]:
        sigs.append(("MACD死叉 ↓", "SELL", f"DIF↓DEA"))
    # KDJ金叉/死叉
    if k[-2] <= d[-2] and k[-1] > d[-1] and k[-1] < 80:
        sigs.append(("KDJ金叉 ↑", "BUY", f"K{k[-2]:.0f}→K{k[-1]:.0f}"))
    if k[-2] >= d[-2] and k[-1] < d[-1] and k[-1] > 20:
        sigs.append(("KDJ死叉 ↓", "SELL", f"K{k[-2]:.0f}→K{k[-1]:.0f}"))
    # MA金叉/死叉
    if ma20[-1] and ma20[-2] and ma5[-1] and ma20[-1] and ma20[-2]:
        if ma5[-2] <= ma20[-2] and ma5[-1] > ma20[-1]:
            sigs.append(("MA金叉 ↑", "BUY", f"MA5{ma5[-2]:.2f}↑MA20"))
        if ma5[-2] >= ma20[-2] and ma5[-1] < ma20[-1]:
            sigs.append(("MA死叉 ↓", "SELL", f"MA5{ma5[-2]:.2f}↓MA20"))
    # KDJ超卖/超买
    if k[-1] < 20 and prev.get("k", 100) >= 20:
        sigs.append(("KDJ超卖 ⚠", "WARN", f"K={k[-1]:.1f}<20"))
    if k[-1] > 80 and prev.get("k", 0) <= 80:
        sigs.append(("KDJ超买 ⚠", "WARN", f"K={k[-1]:.1f}>80"))
    # RSI超卖/超买
    if rsi[-1] < 30 and prev.get("rsi", 100) >= 30:
        sigs.append(("RSI超卖 ⚠", "WARN", f"RSI={rsi[-1]:.1f}<30"))
    if rsi[-1] > 70 and prev.get("rsi", 0) <= 70:
        sigs.append(("RSI超买 ⚠", "WARN", f"RSI={rsi[-1]:.1f}>70"))
    # 布林触轨
    if dn[-1] and price <= round(dn[-1]+0.01, 2) and prev.get("boll_dn", 0) < price:
        sigs.append(("触布林下轨", "WARN", f"价{price:.2f}≈下轨{dn[-1]:.2f}"))
    if up[-1] and price >= round(up[-1]-0.01, 2) and prev.get("boll_up", 999) > price:
        sigs.append(("触布林上轨", "WARN", f"价{price:.2f}≈上轨{up[-1]:.2f}"))

    return sigs, last


# ═══════════════════ 显示 ═══════════════════
def show(results, all_sigs):
    t = datetime.now().strftime("%Y-%m-%d %H:%M")
    print(f"\n{'='*100}")
    print(f"  股票监控  {t}")
    print(f"{'='*100}")

    for market, stocks in [("A股", results.get("a",{})), ("美股", results.get("us",{}))]:
        if not stocks: continue
        print(f"\n{'──'} {market}")
        for code, info in stocks.items():
            L = info["last"]
            sigs = info["signals"]
            pl = info.get("pl", {})
            ps = ""
            if pl:
                c = ANSI["G"] if pl["p_pct"]>=0 else ANSI["R"]
                ps = f" {c}{pl['s']}股@{pl['c']:.2f} {'盈' if pl['p_pct']>=0 else '亏'}${abs(pl['p_amt']):.0f}({pl['p_pct']:+.1f}%){ANSI['Z']}"

            m = f"{'🟢' if L['macd']>0 else '🔴'}DIF{L['dif']:.3f} DEA{L['dea']:.3f} {'红柱' if L['macd']>0 else'绿柱'}{abs(L['macd']):.3f}"
            ks = f"{'🟢' if L['k']>L['d'] else '🔴'}K{L['k']:.1f} D{L['d']:.1f} J{L['j']:.1f}"
            if L['k']<20: ks += " ⚠超卖"
            elif L['k']>80: ks += " ⚠超买"
            ms = f"MA5={L['ma5']:.2f} MA20={L['ma20'] or 'N/A'}"
            bs = f"布林:[{L.get('boll_up','N/A')}|{L.get('boll_mid','N/A')}|{L.get('boll_dn','N/A')}]" if L.get('boll_dn') else ""

            print(f"\n{'■' if market=='A股' else '●'} {code:<6} {info['name']:<16}  {L['price']:>8.2f}  {ps}")
            print(f"  {m}")
            print(f"  {ks} | RSI={L['rsi']:.1f} | {ms}")
            if bs: print(f"  {bs}")

            for sn, ac, desc in sigs:
                ac_c = ANSI["G"] if ac=="BUY" else ANSI["R"] if ac=="SELL" else ANSI["Y"]
                ic = "🟢" if ac=="BUY" else "🔴" if ac=="SELL" else "⚠️"
                print(f"  {ac_c}{ic} [{ac}] {sn}: {desc}{ANSI['Z']}")

    if all_sigs:
        print(f"\n{'!'*60}")
        print(f"  新信号 {len(all_sigs)} 条")
        for c, n, (sn,_,_) in all_sigs:
            print(f"  {c:>6} {n:<14} {sn}")
        print(f"{'!'*60}")
    else:
        print(f"\n{'─'*40}\n  无新信号触发\n{'─'*40}")


# ═══════════════════ 主控 ═══════════════════
def load_state():
    return json.load(open(STATE)) if os.path.exists(STATE) else {}
def save_state(st):
    json.dump(st, open(STATE,"w"), indent=2, ensure_ascii=False)

def main(notify=False):
    cfg = json.load(open(CONFIG))
    prev = load_state()
    results = {"a":{}, "us":{}}
    all_sigs = []

    # A股
    for code, info in cfg.get("a_shares",{}).items():
        print(f"\n{info['name']}({code})...", end="", flush=True)
        try:
            k = KLINE.get(code)
            if not k: raise ValueError("无内置数据")
            # 前导补齐+最新价替换
            c = [k["close"][0]]*30 + list(k["close"])
            h = [k["high"][0]]*30 + list(k["high"])
            l = [k["low"][0]]*30 + list(k["low"])
            p = get_price(code)
            c[-1] = p

            sigs, last = detect(c, h, l, prev.get(code, {}))
            pl = {}
            if info.get("cost"):
                p_pct = (p-info["cost"])/info["cost"]*100
                pl = {"s":info["shares"], "c":info["cost"],
                      "p_pct":p_pct, "p_amt":info["shares"]*(p-info["cost"])}
            results["a"][code] = {"name":info["name"], "last":last, "signals":sigs, "pl":pl}
            if sigs: all_sigs.append((code, info["name"], sigs[0]))
            print(f" ¥{p:.2f}", end="")
            if sigs: print(f" {ANSI['R']}{len(sigs)}信号{ANSI['Z']}", end="")
        except Exception as e:
            print(f" ✗{e}", end="")

    # 美股
    us_tickers = list(cfg.get("us_shares",{}).keys())
    if us_tickers:
        time.sleep(3)
        print(f"\n美股...", end="", flush=True)
        try:
            up = get_us_prices(us_tickers)
            # 用内置K线或yfinance缓存
            for ticker in us_tickers:
                cache_f = os.path.join(BASE, "cache", f"us_{ticker}.pkl")
                if os.path.exists(cache_f):
                    import pickle
                    with open(cache_f,"rb") as f:
                        k = pickle.load(f)
                    c = list(k["c"]); h = list(k["h"]); l = list(k["l"])
                else:
                    import yfinance as yf
                    s = yf.Ticker(ticker)
                    df = s.history(period="6mo", auto_adjust=True)
                    if df.empty: continue
                    c = [float(df["Close"].iloc[i]) for i in range(len(df))]
                    h = [float(df["High"].iloc[i]) for i in range(len(df))]
                    l = [float(df["Low"].iloc[i]) for i in range(len(df))]
                    os.makedirs(os.path.join(BASE,"cache"), exist_ok=True)
                    import pickle
                    with open(cache_f,"wb") as f:
                        pickle.dump({"c":c,"h":h,"l":l}, f)
                    time.sleep(5)

                p = up.get(ticker, c[-1])
                c_pre = [c[0]]*30 + c; h_pre = [h[0]]*30 + h; l_pre = [l[0]]*30 + l
                c_pre[-1] = p

                sigs, last = detect(c_pre, h_pre, l_pre, prev.get(ticker, {}))
                pl = {}
                info = cfg["us_shares"][ticker]
                if info.get("cost"):
                    p_pct = (p-info["cost"])/info["cost"]*100
                    pl = {"s":info["shares"], "c":info["cost"],
                          "p_pct":p_pct, "p_amt":info["shares"]*(p-info["cost"])}
                results["us"][ticker] = {"name":info["name"], "last":last, "signals":sigs, "pl":pl}
                if sigs: all_sigs.append((ticker, info["name"], sigs[0]))
                print(f" ${p:.2f}", end="")
                if sigs: print(f" {ANSI['R']}{len(sigs)}信号{ANSI['Z']}", end="")
        except Exception as e:
            print(f" ✗{e}", end="")

    # 保存状态
    st = {}
    for mk in results:
        for code, info in results[mk].items():
            st[code] = info["last"]
    save_state(st)

    show(results, all_sigs)

    if notify and all_sigs:
        for code, name, (sn,_,_) in all_sigs:
            try:
                subprocess.run(["osascript","-e",
                    f'display notification "{code} {name}: {sn}" with title "📊 股票信号"'
                ], capture_output=True, timeout=5)
            except: pass

if __name__ == "__main__":
    main(notify="--notify" in sys.argv)
