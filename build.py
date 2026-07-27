# -*- coding: utf-8 -*-
"""
build.py — سازنده‌ی ابری کانفیگ (روی GitHub Actions اجرا می‌شود، نه روی PC تو)

کار:
  1) از همه‌ی منابع (تلگرام + ساب‌ها) کانفیگ می‌گیرد — بدون محدودیت (پهنای‌باندِ گیت‌هاب).
  2) تکراری‌ها را حذف و به پروفایل فیلتر می‌کند.
  3) کشورِ سرورِ هر کانفیگ را با GeoIP پیدا می‌کند.
  4) یک کانفیگِ sing-box می‌سازد با «گروهِ خودکار برای هر کشور» (urltest):
        - داخلِ هر کشور، بهترین کانفیگ خودکار انتخاب و در صورت افتادن، سریع failover.
        - یک انتخابگرِ بالادست تا کشور را تو انتخاب کنی (کشور ثابت می‌ماند).
  5) خروجی: sub.txt (ساب معمولی) + singbox.json (کانفیگِ گروه‌بندی‌شده) در همین repo.

PC تو فقط singbox.json را در hiddify import می‌کند — یک ورودیِ ثابت، failover خودکار،
و ping‌های محلیِ ریز. مصرفِ خارجیِ تو ~صفر.
"""
import os, re, ssl, json, gzip, time, base64, socket, urllib.request, urllib.parse
from concurrent.futures import ThreadPoolExecutor

# ---- منابع (هرچه بیشتر بهتر — روی گیت‌هاب محدودیت نداریم) ----
CHANNELS = [
    "filembad","v2ray_configs_pool","VlessConfig","PrivateVPNs","ShadowProxy66",
    "DirectVPN","VmessProtocol","customv2ray","v2rayNG_VPNN","free_v2rayyy",
    "config_v2ray","v2rayng_fa2","proxystore11","napsternetv_config","vpnfail_v2ray",
    "ARV2RAY","v2rayng_org","vmess_vless_v2rayng","iSegaro","v2rayng_vpnrog",
    "v2rayngvpn","vpnmasi","vmess_iran","v2rayng_config_free","meli_proxyy",
    "s_v2ray","configV2rayForFree","v2ray_vpn_ir","FreakConfig","PROXY_KHUNE",
    "freevpnhomes4","flyv2ray","expressvpn_ir","Outline_ir","vmessorg","v2rayngrit",
    "lonup_m","shadowsockskeys","prrofile_purple","mftizi","v2rayvpnpro","sinavm",
    "servermtm","free4allVPN","vip_vpn_2022","VPN_443","proxymetashdetm","canfigvps",
    "v2rayng_config","mahsaamneh","daorg","ShadowsocksM",
]
SUB_URLS = [
    "https://raw.githubusercontent.com/barry-far/V2ray-Config/main/All_Configs_Sub.txt",
    "https://raw.githubusercontent.com/Epodonios/v2ray-configs/main/All_Configs_Sub.txt",
    "https://raw.githubusercontent.com/mahdibland/V2RayAggregator/master/sub/sub_merge.txt",
    "https://raw.githubusercontent.com/mahdibland/ShadowsocksAggregator/master/Eternity.txt",
    "https://raw.githubusercontent.com/MhdiTaheri/V2rayCollector/main/sub/mix",
    "https://raw.githubusercontent.com/ALIILAPRO/v2rayNG-Config/main/server.txt",
    "https://raw.githubusercontent.com/Pawdroid/Free-servers/main/sub",
    "https://raw.githubusercontent.com/mheidari98/.proxy/main/all",
    "https://raw.githubusercontent.com/Kwinshadow/TelegramV2rayCollector/main/sublinks/mix.txt",
    "https://raw.githubusercontent.com/ripaojiedian/freenode/main/sub",
    "https://raw.githubusercontent.com/Leon406/SubCrawler/main/sub/share/all",
    "https://raw.githubusercontent.com/ermaozi/get_subscribe/main/subscribe/v2ray.txt",
    "https://raw.githubusercontent.com/peasoft/NoMoreWalls/master/list.txt",
    "https://raw.githubusercontent.com/Surfboardv2ray/TGParse/main/python/merged_configs.txt",
    "https://raw.githubusercontent.com/soroushmirzaei/telegram-configs-collector/main/protocols/vless",
    "https://raw.githubusercontent.com/soroushmirzaei/telegram-configs-collector/main/protocols/trojan",
    "https://raw.githubusercontent.com/soroushmirzaei/telegram-configs-collector/main/protocols/vmess",
    "https://raw.githubusercontent.com/MatinGhanbari/v2ray-configs/main/subscriptions/xray/base64/mix",
    "https://raw.githubusercontent.com/barry-far/V2ray-Config/main/Splitted-By-Protocol/vless.txt",
    "https://raw.githubusercontent.com/NiREvil/vless/main/sub/wildcard",
    "https://raw.githubusercontent.com/w1770946466/Auto_proxy/main/Long_term_subscription_num",
    "https://raw.githubusercontent.com/Leon406/SubCrawler/main/sub/share/vless",
    "https://raw.githubusercontent.com/Leon406/SubCrawler/main/sub/share/tr",
    "https://raw.githubusercontent.com/mfuu/v2ray/master/merge/all.txt",
    "https://raw.githubusercontent.com/mahdibland/V2RayAggregator/master/sub/sub_merge_yaml.yml",
    # ── منابعِ بیشتر (هرچه پکیج بزرگ‌تر، خروجیِ تأییدشده بیشتر) ──
    "https://raw.githubusercontent.com/Epodonios/v2ray-configs/main/Splitted-By-Protocol/vless.txt",
    "https://raw.githubusercontent.com/Epodonios/v2ray-configs/main/Splitted-By-Protocol/vmess.txt",
    "https://raw.githubusercontent.com/Epodonios/v2ray-configs/main/Splitted-By-Protocol/trojan.txt",
    "https://raw.githubusercontent.com/barry-far/V2ray-Config/main/Splitted-By-Protocol/vmess.txt",
    "https://raw.githubusercontent.com/barry-far/V2ray-Config/main/Splitted-By-Protocol/trojan.txt",
    "https://raw.githubusercontent.com/barry-far/V2ray-Config/main/Splitted-By-Protocol/ss.txt",
    "https://raw.githubusercontent.com/soroushmirzaei/telegram-configs-collector/main/protocols/shadowsocks",
    "https://raw.githubusercontent.com/soroushmirzaei/telegram-configs-collector/main/protocols/reality",
    "https://raw.githubusercontent.com/Kwinshadow/TelegramV2rayCollector/main/sublinks/vless.txt",
    "https://raw.githubusercontent.com/Kwinshadow/TelegramV2rayCollector/main/sublinks/trojan.txt",
    "https://raw.githubusercontent.com/Leon406/SubCrawler/main/sub/share/ss",
    "https://raw.githubusercontent.com/Leon406/SubCrawler/main/sub/share/v2",
    "https://raw.githubusercontent.com/mfuu/v2ray/master/v2ray",
    "https://raw.githubusercontent.com/aiboboxx/v2rayfree/main/v2",
    "https://raw.githubusercontent.com/freefq/free/master/v2",
    "https://raw.githubusercontent.com/vpei/Free-Node-Merge/main/o/node.txt",
    "https://raw.githubusercontent.com/Vauth/node/main/Main",
    "https://raw.githubusercontent.com/ZywChannel/free/main/sub",
    "https://raw.githubusercontent.com/mianfeifq/share/main/data2024118.txt",
    # ── مخصوصِ hysteria2 / tuic (UDP-محور: بهترین برای بازی؛ الان فقط ۱ تا داشتیم) ──
    "https://raw.githubusercontent.com/soroushmirzaei/telegram-configs-collector/main/protocols/hysteria",
    "https://raw.githubusercontent.com/soroushmirzaei/telegram-configs-collector/main/protocols/tuic",
    "https://raw.githubusercontent.com/barry-far/V2ray-Config/main/Splitted-By-Protocol/hysteria.txt",
    "https://raw.githubusercontent.com/barry-far/V2ray-Config/main/Splitted-By-Protocol/tuic.txt",
    "https://raw.githubusercontent.com/Epodonios/v2ray-configs/main/Splitted-By-Protocol/hysteria2.txt",
    "https://raw.githubusercontent.com/Epodonios/v2ray-configs/main/Splitted-By-Protocol/tuic.txt",
    "https://raw.githubusercontent.com/mheidari98/.proxy/main/hysteria2",
    "https://raw.githubusercontent.com/mheidari98/.proxy/main/tuic",
    "https://raw.githubusercontent.com/Surfboardv2ray/TGParse/main/splitted/hysteria2",
    "https://raw.githubusercontent.com/Surfboardv2ray/TGParse/main/splitted/tuic",
    # ── منابعِ تازه (۲۰۲۶-۰۷-۲۷، همه با curl تست شدند و واقعاً کانفیگ می‌دهند) ──
    "https://raw.githubusercontent.com/ebrasha/free-v2ray-public-list/main/V2Ray-Config-By-EbraSha-All-Type.txt",
    "https://raw.githubusercontent.com/ebrasha/free-v2ray-public-list/main/all_extracted_configs.txt",
    "https://raw.githubusercontent.com/ebrasha/free-v2ray-public-list/main/vless_configs.txt",
    "https://raw.githubusercontent.com/ebrasha/free-v2ray-public-list/main/trojan_configs.txt",
    "https://raw.githubusercontent.com/ebrasha/free-v2ray-public-list/main/vmess_configs.txt",
    "https://raw.githubusercontent.com/ebrasha/free-v2ray-public-list/main/ss_configs.txt",
    "https://raw.githubusercontent.com/4n0nymou3/multi-proxy-config-fetcher/main/configs/proxy_configs.txt",
    "https://raw.githubusercontent.com/Surfboardv2ray/Proxy-sorter/main/submerge/converted.txt",
    "https://raw.githubusercontent.com/liketolivefree/kobabi/main/sub.txt",
    "https://raw.githubusercontent.com/hans-thomas/v2ray-subscription/master/servers.txt",
    # ── دورِ دومِ منابعِ تازه (۲۰۲۶-۰۷-۲۷، همه با curl تست شدند) ──
    "https://raw.githubusercontent.com/barry-far/V2ray-Config/main/Sub1.txt",
    "https://raw.githubusercontent.com/barry-far/V2ray-Config/main/Sub2.txt",
    "https://raw.githubusercontent.com/barry-far/V2ray-Config/main/Sub3.txt",
    "https://raw.githubusercontent.com/barry-far/V2ray-Config/main/Sub4.txt",
    "https://raw.githubusercontent.com/MatinGhanbari/v2ray-configs/main/subscriptions/v2ray/super-sub.txt",
    "https://raw.githubusercontent.com/AzadNetCH/Clash/main/AzadNet.txt",
    "https://raw.githubusercontent.com/ts-sf/fly/main/v2",
    "https://raw.githubusercontent.com/ermaozi01/free_clash_vpn/main/subscribe/v2ray.txt",
    "https://raw.githubusercontent.com/Rayan-Config/C-Sub/main/configs/proxy.txt",
    "https://raw.githubusercontent.com/ALIILAPRO/v2rayNG-Config/main/sub.txt",
    "https://raw.githubusercontent.com/mheidari98/.proxy/main/vless",
    "https://raw.githubusercontent.com/mheidari98/.proxy/main/trojan",
    "https://raw.githubusercontent.com/mheidari98/.proxy/main/vmess",
    "https://raw.githubusercontent.com/Epodonios/v2ray-configs/main/Splitted-By-Protocol/ss.txt",
    "https://raw.githubusercontent.com/Surfboardv2ray/TGParse/main/splitted/vless",
    "https://raw.githubusercontent.com/Surfboardv2ray/TGParse/main/splitted/vmess",
    "https://raw.githubusercontent.com/Surfboardv2ray/TGParse/main/splitted/trojan",
]
# کانال‌های تلگرامِ بیشتر (تنوعِ بیشتر = کانفیگِ زنده‌ی بیشتر)
CHANNELS += [
    "v2rayngconfig", "vpnhubmarket", "custom_14v", "v2ray_swhil", "vmesskhonemun",
    "v2ray_alpha", "vpn_ocean", "netmelli", "v2rayng_matsuri", "hope_net",
    "oneclickvpnkeys", "v2rayNGvpni", "PrivateVPNs2", "MTConfig", "VlessConfigs",
    "proxyfarsi", "V2rayNGvpnfree", "vpnrooz", "configforvpn01", "melov2ray",
]

GOOD_PROTOS = ("vless", "trojan", "ss", "vmess", "hysteria2", "hy2", "tuic")
LINK_RE = re.compile(r'(?:vmess|vless|trojan|ss|hysteria2|hy2|tuic)://[^\s"\'<>\\`]+', re.I)
FLAGS = {}  # country_code -> flag emoji (built below)

_CTX = ssl.create_default_context(); _CTX.check_hostname = False; _CTX.verify_mode = ssl.CERT_NONE


def cc_to_flag(cc):
    if not cc or len(cc) != 2:
        return "🏳️"
    return chr(0x1F1E6 + ord(cc[0].upper()) - 65) + chr(0x1F1E6 + ord(cc[1].upper()) - 65)


COUNTRY_NAMES = {
    "DE": "آلمان", "NL": "هلند", "US": "آمریکا", "GB": "انگلیس", "FR": "فرانسه",
    "FI": "فنلاند", "SE": "سوئد", "CA": "کانادا", "JP": "ژاپن", "SG": "سنگاپور",
    "TR": "ترکیه", "RU": "روسیه", "AE": "امارات", "IN": "هند", "IR": "ایران",
    "PL": "لهستان", "AT": "اتریش", "CH": "سوئیس", "IT": "ایتالیا", "ES": "اسپانیا",
    "RO": "رومانی", "UA": "اوکراین", "LT": "لیتوانی", "LV": "لتونی", "HK": "هنگ‌کنگ",
    "KR": "کره", "AU": "استرالیا", "BR": "برزیل", "CZ": "چک", "DK": "دانمارک",
    "NO": "نروژ", "IE": "ایرلند", "BE": "بلژیک", "HU": "مجارستان", "BG": "بلغارستان",
    "MD": "مولداوی", "RS": "صربستان", "LU": "لوکزامبورگ", "CY": "قبرس", "EE": "استونی",
    "KZ": "قزاقستان", "AM": "ارمنستان", "GE": "گرجستان", "VN": "ویتنام", "MY": "مالزی",
    "ID": "اندونزی", "TH": "تایلند", "ZA": "آفریقای‌جنوبی", "MX": "مکزیک", "PT": "پرتغال",
    "GR": "یونان", "SK": "اسلواکی", "SI": "اسلوونی", "HR": "کرواسی", "IS": "ایسلند",
    "SA": "عربستان", "CW": "کوراسائو", "PA": "پاناما", "XX": "نامشخص",
}


def cc_name(cc):
    return COUNTRY_NAMES.get((cc or "").upper(), cc or "؟")


def fetch(url):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept-Encoding": "gzip"})
    try:
        with urllib.request.urlopen(req, timeout=25, context=_CTX) as r:
            raw = r.read()
            if (r.headers.get("Content-Encoding") or "").lower() == "gzip":
                raw = gzip.decompress(raw)
            return raw.decode("utf-8", "ignore")
    except Exception:
        return ""


def b64d(s):
    s = s.strip().replace("-", "+").replace("_", "/"); s += "=" * (-len(s) % 4)
    try: return base64.b64decode(s)
    except Exception: return b""


def extract(text):
    if not text: return set()
    text = (text.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">"))
    out = set(m.strip() for m in LINK_RE.findall(text))
    for blob in re.findall(r'[A-Za-z0-9+/_\-]{80,}={0,2}', text):
        dec = b64d(blob).decode("utf-8", "ignore")
        if dec:
            out |= set(m.strip() for m in LINK_RE.findall(dec))
    whole = b64d(text).decode("utf-8", "ignore")
    if whole:
        out |= set(m.strip() for m in LINK_RE.findall(whole))
    return out


def qs(q): return {k: urllib.parse.unquote(v) for k, v in urllib.parse.parse_qsl(q, keep_blank_values=True)}


def to_singbox(link, tag):
    """یک لینک را به outbound سینگ‌باکس تبدیل می‌کند. (server, outbound) یا None."""
    try:
        low = link.lower()
        def tls_block(sni, fp, alpn, insecure):
            t = {"enabled": True}
            if sni: t["server_name"] = sni
            if insecure: t["insecure"] = True
            if fp: t["utls"] = {"enabled": True, "fingerprint": fp}
            if alpn: t["alpn"] = [a for a in alpn.split(",") if a]
            return t
        def transport(net, path, host, sname):
            net = (net or "tcp").lower()
            if net == "ws":
                tr = {"type": "ws"}
                if path: tr["path"] = path
                if host: tr["headers"] = {"Host": host}
                return tr
            if net == "grpc":
                return {"type": "grpc", "service_name": sname or path or ""}
            if net in ("http", "h2"):
                tr = {"type": "http"}
                if path: tr["path"] = path
                if host: tr["host"] = [host]
                return tr
            return None

        if low.startswith("vmess://"):
            j = json.loads(b64d(link[8:]).decode("utf-8", "ignore"))
            add, port = j.get("add"), int(j.get("port", 0) or 0)
            if not add or not port: return None
            ob = {"type": "vmess", "tag": tag, "server": add, "server_port": port,
                  "uuid": j.get("id"), "security": j.get("scy", "auto") or "auto",
                  "alter_id": int(j.get("aid", 0) or 0)}
            if str(j.get("tls", "")).lower() == "tls":
                ob["tls"] = tls_block(j.get("sni") or j.get("host"), j.get("fp"), j.get("alpn"), True)
            tr = transport(j.get("net"), j.get("path"), j.get("host"), j.get("path"))
            if tr: ob["transport"] = tr
            return add, ob

        if low.startswith("vless://"):
            u = urllib.parse.urlparse(link); p = qs(u.query)
            if not (u.username and u.hostname and u.port): return None
            ob = {"type": "vless", "tag": tag, "server": u.hostname, "server_port": u.port,
                  "uuid": u.username}
            if p.get("flow"): ob["flow"] = p["flow"]
            sec = (p.get("security") or "none").lower()
            if sec == "reality":
                t = {"enabled": True, "server_name": p.get("sni", ""),
                     "utls": {"enabled": True, "fingerprint": p.get("fp", "chrome")},
                     "reality": {"enabled": True, "public_key": p.get("pbk", ""),
                                 "short_id": p.get("sid", "")}}
                ob["tls"] = t
            elif sec == "tls":
                ob["tls"] = tls_block(p.get("sni") or p.get("host"), p.get("fp"), p.get("alpn"), True)
            tr = transport(p.get("type"), p.get("path"), p.get("host"), p.get("serviceName"))
            if tr: ob["transport"] = tr
            return u.hostname, ob

        if low.startswith("trojan://"):
            u = urllib.parse.urlparse(link); p = qs(u.query)
            if not (u.username and u.hostname and u.port): return None
            ob = {"type": "trojan", "tag": tag, "server": u.hostname, "server_port": u.port,
                  "password": urllib.parse.unquote(u.username),
                  "tls": tls_block(p.get("sni") or p.get("host"), p.get("fp"), p.get("alpn"), True)}
            tr = transport(p.get("type"), p.get("path"), p.get("host"), p.get("serviceName"))
            if tr: ob["transport"] = tr
            return u.hostname, ob

        if low.startswith("ss://"):
            body = link[5:].split("#", 1)[0]
            if "@" in body:
                ui, hp = body.split("@", 1)
                if ":" not in ui: ui = b64d(ui).decode("utf-8", "ignore")
                method, pwd = ui.split(":", 1); hp = hp.split("?", 1)[0]; add, port = hp.rsplit(":", 1)
            else:
                dec = b64d(body).decode("utf-8", "ignore"); ui, hp = dec.split("@", 1)
                method, pwd = ui.split(":", 1); add, port = hp.rsplit(":", 1)
            port = int(re.sub(r"[^0-9]", "", port))
            return add, {"type": "shadowsocks", "tag": tag, "server": add, "server_port": port,
                         "method": method, "password": pwd}

        if low.startswith(("hysteria2://", "hy2://")):
            u = urllib.parse.urlparse(link); p = qs(u.query)
            if not (u.hostname and u.port): return None
            return u.hostname, {"type": "hysteria2", "tag": tag, "server": u.hostname,
                                "server_port": u.port, "password": u.username or "",
                                "tls": {"enabled": True, "server_name": p.get("sni", ""),
                                        "insecure": p.get("insecure") in ("1", "true")}}

        if low.startswith("tuic://"):
            u = urllib.parse.urlparse(link); p = qs(u.query)
            if not (u.username and u.hostname and u.port): return None
            return u.hostname, {"type": "tuic", "tag": tag, "server": u.hostname, "server_port": u.port,
                                "uuid": u.username, "password": (u.password or ""),
                                "tls": {"enabled": True, "server_name": p.get("sni", ""),
                                        "alpn": ["h3"], "insecure": p.get("insecure") in ("1", "true")}}
    except Exception:
        return None
    return None


def resolve_ip(host):
    try:
        return socket.gethostbyname(host)
    except Exception:
        return None


def geoip_batch(ips):
    """کشورِ هر IP را با ip-api.com (رایگان، دسته‌ای ۱۰۰تایی) پیدا می‌کند."""
    res = {}
    ips = list(ips)
    for i in range(0, len(ips), 100):
        chunk = ips[i:i + 100]
        body = json.dumps([{"query": ip, "fields": "query,countryCode"} for ip in chunk]).encode()
        try:
            req = urllib.request.Request("http://ip-api.com/batch", data=body,
                                         headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=30) as r:
                for row in json.loads(r.read().decode()):
                    res[row.get("query")] = row.get("countryCode") or "XX"
        except Exception:
            pass
        import time as _t; _t.sleep(1.5)  # مهربان با rate-limit
    return res


# ---- تنظیمات کیفیت ----
TOP_PER_COUNTRY   = 20     # چند کانفیگِ برتر per کشور در خروجی بماند (حالا همه تستِ واقعی شده‌اند)
LIVE_TEST_PER_CC  = 30     # چندتای برترِ هر کشور برای liveness تست شوند
LIVE_TEST_CAP     = 2500   # سقفِ کلِ تست‌های TCP (تا Action طولانی نشود)
DEAD_DROP         = 12     # اگر کانفیگی این‌قدر بار پشت‌سرهم مرده بود، از حافظه حذف شود (#10)
STALE_HOURS       = 72     # اگر این‌قدر ساعت دیده نشد هم حذف (#19/#20 با بازیابی)
STATE_FILE        = "state.json"


def load_state():
    try:
        with open(STATE_FILE, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


# ── مخزنِ انباشتی ──────────────────────────────────────────────────────────
# هر کانفیگی که تا حالا در اینترنت پیدا کرده‌ایم اینجا می‌ماند و هر اجرا دوباره
# در استخرِ کاندیداها می‌آید. کانالِ تلگرام پستِ قدیمی را پاک می‌کند ولی سرورش
# ممکن است ماه‌ها زنده باشد؛ با این کار «پکیجِ» ما هر ۳۰ دقیقه بزرگ‌تر می‌شود.
POOL_FILE = "pool.txt"
POOL_MAX = 60000          # سقف (فایل ~۶–۸ مگ) تا مخزن بی‌نهایت رشد نکند
POOL_STALE_DAYS = 21      # اگر این‌قدر روز نه دیده شد نه جواب داد، حذف


def load_pool():
    """{link: last_ok_or_seen_ts} — از فایلِ متنیِ ساده (هر خط: ts<TAB>link)"""
    out = {}
    try:
        with open(POOL_FILE, encoding="utf-8") as f:
            for line in f:
                ts, _, link = line.rstrip("\n").partition("\t")
                if link and ts.isdigit():
                    out[link] = int(ts)
    except Exception:
        pass
    return out


def save_pool(pool, now):
    cutoff = now - POOL_STALE_DAYS * 86400
    items = [(ts, l) for l, ts in pool.items() if ts >= cutoff]
    items.sort(reverse=True)                 # تازه‌ترها بمانند
    items = items[:POOL_MAX]
    with open(POOL_FILE, "w", encoding="utf-8") as f:
        for ts, l in items:
            f.write(f"{ts}\t{l}\n")
    return len(items)


def tcp_ok(host, port):
    try:
        s = socket.create_connection((host, int(port)), timeout=3)
        s.close(); return True
    except Exception:
        return False


def tcp_latency(host, port, tries=3):
    """تأخیر و جیترِ واقعی (نه فقط «پورت باز است») — برای انتخابِ سرورِ گیم.
    چند بار وصل می‌شود: میانه = پینگ، اختلافِ بیشینه/کمینه = جیتر (ناپایداری).
    خروجی: (ping_ms, jitter_ms) یا None اگر مرده باشد."""
    ts = []
    for _ in range(tries):
        t0 = time.perf_counter()
        try:
            s = socket.create_connection((host, int(port)), timeout=2.5)
            ts.append((time.perf_counter() - t0) * 1000.0)
            s.close()
        except Exception:
            pass
    if not ts:
        return None
    ts.sort()
    ping = ts[len(ts) // 2]                     # میانه = مقاوم به یک نمونه‌ی پرت
    jitter = (ts[-1] - ts[0]) if len(ts) > 1 else 0.0
    return (round(ping, 1), round(jitter, 1))


# کانفیگِ خوبِ گیم: پینگِ کم + جیترِ کم + ترجیحاً پروتکلِ UDP-محور (hy2/tuic)
GAME_MAX_PING   = 220     # میلی‌ثانیه — بالاتر از این برای بازی بی‌فایده است
GAME_MAX_JITTER = 60      # میلی‌ثانیه — ناپایداری = لگ/پرش در بازی


def game_score(ob, ping, jitter):
    """امتیازِ گیم: کم‌ترین بهتر. جیتر برای بازی از پینگ هم آزاردهنده‌تر است."""
    s = ping + jitter * 2.0
    if ob["type"] in ("hysteria2", "tuic"):
        s -= 60           # QUIC/UDP: پکت‌لاس را خودش جبران می‌کند → برای بازی به‌مراتب بهتر
    return s


# ==========================================================================
#   تستِ واقعیِ کانفیگ با sing-box (روی رانرِ گیت‌هاب) — «real delay»
#   TCP باز بودن هیچ تضمینی نیست: خیلی کانفیگ‌ها پورتشان باز است ولی کلید/uuid
#   منقضی شده و اصلاً ترافیک رد نمی‌کنند. اینجا واقعاً sing-box را بالا می‌آوریم و
#   از داخلِ تونل یک درخواستِ HTTP می‌زنیم → فقط کانفیگ‌هایی می‌مانند که *کار می‌کنند*.
# ==========================================================================
SB_BIN = os.environ.get("SINGBOX_BIN", "sing-box")   # در CI نصب می‌شود
REAL_TEST = os.environ.get("REAL_TEST", "1") != "0"
# سقفِ تستِ واقعی — گلوگاهِ اصلی بود: از ۲۶۵۱ زنده فقط ۷۰۰ تست می‌شد و ۱۳۶ تأیید.
# رانرِ گیت‌هاب کلِ کار را در ~۳ دقیقه (از ۴۵ دقیقه) انجام می‌داد، پس جا زیاد داریم.
REAL_TEST_CAP = int(os.environ.get("REAL_TEST_CAP", "5000"))
# ۴۸ کارگر اشتباه بود: رانرِ گیت‌هاب ۴ هسته دارد و ۴۸ پروسه‌ی sing-box همدیگر را
# خفه کردند (تأییدشده از ۱۳۶ افتاد به ۱۰۱). ۱۴ تعادلِ درست است.
REAL_TEST_WORKERS = int(os.environ.get("REAL_TEST_WORKERS", "14"))
_PORT_LOCK = __import__("threading").Lock()
_next_port = [24000]


def _grab_port():
    with _PORT_LOCK:
        p = _next_port[0]
        _next_port[0] += 1
        if _next_port[0] > 42000:
            _next_port[0] = 24000
        return p


def singbox_available():
    import subprocess
    try:
        subprocess.run([SB_BIN, "version"], capture_output=True, timeout=10)
        return True
    except Exception:
        return False


def real_delay(ob, timeout=10):
    """sing-box را با این تک کانفیگ بالا می‌آورد و از تونل یک درخواستِ واقعی می‌زند.
    خروجی: (delay_ms, dl_kbps) یا None اگر کار نکند."""
    import subprocess, tempfile
    port = _grab_port()
    cfg = {
        "log": {"level": "fatal"},
        "inbounds": [{"type": "mixed", "tag": "in", "listen": "127.0.0.1", "listen_port": port}],
        "outbounds": [dict(ob, tag="proxy"), {"type": "direct", "tag": "direct"}],
        "route": {"final": "proxy"},
    }
    f = tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8")
    json.dump(cfg, f, ensure_ascii=False)
    f.close()
    proc = None
    try:
        proc = subprocess.Popen([SB_BIN, "run", "-c", f.name],
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        # منتظرِ بالا آمدنِ پورت
        up = False
        deadline = time.time() + 6
        while time.time() < deadline:
            if proc.poll() is not None:
                return None                 # sing-box خودش مرد = کانفیگ نامعتبر
            try:
                socket.create_connection(("127.0.0.1", port), timeout=0.4).close()
                up = True
                break
            except Exception:
                time.sleep(0.15)
        if not up:
            return None
        px = urllib.request.ProxyHandler({"http": f"http://127.0.0.1:{port}",
                                          "https": f"http://127.0.0.1:{port}"})
        opener = urllib.request.build_opener(px)
        # ۱) تأخیرِ واقعی
        t0 = time.perf_counter()
        try:
            r = opener.open("http://cp.cloudflare.com/generate_204", timeout=timeout)
            r.read(64)
            if r.status not in (200, 204):
                return None
        except Exception:
            return None
        delay = (time.perf_counter() - t0) * 1000.0
        # ۲) سرعتِ تقریبی (دانلودِ کوچک ~256KB) — برای استریم/دانلود مهم است
        kbps = 0.0
        try:
            t1 = time.perf_counter()
            r2 = opener.open("http://speed.cloudflare.com/__down?bytes=262144", timeout=timeout)
            n = len(r2.read())
            dt = max(time.perf_counter() - t1, 0.001)
            kbps = (n / 1024.0) / dt
        except Exception:
            pass
        return (round(delay, 1), round(kbps, 1))
    except Exception:
        return None
    finally:
        if proc:
            try:
                proc.kill(); proc.wait(timeout=5)
            except Exception:
                pass
        try:
            os.unlink(f.name)
        except Exception:
            pass


def score(link, ob, pop, st, now):
    """رتبه: reality/tls امن‌تر، hy2/tuic برای پکت‌لاست بهتر، محبوبیت، و تاریخچه."""
    s = 0.0
    tls = ob.get("tls")
    if tls and tls.get("reality"): s += 3            # #13 reality امن‌ترین
    elif tls:                      s += 2            # #13 tls
    if ob["type"] in ("hysteria2", "tuic"): s += 1.5 # #9 مقاومِ پکت‌لاست
    s += min(pop.get(link, 1), 5) * 0.3              # #2 محبوبیت
    if st.get("last_ok", 0) > now - 6 * 3600: s += 2 # #10 تازه جواب داده
    s -= min(st.get("dead", 0), 5) * 0.3             # #1 سابقه‌ی مرگ
    return s


# استخرِ کاندیدا قبل از liveness. با مخزنِ انباشتی این عدد باید بزرگ باشد وگرنه
# کانفیگ‌های ذخیره‌شده‌ی قدیمی هیچ‌وقت دوباره شانسِ تست پیدا نمی‌کنند.
CAND_CAP = int(os.environ.get("CAND_CAP", "12000"))


def main():
    import time as _t
    from collections import Counter
    from concurrent.futures import as_completed
    now = int(_t.time())
    state = load_state()

    print("گرفتن منابع…")
    sources = [f"https://t.me/s/{c.strip()}" for c in CHANNELS if c.strip()] + SUB_URLS
    pop = Counter()
    with ThreadPoolExecutor(max_workers=32) as ex:
        for links in ex.map(lambda u: extract(fetch(u)), sources):
            for l in links:
                pop[l] += 1
    fresh = list(pop)
    print(f"{len(fresh)} کانفیگِ یکتا از {len(sources)} منبع")

    # ── مخزنِ انباشتی: هرچه تا حالا پیدا کرده‌ایم را هم به کاندیداها اضافه کن ──
    pool = load_pool()
    before = len(pool)
    revived = sum(1 for l in pool if l not in pop)   # فقط در مخزن هست، منابع دیگر ندارندش
    for l in fresh:
        pool[l] = now                       # تازه دیده شد
    all_links = list(pool)
    print(f"مخزنِ انباشتی: {len(pool)} کانفیگ (تازه از منابع: {len(fresh)}، "
          f"از مخزنِ قبلی: {before}، فقط-در-مخزن: {revived})")

    # تبدیل + dedup بر اساس هویتِ سرور (نه اسم)
    items = []; seen_id = set()
    for l in all_links:
        conv = to_singbox(l, "t")
        if not conv:
            continue
        host, ob = conv
        if not ob.get("server") or not ob.get("server_port"):
            continue
        _tls = ob.get("tls") or {}
        if _tls.get("reality") and not _tls["reality"].get("public_key"):
            continue   # reality بدون public_key کار نمی‌کند
        ident = (ob["type"], ob["server"], ob["server_port"],
                 ob.get("uuid") or ob.get("password") or "")
        if ident in seen_id:
            continue
        seen_id.add(ident)
        items.append((host, ob, l))
    print(f"{len(items)} کانفیگِ یکتا (پس از dedup)")

    # رتبه (بدون GeoIP) → فقط برترها را نگه می‌داریم
    items.sort(key=lambda t: -score(t[2], t[1], pop, state.get(t[2], {}), now))
    cand = items[:CAND_CAP]

    # liveness روی برترها (create_connection خودش resolve می‌کند)
    print(f"تستِ تأخیر/جیتر روی {len(cand)} کانفیگِ برتر…")
    alive = []
    lat = {}   # link -> (ping_ms, jitter_ms)
    with ThreadPoolExecutor(max_workers=300) as ex:
        futs = {ex.submit(tcp_latency, ob["server"], ob["server_port"]): (host, ob, link)
                for host, ob, link in cand}
        for fut in as_completed(futs):
            host, ob, link = futs[fut]
            st = state.setdefault(link, {}); st["seen"] = now
            r = fut.result()
            if r:
                ping, jitter = r
                lat[link] = r
                st["dead"] = 0; st["last_ok"] = now; st["ping"] = ping; st["jitter"] = jitter
                alive.append((host, ob, link))
            else:
                st["dead"] = st.get("dead", 0) + 1
    # مرتب‌سازیِ ترکیبی: پینگ و جیترِ واقعی + امتیازِ کیفیت (reality/tls، محبوبیت، تاریخچه).
    # فقط-پینگ کافی نیست (کانفیگِ کم‌پینگِ ناامن/بی‌دوام بالا می‌آمد) و فقط-امتیاز هم پینگ را
    # نادیده می‌گرفت — همان چیزی که باعث می‌شد «پینگ سرِ کانفیگ‌های بد برود بالا».
    def _rank(t):
        ping, jitter = lat.get(t[2], (9999, 0))
        return ping + jitter * 2.0 - score(t[2], t[1], pop, state.get(t[2], {}), now) * 15.0
    alive.sort(key=_rank)
    print(f"زنده (TCP): {len(alive)}")

    # ── تستِ واقعی با sing-box: فقط کانفیگ‌هایی که *واقعاً ترافیک رد می‌کنند* ──
    real = {}   # link -> (delay_ms, kbps)
    if REAL_TEST and singbox_available():
        # اولویتِ صف — درسِ گران‌بها: وقتی استخر بزرگ شد، مرتب‌سازیِ صرفاً بر اساسِ
        # پینگ/امتیاز پر شد از کانفیگ‌های کم‌پینگِ بی‌کیفیت و «تأییدشده‌های دفعه‌ی قبل»
        # از صدر بیرون افتادند (تأییدشده از ۱۳۶ افتاد به ۶۳).
        # حالا اول همه‌ی کانفیگ‌هایی که قبلاً *واقعاً* جواب داده‌اند تست می‌شوند،
        # بعد بقیه به ترتیبِ رتبه. این‌طور کیفیت روی هم انباشته می‌شود.
        proven, others = [], []
        for t3 in alive:
            st = state.get(t3[2], {})
            if st.get("real_ok", 0) > now - 7 * 86400:   # هفته‌ی اخیر واقعاً کار کرده
                proven.append(t3)
            else:
                others.append(t3)
        proven.sort(key=lambda t: state.get(t[2], {}).get("real_ms", 9999))
        target = (proven + others)[:REAL_TEST_CAP]
        print(f"صفِ تست: {len(proven)} تأییدشده‌ی قبلی + {len(target) - min(len(proven), len(target))} جدید")
        print(f"تستِ واقعی (sing-box) روی {len(target)} کانفیگِ برتر…")
        with ThreadPoolExecutor(max_workers=REAL_TEST_WORKERS) as ex:
            futs = {ex.submit(real_delay, ob): (host, ob, link) for host, ob, link in target}
            for fut in as_completed(futs):
                host, ob, link = futs[fut]
                r = fut.result()
                st = state.setdefault(link, {})
                if r:
                    real[link] = r
                    st["real_ms"], st["kbps"] = r[0], r[1]
                    st["real_ok"] = now
                    st["real_fail"] = 0
                else:
                    st["real_fail"] = st.get("real_fail", 0) + 1
        print(f"واقعاً کار می‌کنند: {len(real)} از {len(target)}")
        # فقط تست‌شده‌های موفق بمانند (اگر تعدادشان معقول بود)؛ وگرنه به TCP برگرد
        verified = [t for t in alive if t[2] in real]
        if len(verified) >= 25:
            verified.sort(key=lambda t: real[t[2]][0] - min(real[t[2]][1], 4000) / 200.0)
            alive = verified
            print(f"لیست به {len(alive)} کانفیگِ تأییدشده محدود شد")
        else:
            print("تعدادِ تأییدشده کم بود — از نتیجه‌ی TCP استفاده می‌شود")
    else:
        print("تستِ واقعی غیرفعال/در دسترس نیست — فقط TCP")

    # GeoIP فقط روی زنده‌ها (چند صدتا)
    hosts = {h for h, _o, _l in alive}
    host_ip = {}
    with ThreadPoolExecutor(max_workers=64) as ex:
        for h, ip in zip(hosts, ex.map(resolve_ip, hosts)):
            if ip: host_ip[h] = ip
    cc = geoip_batch(set(host_ip.values()))

    # گروه‌بندیِ کشوری (alive از قبل بر اساس رتبه مرتب است)
    by_cc_items = {}
    for host, ob, link in alive:
        code = (cc.get(host_ip.get(host)) if host_ip.get(host) else None) or "XX"
        by_cc_items.setdefault(code, []).append((host, ob, link))

    # اگر کل زنده‌ها کم بود، از cand (تست‌نشده‌ها) پر کن (#20 fallback)
    alive_links = {l for _h, _o, l in alive}
    if len(alive) < 40:
        for host, ob, link in cand:
            if link not in alive_links:
                by_cc_items.setdefault("XX", []).append((host, ob, link))
            if sum(len(v) for v in by_cc_items.values()) >= 60:
                break

    # ساخت outboundها: top-N per کشور
    seen_tags = set(); by_country = {}; all_tags = []; outbounds = []
    sub_entries = []   # برای sub.txt (v2rayN / v2rayNG / موبایل) با اسمِ پرچم‌دار
    for code, lst in by_cc_items.items():
        for host, ob, link in lst[:TOP_PER_COUNTRY]:
            flag = cc_to_flag(code)
            tag = f"{flag} {code} | {ob['type']} | {host}:{ob['server_port']}"
            base = tag; n = 2
            while tag in seen_tags:
                tag = f"{base} #{n}"; n += 1
            seen_tags.add(tag)
            ob = dict(ob); ob["tag"] = tag
            outbounds.append(ob); all_tags.append(tag)
            by_country.setdefault(code, []).append(tag)
            # همان لینکِ اصلی، ولی با اسمِ پرچم‌دار + تأخیرِ اندازه‌گیری‌شده (✅ = تستِ واقعی پاس شده)
            if link in real:
                _pt = f" | ✅{int(real[link][0])}ms"
            else:
                _p = lat.get(link)
                _pt = f" | {int(_p[0])}ms" if _p else ""
            sub_entries.append(link.split("#", 1)[0] + "#" + urllib.parse.quote(f"{flag} {code} | {ob['type']}{_pt}"))

    # گروهِ urltest per کشور (failoverِ سریع) + گروهِ کلی + انتخابگر
    country_groups = []
    for code in sorted(by_country, key=lambda c: -len(by_country[c])):
        gtag = f"{cc_to_flag(code)} {code} (بهترین)"
        country_groups.append(gtag)
        # چسبنده: IP توی همان کشور ثابت می‌ماند و فقط وقتی واقعاً افتاد عوض می‌شود
        # (interval بلند = ثابت؛ ولی failover روی خرابیِ واقعی همچنان سریع است)
        outbounds.append({"type": "urltest", "tag": gtag, "outbounds": by_country[code],
                          "url": "https://www.gstatic.com/generate_204",
                          "interval": "10m", "tolerance": 200, "idle_timeout": "30m"})
    outbounds.append({"type": "urltest", "tag": "⚡ خودکار (همه)", "outbounds": all_tags,
                      "url": "https://www.gstatic.com/generate_204", "interval": "90s", "tolerance": 60})
    selector = {"type": "selector", "tag": "🌍 انتخاب",
                "outbounds": ["⚡ خودکار (همه)"] + country_groups, "default": "⚡ خودکار (همه)"}
    outbounds = [selector] + outbounds + [{"type": "direct", "tag": "direct"}]

    config = {
        "log": {"level": "warn"},
        "dns": {"servers": [{"tag": "remote", "address": "https://1.1.1.1/dns-query", "detour": "🌍 انتخاب"},
                            {"tag": "local", "address": "local"}],
                "rules": [{"outbound": "any", "server": "local"}], "final": "remote"},
        "inbounds": [{"type": "mixed", "tag": "in", "listen": "127.0.0.1", "listen_port": 2080}],
        "outbounds": outbounds,
        "route": {"auto_detect_interface": True, "final": "🌍 انتخاب"},
    }
    with open("singbox.json", "w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False, indent=1)

    # ساب معمولی با اسم‌های پرچم‌دار (v2rayN / v2rayNG / نکوباکس / موبایل)
    sub_text = "\n".join(sub_entries) if sub_entries else "\n".join(sorted(all_links)[:400])
    with open("sub.txt", "w", encoding="utf-8") as f:
        f.write(base64.b64encode(sub_text.encode()).decode())

    # ── ساب مخصوصِ گیم: کم‌پینگ‌ترین + پایدارترین (جیترِ کم) + ترجیحِ hy2/tuic ──
    # برای بازی، جیتر (ناپایداری) از پینگ هم آزاردهنده‌تر است، و پروتکل‌های UDP-محور
    # (hysteria2/tuic) پکت‌لاس را جبران می‌کنند. اینجا فقط همان‌ها را جدا می‌کنیم.
    game_pool = []
    for host, ob, link in alive:
        r = lat.get(link)
        if not r:
            continue
        ping, jitter = r
        # اگر تستِ واقعی داریم، تأخیرِ واقعی (از داخلِ تونل) ملاک است — نه صرفِ TCP
        if link in real:
            ping = real[link][0]
        if ping > GAME_MAX_PING or jitter > GAME_MAX_JITTER:
            continue
        game_pool.append((game_score(ob, ping, jitter), ping, jitter, host, ob, link))
    game_pool.sort(key=lambda t: t[0])
    game_entries = []
    seen_game_srv = set()
    for _s, ping, jitter, host, ob, link in game_pool:
        key = (ob["server"], ob["server_port"])
        if key in seen_game_srv:      # از هر سرور یکی (تنوع بیشتر برای failover)
            continue
        seen_game_srv.add(key)
        code = (cc.get(host_ip.get(host)) if host_ip.get(host) else None) or "XX"
        udp = "⚡" if ob["type"] in ("hysteria2", "tuic") else ""
        name = f"{udp}🎮 {cc_to_flag(code)} {code} | {ob['type']} | {int(ping)}ms"
        game_entries.append(link.split("#", 1)[0] + "#" + urllib.parse.quote(name))
        if len(game_entries) >= 80:
            break
    with open("game.txt", "w", encoding="utf-8") as f:
        f.write(base64.b64encode("\n".join(game_entries).encode()).decode())
    print(f"گیم: {len(game_entries)} کانفیگ (پینگ ≤{GAME_MAX_PING}ms، جیتر ≤{GAME_MAX_JITTER}ms)")

    # ══════════════════════════════════════════════════════════════════════
    #  manifest.json — «شبیه‌سازِ نت»
    #  گیت‌هاب نمی‌تواند فیلترینگِ ایران را ببیند (رانرش در آمریکا/اروپاست)، پس
    #  نمی‌تواند بگوید کدام کانفیگ *از داخلِ ایران* باز می‌شود. ولی می‌تواند هر
    #  کانفیگِ تأییدشده را با ویژگی‌هایش برچسب بزند؛ آن‌وقت اپ روی PCِ تو نتت را
    #  تحلیل می‌کند (UDP باز؟ SNI-DPI؟ کدام پورت‌ها؟ کلادفلر؟) و فقط کانفیگ‌های
    #  «مناسبِ همان نت» را برمی‌دارد — یعنی به‌جای تستِ ۴۰۰ کانفیگ، ۲۰ تا.
    #  این همان چیزی است که مصرفِ دیتای تو را می‌خورد و اینجا حل می‌شود.
    # ══════════════════════════════════════════════════════════════════════
    CDN_PORTS = {80, 443, 2052, 2053, 2082, 2083, 2086, 2087, 2095, 2096, 8080, 8443}
    manifest = []
    for host, ob, link in alive:
        code = (cc.get(host_ip.get(host)) if host_ip.get(host) else None) or "XX"
        tls = ob.get("tls") or {}
        tr = (ob.get("transport") or {}).get("type") or "tcp"
        port = int(ob.get("server_port") or 0)
        feats = {
            "t": ob["type"],                              # پروتکل
            "p": port,                                    # پورت
            "n": tr,                                      # transport (tcp/ws/grpc/http)
            "cc": code,
            "udp": ob["type"] in ("hysteria2", "tuic"),   # نیازمندِ باز بودنِ UDP
            "re": bool(tls.get("reality")),               # REALITY (ضدِ SNI-DPI)
            "tls": bool(tls),
            "cdn": port in CDN_PORTS and tr in ("ws", "grpc", "http"),  # پشتِ CDN/Worker
        }
        r = real.get(link)
        if r:
            feats["ms"] = int(r[0])
            if r[1] > 200: feats["kbps"] = int(r[1])
        feats["u"] = link.split("#", 1)[0]
        manifest.append(feats)
    with open("manifest.json", "w", encoding="utf-8") as f:
        json.dump({"updated": now, "count": len(manifest), "configs": manifest},
                  f, ensure_ascii=False, separators=(",", ":"))
    print(f"manifest.json: {len(manifest)} کانفیگِ برچسب‌خورده (اپ بدونِ تست فیلتر می‌کند)")

    # ── ساب‌های تخصصیِ دیگر ──
    # ۱) stream.txt — برای استریم/دانلود: کشورهایی که سرویس‌ها معمولاً باز می‌کنند
    #    (آمریکا/بریتانیا/آلمان/هلند/فرانسه/کانادا) و پهنای باندِ پایدار مهم‌تر از پینگ است.
    STREAM_CC = ("US", "GB", "UK", "DE", "NL", "FR", "CA", "SE", "FI", "JP", "SG")
    stream_pool = []
    for host, ob, link in alive:
        code = (cc.get(host_ip.get(host)) if host_ip.get(host) else None) or "XX"
        if code not in STREAM_CC:
            continue
        kbps = real.get(link, (0, 0))[1]
        stream_pool.append((-kbps, code, ob, link))   # پهنای باندِ بیشتر = بالاتر
    stream_pool.sort(key=lambda t: t[0])
    stream_entries = []
    for _negk, code, ob, link in stream_pool[:80]:
        kbps = real.get(link, (0, 0))[1]
        sp = f" | {int(kbps/1024*8)}Mb" if kbps > 200 else ""
        pt = f" | ✅{int(real[link][0])}ms" if link in real else (
             f" | {int(lat[link][0])}ms" if link in lat else "")
        stream_entries.append(link.split("#", 1)[0] + "#" +
                              urllib.parse.quote(f"🎬 {cc_to_flag(code)} {code} | {ob['type']}{pt}{sp}"))
    with open("stream.txt", "w", encoding="utf-8") as f:
        f.write(base64.b64encode("\n".join(stream_entries).encode()).decode())

    # ۲) udp.txt — فقط پروتکل‌های UDP-محور (hysteria2/tuic): بهترین برای بازی/تماسِ تصویری
    udp_entries = []
    for host, ob, link in alive:
        if ob["type"] not in ("hysteria2", "tuic"):
            continue
        code = (cc.get(host_ip.get(host)) if host_ip.get(host) else None) or "XX"
        r = lat.get(link)
        pt = f" | {int(r[0])}ms" if r else ""
        udp_entries.append(link.split("#", 1)[0] + "#" +
                           urllib.parse.quote(f"⚡ {cc_to_flag(code)} {code} | {ob['type']}{pt}"))
    with open("udp.txt", "w", encoding="utf-8") as f:
        f.write(base64.b64encode("\n".join(udp_entries).encode()).decode())
    print(f"استریم: {len(stream_entries)} · UDP(hy2/tuic): {len(udp_entries)}")

    # ── ساب جدا برای هر کشور (IPت در یک کشور ثابت می‌ماند) — ایده‌ی خودت ──
    import os as _os, shutil as _sh
    if _os.path.isdir("countries"):
        _sh.rmtree("countries")            # کشورهای کهنه پاک شوند
    _os.makedirs("countries", exist_ok=True)
    idx = []
    for code, lst in by_cc_items.items():
        flag = cc_to_flag(code)
        entries = [l.split("#", 1)[0] + "#" + urllib.parse.quote(f"{flag} {code} | {o['type']}")
                   for _h, o, l in lst]
        if not entries:
            continue
        with open(f"countries/{code}.txt", "w", encoding="utf-8") as f:
            f.write(base64.b64encode("\n".join(entries).encode()).decode())
        idx.append((code, len(entries)))
    idx.sort(key=lambda x: -x[1])
    repo = _os.environ.get("GITHUB_REPOSITORY", "USER/REPO")
    raw = f"https://raw.githubusercontent.com/{repo}/main"
    with open("countries/INDEX.md", "w", encoding="utf-8") as f:
        f.write("# ساب‌های کشوری — هرکدام فقط یک کشور\n\n")
        f.write("لینکِ خامِ هر کشوری را که می‌خواهی، جدا به‌عنوان یک subscription اضافه کن:\n\n")
        for code, n in idx:
            f.write(f"- {cc_to_flag(code)} **{cc_name(code)}** ({code}) — {n} کانفیگ → "
                    f"`{raw}/countries/{code}.txt`\n")

    # ── داشبورد (GitHub Pages) — ایده ۱۷ ──
    rows = "".join(
        f"<tr><td>{cc_to_flag(c)} {cc_name(c)} <small>({c})</small></td><td>{n}</td>"
        f"<td><code onclick=\"navigator.clipboard.writeText('{raw}/countries/{c}.txt')\" "
        f"title='کلیک=کپی'>{raw}/countries/{c}.txt</code></td></tr>"
        for c, n in idx)
    total = sum(n for _c, n in idx)
    updated = _t.strftime("%Y-%m-%d %H:%M UTC", _t.gmtime())
    html = f"""<!doctype html><html lang=fa dir=rtl><head><meta charset=utf-8>
<meta name=viewport content="width=device-width,initial-scale=1"><title>config-cloud</title>
<style>body{{font-family:Vazirmatn,Tahoma,sans-serif;max-width:820px;margin:24px auto;padding:0 14px;
background:#0b0e14;color:#e6e6e6}}h1{{font-size:1.4rem}}.s{{color:#9aa}}
table{{width:100%;border-collapse:collapse;margin-top:14px}}
td,th{{border-bottom:1px solid #222;padding:8px;text-align:right}}
code{{background:#161b26;color:#8ad;padding:3px 6px;border-radius:6px;cursor:pointer;
font-size:.8rem;word-break:break-all}}</style></head><body>
<h1>🌍 config-cloud</h1>
<p class=s>مجموع: <b>{total}</b> کانفیگ در <b>{len(idx)}</b> کشور · آخرین آپدیت: {updated}</p>
<p class=s>لینکِ کلی (همه): <code onclick="navigator.clipboard.writeText('{raw}/sub.txt')">{raw}/sub.txt</code></p>
<p class=s>روی هر لینک کلیک کنی، کپی می‌شود. هر کشور را جدا در کلاینتت اضافه کن.</p>
<table><tr><th>کشور</th><th>تعداد</th><th>لینکِ ساب</th></tr>{rows}</table>
</body></html>"""
    with open("index.html", "w", encoding="utf-8") as f:
        f.write(html)

    # ── یک لینکِ واحد: «بهترین کانفیگِ هر کشور» (یکجا اضافه کن) ──
    best_entries = []
    for code, _n in idx:
        if code == "XX":
            continue                      # نامشخص را در «هر کشور» نمی‌آوریم
        host, o, link = by_cc_items[code][0]   # رتبه‌ی اولِ آن کشور
        best_entries.append(link.split("#", 1)[0] + "#" +
                            urllib.parse.quote(f"{cc_to_flag(code)} {cc_name(code)}"))
    with open("best.txt", "w", encoding="utf-8") as f:
        f.write(base64.b64encode("\n".join(best_entries).encode()).decode())
    print(f"ساب‌های کشوری: {len(idx)} کشور | best.txt: {len(best_entries)} کشور | داشبورد: index.html")

    # پاکسازیِ حافظه: مرده‌های مزمن + کهنه‌ها (#10/#19)؛ اگر دوباره دیده شوند، seen تازه است
    cutoff = now - STALE_HOURS * 3600
    for l in list(state.keys()):
        st = state[l]
        if (st.get("dead", 0) >= DEAD_DROP) or (st.get("seen", 0) < cutoff):
            del state[l]
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f)

    # مخزنِ انباشتی: هرچه امروز جواب داد را «تازه» علامت بزن تا دیرتر کهنه شود،
    # و کانفیگ‌هایی که خیلی وقت است نه دیده شده‌اند نه جواب داده‌اند حذف شوند.
    for _h, _o, l in alive:
        pool[l] = now
    for l in real:
        pool[l] = now
    kept = save_pool(pool, now)

    print(f"ساخته شد: {len(all_tags)} کانفیگ در {len(by_country)} کشور | "
          f"زنده: {len(alive)} | تأییدشده: {len(real)} | مخزن: {kept} | حافظه: {len(state)}")


if __name__ == "__main__":
    main()
