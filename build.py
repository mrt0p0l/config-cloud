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
import os, re, ssl, json, gzip, base64, socket, urllib.request, urllib.parse
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
]

GOOD_PROTOS = ("vless", "trojan", "ss", "vmess", "hysteria2", "hy2", "tuic")
LINK_RE = re.compile(r'(?:vmess|vless|trojan|ss|hysteria2|hy2|tuic)://[^\s"\'<>\\`]+', re.I)
FLAGS = {}  # country_code -> flag emoji (built below)

_CTX = ssl.create_default_context(); _CTX.check_hostname = False; _CTX.verify_mode = ssl.CERT_NONE


def cc_to_flag(cc):
    if not cc or len(cc) != 2:
        return "🏳️"
    return chr(0x1F1E6 + ord(cc[0].upper()) - 65) + chr(0x1F1E6 + ord(cc[1].upper()) - 65)


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
TOP_PER_COUNTRY   = 12     # چند کانفیگِ برتر per کشور در خروجی بماند (کوچیک=failover سریع)
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


def tcp_ok(host, port):
    try:
        s = socket.create_connection((host, int(port)), timeout=3)
        s.close(); return True
    except Exception:
        return False


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


CAND_CAP = 3000   # چند کانفیگِ برترِ کل، قبل از liveness (بقیه اصلاً تست/GeoIP نمی‌شوند)


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
    all_links = list(pop)
    print(f"{len(all_links)} کانفیگِ یکتا از {len(sources)} منبع")

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
    print(f"تستِ liveness روی {len(cand)} کانفیگِ برتر…")
    alive = []
    with ThreadPoolExecutor(max_workers=300) as ex:
        futs = {ex.submit(tcp_ok, ob["server"], ob["server_port"]): (host, ob, link)
                for host, ob, link in cand}
        for fut in as_completed(futs):
            host, ob, link = futs[fut]
            st = state.setdefault(link, {}); st["seen"] = now
            if fut.result():
                alive.append((host, ob, link)); st["dead"] = 0; st["last_ok"] = now
            else:
                st["dead"] = st.get("dead", 0) + 1
    print(f"زنده: {len(alive)}")

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
            # همان لینکِ اصلی، ولی با اسمِ پرچم‌دارِ کشوری (برای هر کلاینتی)
            sub_entries.append(link.split("#", 1)[0] + "#" + urllib.parse.quote(f"{flag} {code} | {ob['type']}"))

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

    # پاکسازیِ حافظه: مرده‌های مزمن + کهنه‌ها (#10/#19)؛ اگر دوباره دیده شوند، seen تازه است
    cutoff = now - STALE_HOURS * 3600
    for l in list(state.keys()):
        st = state[l]
        if (st.get("dead", 0) >= DEAD_DROP) or (st.get("seen", 0) < cutoff):
            del state[l]
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f)

    print(f"ساخته شد: {len(all_tags)} کانفیگ در {len(by_country)} کشور | زنده: {len(alive)} | حافظه: {len(state)}")


if __name__ == "__main__":
    main()
