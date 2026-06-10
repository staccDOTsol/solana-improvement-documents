import os, json, time, threading, sqlite3, urllib.request, hashlib
from collections import defaultdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HELIUS_KEY = os.environ.get("HELIUS_KEY", "")
RPC = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_KEY}"
ENH = f"https://api.helius.xyz/v0"
DATA_DIR = os.environ.get("DATA_DIR", "/data")
os.makedirs(DATA_DIR, exist_ok=True)
DB = os.path.join(DATA_DIR, "clusters.db")

# ---- frozen, hash-pinned historical snapshot (the case study) ----
SNAP = {}
try:
    with open("snapshot.json") as f:
        SNAP = json.load(f)
    SNAP_HASH = hashlib.sha256(open("snapshot.json","rb").read()).hexdigest()[:16]
except Exception:
    SNAP_HASH = "unavailable"

# ---- known fingerprint wallets (the persistent t=0 fleet from the case study) ----
FLEET = SNAP.get("fleet", [])
JITO = {"96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5","HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe",
"Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY","ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49",
"DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh","ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt",
"DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL","3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT"}
SOL = "So11111111111111111111111111111111111111112"
# majors/stables to exclude from candidate mints (fleet wallets hold these incidentally)
EXCLUDE = {SOL,
 "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
 "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",  # USDT
 "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So",   # mSOL
 "7vfCXTUXx5WJV5JADk17DUJ4ksgau7utNKj4b963voxs",  # ETH (wormhole)
 "27G8MtK7VtTcCHkpASjSDdkWWYfoqT6ggEuKidVJidD4"}  # JLP
HDRS = {"Content-Type":"application/json","User-Agent":"cluster-monitor/1.0"}

def rpc(method, params, retries=3):
    body = json.dumps({"jsonrpc":"2.0","id":1,"method":method,"params":params}).encode()
    for i in range(retries):
        try:
            req = urllib.request.Request(RPC, data=body, headers=HDRS)
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.loads(r.read()).get("result")
        except Exception:
            time.sleep(1.5*(i+1))
    return None

def enh_addr(addr, limit=20, before=None):
    url = f"{ENH}/addresses/{addr}/transactions?api-key={HELIUS_KEY}&limit={limit}"
    if before: url += f"&before={before}"
    for i in range(3):
        try:
            req = urllib.request.Request(url, headers=HDRS)
            with urllib.request.urlopen(req, timeout=40) as r:
                return json.loads(r.read())
        except Exception:
            time.sleep(1.5*(i+1))
    return []

def enh_txs(sigs):
    out=[]
    for i in range(0,len(sigs),100):
        try:
            req = urllib.request.Request(f"{ENH}/transactions?api-key={HELIUS_KEY}",
                data=json.dumps({"transactions":sigs[i:i+100]}).encode(), headers=HDRS)
            with urllib.request.urlopen(req, timeout=40) as r:
                d=json.loads(r.read())
                if isinstance(d,list): out+=d
        except Exception:
            pass
    return out

def db():
    c = sqlite3.connect(DB, timeout=30)
    c.execute("""CREATE TABLE IF NOT EXISTS clusters(
        mint TEXT PRIMARY KEY, first_seen INTEGER, fleet_hits INTEGER,
        fleet_wallets TEXT, n_big_buyers INTEGER, big_buyer_sol REAL,
        wash_ratio REAL, wash_sol REAL, gross_sol REAL, score REAL, updated INTEGER, note TEXT)""")
    c.execute("""CREATE TABLE IF NOT EXISTS scan_log(ts INTEGER, msg TEXT)""")
    c.execute("""CREATE TABLE IF NOT EXISTS wallets(
        addr TEXT PRIMARY KEY, clusters TEXT, recurrence INTEGER,
        seed INTEGER, first_seen INTEGER, last_seen INTEGER, promoted INTEGER)""")
    return c

def discovered_watchlist():
    """Seed fleet plus any wallet that exhibited the signature at >=2 distinct clusters."""
    try:
        c=db()
        rows=[r[0] for r in c.execute("SELECT addr FROM wallets WHERE recurrence>=2")]
        c.close()
    except Exception:
        rows=[]
    # de-dup, seed first, cap to keep each cycle cheap
    extra=[w for w in rows if w not in FLEET]
    return (FLEET + extra)[:60]

def record_sig_wallets(mint, wallets, seed_set):
    if not wallets: return
    c=db(); now=int(time.time())
    for w in wallets:
        row=c.execute("SELECT clusters,seed FROM wallets WHERE addr=?",(w,)).fetchone()
        if row:
            cl=set(json.loads(row[0])); cl.add(mint)
            c.execute("UPDATE wallets SET clusters=?,recurrence=?,last_seen=? WHERE addr=?",
                      (json.dumps(sorted(cl)), len(cl), now, w))
        else:
            c.execute("INSERT INTO wallets VALUES(?,?,?,?,?,?,?)",
                      (w, json.dumps([mint]), 1, 1 if w in seed_set else 0, now, now, 0))
    c.commit(); c.close()

def log(msg):
    try:
        c=db(); c.execute("INSERT INTO scan_log VALUES(?,?)",(int(time.time()),msg)); c.commit(); c.close()
    except Exception: pass
    print(msg, flush=True)

# ---------- wash + signature scoring for a single mint ----------
def analyze_mint(mint, cap=600):
    sigs=[]; before=None
    while len(sigs)<cap:
        r=rpc("getSignaturesForAddress",[mint,{"limit":1000,**({"before":before} if before else {})}])
        if not r: break
        sigs+=[s["signature"] for s in r if s.get("err") is None]
        before=r[-1]["signature"]
        if len(r)<1000: break
    txs=enh_txs(sigs[:cap])
    if not txs: return None
    txs.sort(key=lambda t:t["timestamp"])
    t0=txs[0]["timestamp"]
    buy=defaultdict(float); sell=defaultdict(float)
    first60=set(); fleet_hit=set()
    for t in txs:
        if t.get("source") not in ("PUMP_FUN","PUMP_AMM"): continue
        tf=defaultdict(float); sf=defaultdict(float)
        for tt in t.get("tokenTransfers",[]):
            if tt["mint"]==mint:
                if tt.get("toUserAccount"): tf[tt["toUserAccount"]]+=tt.get("tokenAmount",0)
                if tt.get("fromUserAccount"): tf[tt["fromUserAccount"]]-=tt.get("tokenAmount",0)
        for n in t.get("nativeTransfers",[]):
            sf[n["fromUserAccount"]]-=n["amount"]/1e9; sf[n["toUserAccount"]]+=n["amount"]/1e9
        dt=t["timestamp"]-t0
        for w,v in tf.items():
            if v>0:
                spent=max(0,-sf.get(w,0)); buy[w]+=spent
                if dt<=60 and spent>0.5: first60.add(w)
                if w in FLEET: fleet_hit.add(w)
            elif v<0: sell[w]+=max(0,sf.get(w,0))
    wash_wallets=[w for w in set(buy)|set(sell) if min(buy[w],sell[w])>1]
    tb=sum(buy.values()); ts=sum(sell.values())
    rt=sum(min(buy[w],sell[w]) for w in wash_wallets)
    big=[w for w in first60 if buy[w]>=10]
    big_sol=sum(buy[w] for w in big)
    wash_ratio=(rt/tb) if tb>0 else 0
    # composite signature score (0..1): fleet recurrence + coordinated fresh big buyers + wash
    score = min(1.0, 0.45*min(len(fleet_hit)/4.0,1) + 0.30*min(len(big)/10.0,1) + 0.25*min(wash_ratio,1))
    # signature wallets = anyone exhibiting the behaviour (round-trippers + coordinated big buyers + dust fleet)
    sig_wallets = sorted(set(wash_wallets) | set(big) | set(fleet_hit))
    return {"mint":mint,"fleet_hits":len(fleet_hit),"fleet_wallets":sorted(fleet_hit),
            "n_big_buyers":len(big),"big_buyer_sol":round(big_sol,1),
            "wash_ratio":round(wash_ratio,4),"wash_sol":round(rt,1),"gross_sol":round(tb+ts,1),
            "score":round(score,3),"sig_wallets":sig_wallets,"wash_wallets":wash_wallets}

# ---------- forward scanner: watch the fingerprint fleet for NEW launches ----------
def scanner():
    log("scanner started; fleet size=%d" % len(FLEET))
    # self-heal: drop any excluded majors/stables that slipped in before exclusion
    try:
        c=db(); q=",".join("?"*len(EXCLUDE))
        c.execute(f"DELETE FROM clusters WHERE mint IN ({q})", tuple(EXCLUDE))
        c.execute(f"DELETE FROM wallets WHERE 0"); c.commit(); c.close()
    except Exception: pass
    seen=set()
    c=db()
    for row in c.execute("SELECT mint FROM clusters"): seen.add(row[0])
    c.close()
    while True:
        try:
            seed_set=set(FLEET)
            watch=discovered_watchlist()  # seed fleet + behaviorally-discovered recurring wallets
            candidate_mints=defaultdict(int)
            # 1) collect recent mints touched by the (growing) fingerprint watchlist
            for w in watch:
                batch=enh_addr(w, limit=15)
                for t in (batch or []):
                    for tt in t.get("tokenTransfers",[]):
                        m=tt.get("mint")
                        if m and m not in EXCLUDE:
                            candidate_mints[m]+=1
                time.sleep(0.3)
            # 2) prioritise mints hit by >=2 fingerprint wallets and not yet analysed deeply
            ranked=sorted(candidate_mints.items(), key=lambda kv:-kv[1])
            checked=0
            for mint,hits in ranked:
                if checked>=8: break
                if hits<1: continue
                res=analyze_mint(mint)
                if not res: continue
                checked+=1
                if res["score"]>=0.25 or res["fleet_hits"]>=1:
                    c=db()
                    prev=c.execute("SELECT first_seen FROM clusters WHERE mint=?",(mint,)).fetchone()
                    fs=prev[0] if prev else int(time.time())
                    c.execute("""INSERT OR REPLACE INTO clusters
                        (mint,first_seen,fleet_hits,fleet_wallets,n_big_buyers,big_buyer_sol,wash_ratio,wash_sol,gross_sol,score,updated,note)
                        VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (mint,fs,res["fleet_hits"],json.dumps(res["fleet_wallets"]),res["n_big_buyers"],
                         res["big_buyer_sol"],res["wash_ratio"],res["wash_sol"],res["gross_sol"],res["score"],
                         int(time.time()),"auto"))
                    c.commit(); c.close()
                    # behaviorally discover + track new fingerprint wallets seen at this cluster
                    record_sig_wallets(mint, res.get("sig_wallets",[]), seed_set)
                    if mint not in seen:
                        seen.add(mint)
                        log(f"NEW candidate {mint[:10]} score={res['score']} fleet={res['fleet_hits']} wash={res['wash_ratio']} sigwallets={len(res.get('sig_wallets',[]))}")
                time.sleep(0.5)
            # promote newly-recurring wallets into the watchlist
            c=db()
            newp=c.execute("SELECT COUNT(*) FROM wallets WHERE recurrence>=2 AND promoted=0").fetchone()[0]
            if newp: c.execute("UPDATE wallets SET promoted=1 WHERE recurrence>=2 AND promoted=0"); c.commit()
            disc=c.execute("SELECT COUNT(*) FROM wallets WHERE recurrence>=2").fetchone()[0]
            c.close()
            log(f"scan cycle done: {len(ranked)} candidate mints, {checked} analysed, watchlist={len(watch)}, discovered_recurring={disc} (+{newp} new)")
        except Exception as e:
            log("scan error: %s" % e)
        time.sleep(int(os.environ.get("SCAN_INTERVAL","180")))

def state():
    c=db()
    rows=[dict(zip([d[0] for d in cur.description],r))
          for cur in [c.execute("""SELECT mint,first_seen,fleet_hits,fleet_wallets,n_big_buyers,big_buyer_sol,
                                    wash_ratio,wash_sol,gross_sol,score,updated,note FROM clusters
                                    ORDER BY score DESC, updated DESC LIMIT 100""")] for r in cur.fetchall()]
    logs=[{"ts":r[0],"msg":r[1]} for r in c.execute("SELECT ts,msg FROM scan_log ORDER BY ts DESC LIMIT 25")]
    disc=[{"addr":r[0],"recurrence":r[1],"seed":r[2],"clusters":json.loads(r[3]),"last_seen":r[4]}
          for r in c.execute("""SELECT addr,recurrence,seed,clusters,last_seen FROM wallets
                                WHERE recurrence>=2 ORDER BY recurrence DESC, last_seen DESC LIMIT 80""")]
    nwallets=c.execute("SELECT COUNT(*) FROM wallets").fetchone()[0]
    c.close()
    for r in rows:
        try: r["fleet_wallets"]=json.loads(r["fleet_wallets"])
        except: r["fleet_wallets"]=[]
    return {"snapshot":SNAP,"snapshot_hash":SNAP_HASH,"fleet":FLEET,
            "candidates":rows,"discovered_wallets":disc,"wallets_tracked":nwallets,
            "scan_log":logs,"server_time":int(time.time())}

PAGE = open("page.html","rb").read() if os.path.exists("page.html") else b"<h1>loading...</h1>"

class H(BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def _send(self, code, body, ctype):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)
    def do_GET(self):
        p=self.path.split("?")[0]
        if p=="/healthz": return self._send(200,b"ok","text/plain")
        if p=="/api/state":
            try: body=json.dumps(state()).encode()
            except Exception as e: body=json.dumps({"error":str(e)}).encode()
            return self._send(200, body, "application/json")
        return self._send(200, PAGE, "text/html; charset=utf-8")

if __name__ == "__main__":
    if HELIUS_KEY:
        threading.Thread(target=scanner, daemon=True).start()
    else:
        print("WARNING: HELIUS_KEY not set; scanner disabled", flush=True)
    port=int(os.environ.get("PORT","8080"))
    print(f"serving on :{port}", flush=True)
    ThreadingHTTPServer(("0.0.0.0",port), H).serve_forever()
