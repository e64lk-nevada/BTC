import os,time,threading,sqlite3,logging,json
from datetime import datetime,timezone
from contextlib import asynccontextmanager
import requests
from fastapi import FastAPI
from fastapi.responses import HTMLResponse,JSONResponse
logging.basicConfig(level=logging.INFO); log=logging.getLogger("signals")
DB=os.getenv("DB_PATH","signals.db"); POLL=max(10,int(os.getenv("POLL_SECONDS","15"))); MIN_SCORE=float(os.getenv("MIN_SCORE","0.72"))
S=requests.Session(); S.headers["User-Agent"]="BTC-Signal-Only/1.0"
state={"status":"starting","updated_at":None,"btc":None,"market":None,"orderbook":None,"signal":{"action":"WAIT","score":0,"reasons":["Збираємо дані"]},"error":None}
def getj(url,params=None):
 r=S.get(url,params=params,timeout=9); r.raise_for_status(); return r.json()
def initdb():
 with sqlite3.connect(DB) as c:c.execute("CREATE TABLE IF NOT EXISTS signals(ts TEXT,action TEXT,score REAL,btc REAL,market TEXT,reasons TEXT)")
def ema(a,n):
 k=2/(n+1); v=a[0]
 for x in a[1:]:v=x*k+v*(1-k)
 return v
def rsi(a):
 d=[a[i]-a[i-1] for i in range(1,len(a))][-14:]; g=sum(max(x,0) for x in d)/14; l=sum(max(-x,0) for x in d)/14
 return 100 if l==0 else 100-100/(1+g/l)
def market():
 now=int(time.time()); t=now-now%300
 for stamp in (t,t-300):
  slug=f"btc-updown-5m-{stamp}"
  try:
   rows=getj("https://gamma-api.polymarket.com/markets",{"slug":slug,"limit":1})
   if rows:
    m=rows[0]; out=m.get("outcomes",[]); prices=m.get("outcomePrices",[]); tokens=m.get("clobTokenIds",[])
    if isinstance(out,str):out=json.loads(out)
    if isinstance(prices,str):prices=json.loads(prices)
    if isinstance(tokens,str):tokens=json.loads(tokens)
    up=next((float(prices[i]) for i,x in enumerate(out) if str(x).lower()=="up" and i<len(prices)),None)
    return {"slug":slug,"question":m.get("question"),"up_price":up,"tokens":tokens}
  except Exception as e:log.warning("Gamma: %s",e)
 return None
def book(m):
 try:
  ids=m.get("tokens",[]) if m else []
  if not ids:return None
  b=getj("https://clob.polymarket.com/book",{"token_id":ids[0]})
  bids=b.get("bids",[]);asks=b.get("asks",[])
  bid=max((float(x["price"]) for x in bids),default=None);ask=min((float(x["price"]) for x in asks),default=None)
  return {"bid":bid,"ask":ask,"spread":ask-bid if bid is not None and ask is not None else None}
 except Exception as e:log.warning("CLOB: %s",e);return None
def evaluate(c,v,m,b):
 p=c[-1]; a=(p/c[-2]-1)*100; z=(p/c[-4]-1)*100; q=(p/c[-6]-1)*100
 trend=ema(c[-20:],8)>ema(c[-20:],18); up=a>0 and z>0 and q>0; down=a<0 and z<0 and q<0
 vr=v[-1]/(sum(v[-11:-1])/10) if sum(v[-11:-1]) else 0; rr=rsi(c)
 vu=sum((up,trend,rr>52)); vd=sum((down,not trend,rr<48)); direction="UP" if vu>vd else "DOWN" if vd>vu else "WAIT"
 score=max(vu,vd)/3; why=[f"Імпульс 1/3/5хв: {a:+.3f}% / {z:+.3f}% / {q:+.3f}%",f"RSI(14): {rr:.1f}",f"Обсяг: {vr:.2f}x"]
 if m and m.get("up_price") is not None:
  u=m["up_price"];why.append(f"Polymarket UP: {u:.3f}")
  if direction=="UP" and u>.72:score-=.12;why.append("UP вже дорогий")
  if direction=="DOWN" and u<.28:score-=.12;why.append("DOWN вже дорогий")
 if b and b.get("spread") is not None:
  why.append(f"Спред UP: {b['spread']:.3f}")
  if b["spread"]>.06:score-=.15;why.append("Широкий спред")
 if vr<.55:score-=.12;why.append("Низький обсяг")
 score=max(0,min(1,score)); action=direction if direction!="WAIT" and score>=MIN_SCORE else "WAIT"
 why.append("Фільтри виконані; сигнал не гарантує результат" if action!="WAIT" else "Недостатньо підтверджень — пропуск")
 return {"action":action,"score":round(score,3),"reasons":why,"metrics":{"m1_pct":round(a,4),"m3_pct":round(z,4),"m5_pct":round(q,4),"rsi":round(rr,1),"volume_ratio":round(vr,2),"votes_up":vu,"votes_down":vd}}
def worker():
 initdb()
 while True:
  try:
   k=getj("https://api.binance.com/api/v3/klines",{"symbol":"BTCUSDT","interval":"1m","limit":30})
   c=[float(x[4]) for x in k];v=[float(x[5]) for x in k];m=market();b=book(m);sig=evaluate(c,v,m,b);ts=datetime.now(timezone.utc).isoformat()
   state.update(status="online",updated_at=ts,btc=c[-1],market=m,orderbook=b,signal=sig,error=None)
   if sig["action"]!="WAIT":
    with sqlite3.connect(DB) as db:db.execute("INSERT INTO signals VALUES(?,?,?,?,?,?)",(ts,sig["action"],sig["score"],c[-1],m.get("slug") if m else None," | ".join(sig["reasons"])))
  except Exception as e:
   log.exception("update failed");state.update(status="degraded",error=str(e),updated_at=datetime.now(timezone.utc).isoformat(),signal={"action":"WAIT","score":0,"reasons":["Помилка джерела даних. Нових входів немає."]})
  time.sleep(POLL)
@asynccontextmanager
async def lifespan(app):
 threading.Thread(target=worker,daemon=True).start();yield
app=FastAPI(title="BTC Signal Only",lifespan=lifespan)
@app.get("/health")
def health():return {"status":state["status"]}
@app.get("/api/status")
def api():return JSONResponse(state)
@app.get("/",response_class=HTMLResponse)
def home():return HTML
HTML=r'''<!doctype html><html lang="uk"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"><meta name="theme-color" content="#0b1220"><title>BTC Signal Only</title><style>
*{box-sizing:border-box}body{margin:0;background:#0b1220;color:#e8eef8;font:16px -apple-system,BlinkMacSystemFont,Segoe UI,sans-serif;padding:env(safe-area-inset-top) 16px env(safe-area-inset-bottom)}main{max-width:680px;margin:auto;padding:20px 0}.top,.row{display:flex;justify-content:space-between;gap:12px}.top{align-items:center;color:#9fb0c8;font-size:13px}.card{background:#131f32;border:1px solid #263650;border-radius:18px;padding:20px;margin:14px 0}.signal{text-align:center;padding:30px 15px}.action{font-size:42px;font-weight:850}.UP{color:#40d69a}.DOWN{color:#ff7272}.WAIT{color:#f4c66a}.muted,.small{color:#9fb0c8}.price{font-size:28px;font-weight:750;margin:8px}.row{padding:9px 0;border-bottom:1px solid #263650}.row:last-child{border:0}h1{font-size:21px;margin:0;color:white}h2{font-size:16px;margin:0 0 12px}.small{font-size:12px;line-height:1.5}.pill{padding:5px 9px;border-radius:30px;background:#22334d}.warning{color:#f4c66a}ul{padding-left:20px;line-height:1.55;color:#c7d4e5}footer{text-align:center;color:#70839e;font-size:12px;padding:12px}</style></head><body><main><div class="top"><h1>₿ BTC SIGNAL ONLY</h1><span class="pill" id="status">Підключення…</span></div><div class="card signal"><div class="muted">СИГНАЛ · POLYMARKET BTC 5M</div><div id="action" class="action WAIT">WAIT</div><div id="score" class="small">Очікування даних</div><div id="btc" class="price">—</div><div id="market" class="muted">Ринок завантажується…</div></div><div class="card"><h2>Причини</h2><ul id="reasons"><li>Збираємо дані…</li></ul></div><div class="card"><h2>Показники</h2><div id="metrics"></div></div><div class="card"><div class="row"><span class="muted">Оновлено</span><b id="updated">—</b></div><div class="row"><span class="muted">Режим</span><b>Тільки сигнали · без торгівлі</b></div></div><p class="small warning">Сигнали можуть бути хибними. 5-хвилинні ринки дуже волатильні. Це не фінансова порада.</p><footer>Оновлення кожні 15 секунд · Safari → Поділитися → На початковий екран</footer></main><script>
const $=x=>document.getElementById(x);async function refresh(){try{let d=await(await fetch('/api/status',{cache:'no-store'})).json(),s=d.signal||{},m=s.metrics||{};$('status').textContent=d.status==='online'?'● ONLINE':'⚠ '+d.status;$('action').textContent=s.action||'WAIT';$('action').className='action '+(s.action||'WAIT');$('score').textContent='Сила сигналу: '+Math.round((s.score||0)*100)+'%';$('btc').textContent=d.btc?'$'+Number(d.btc).toLocaleString('en-US',{maximumFractionDigits:2}):'—';$('market').textContent=d.market?(d.market.question||d.market.slug):'Ринок не знайдено';$('reasons').textContent='';(s.reasons||[]).forEach(x=>{let li=document.createElement('li');li.textContent=x;$('reasons').append(li)});let vals=[['Momentum 1m',m.m1_pct+'%'],['Momentum 3m',m.m3_pct+'%'],['Momentum 5m',m.m5_pct+'%'],['RSI',m.rsi],['Volume',m.volume_ratio+'x'],['Votes UP/DOWN',(m.votes_up??'—')+' / '+(m.votes_down??'—')]];$('metrics').textContent='';vals.forEach(x=>{let r=document.createElement('div');r.className='row';let a=document.createElement('span'),b=document.createElement('b');a.className='muted';a.textContent=x[0];b.textContent=x[1]??'—';r.append(a,b);$('metrics').append(r)});$('updated').textContent=d.updated_at?new Date(d.updated_at).toLocaleTimeString():'—'}catch(e){$('status').textContent='RECONNECTING';$('action').textContent='WAIT';$('action').className='action WAIT'}}refresh();setInterval(refresh,15000);</script></body></html>'''
