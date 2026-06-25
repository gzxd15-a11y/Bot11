'use strict';
// ============================================================
// BOT1 — Trading Engine (runs fully on Node.js server)
// Binance WebSocket + REST for live prices
// All indicator logic, signal engine, and trade management
// runs here — browser only shows results via SSE
// ============================================================
const fetch = require('node-fetch');
const WebSocket = require('ws');

// ---- Math helpers ----
const ema = (d,p) => {
  const k=2/(p+1);
  return d.reduce((a,v,i)=>{ a.push(i===0?v:v*k+a[i-1]*(1-k)); return a; },[]);
};
const sma = (d,p) => d.map((_,i)=>{
  const s=d.slice(Math.max(0,i-p+1),i+1);
  return s.reduce((a,b)=>a+b,0)/s.length;
});
const stdv = (d,p) => {
  const s=d.slice(-p), m=s.reduce((a,b)=>a+b,0)/s.length;
  return Math.sqrt(s.reduce((a,b)=>a+(b-m)**2,0)/s.length)||0.0001;
};
const rsiOf = (d,p=14) => {
  if(d.length<p+1) return 50;
  let g=0,l=0;
  for(let i=1;i<=p;i++){ const x=d[i]-d[i-1]; x>0?g+=x:l-=x; }
  let ag=g/p,al=l/p;
  for(let i=p+1;i<d.length;i++){
    const x=d[i]-d[i-1];
    ag=(ag*(p-1)+Math.max(x,0))/p; al=(al*(p-1)+Math.max(-x,0))/p;
  }
  return al===0?100:100-100/(1+ag/al);
};
const atrArr = (hi,lo,cl,p=14) => {
  const tr=cl.map((_,i)=>i===0?hi[i]-lo[i]:
    Math.max(hi[i]-lo[i],Math.abs(hi[i]-cl[i-1]),Math.abs(lo[i]-cl[i-1])));
  return sma(tr,p);
};

// ---- Indicators ----
function calcEMA(cl){
  if(cl.length<55) return null;
  const e21=ema(cl,21),e55=ema(cl,55);
  const e200=cl.length>=200?ema(cl,200):null;
  const L=cl.length-1;
  return {
    e21:e21[L],e55:e55[L],e200:e200?e200[L]:null,
    bullStack:e21[L]>e55[L], bearStack:e21[L]<e55[L],
    aboveE200:e200?cl[L]>e200[L]:null,
    belowE200:e200?cl[L]<e200[L]:null,
    slope21:(e21[L]-e21[L-3])/e21[L]*100,
  };
}
function calcST(hi,lo,cl,p=10,m=3){
  if(cl.length<p+5) return null;
  const atr=atrArr(hi,lo,cl,p);
  const hl2=cl.map((_,i)=>(hi[i]+lo[i])/2);
  const rU=hl2.map((v,i)=>v+m*atr[i]);
  const rL=hl2.map((v,i)=>v-m*atr[i]);
  const fU=[...rU],fL=[...rL];
  for(let i=1;i<cl.length;i++){
    fU[i]=fU[i]<fU[i-1]||cl[i-1]>fU[i-1]?fU[i]:fU[i-1];
    fL[i]=fL[i]>fL[i-1]||cl[i-1]<fL[i-1]?fL[i]:fL[i-1];
  }
  const dirs=new Array(cl.length).fill(1);
  for(let i=p;i<cl.length;i++){
    dirs[i]=dirs[i-1]===1?(cl[i]<fL[i]?-1:1):(cl[i]>fU[i]?1:-1);
  }
  const L=cl.length-1;
  return {
    bull:dirs[L]===1,bear:dirs[L]===-1,
    flipUp:dirs[L]===1&&dirs[L-1]===-1,
    flipDown:dirs[L]===-1&&dirs[L-1]===1,
  };
}
function calcWT(cl,n1=10,n2=21){
  if(cl.length<n2+10) return null;
  const esa=ema(cl,n1);
  const d=cl.map((c,i)=>Math.abs(c-esa[i]));
  const de=ema(d,n1);
  const ci=cl.map((c,i)=>(c-esa[i])/(0.015*(de[i]||1e-9)));
  const wt1=ema(ci,n2),wt2=sma(wt1,4);
  const L=wt1.length-1;
  const freshBull=wt1[L]>wt2[L]&&wt1[L-1]<=wt2[L-1];
  const freshBear=wt1[L]<wt2[L]&&wt1[L-1]>=wt2[L-1];
  const recBull=freshBull
    ||(wt1[L-1]>wt2[L-1]&&wt1[L-2]<=wt2[L-2])
    ||(wt1[L-2]>wt2[L-2]&&wt1[L-3]<=wt2[L-3])
    ||(wt1[L-3]>wt2[L-3]&&wt1[L-4]<=wt2[L-4]);
  const recBear=freshBear
    ||(wt1[L-1]<wt2[L-1]&&wt1[L-2]>=wt2[L-2])
    ||(wt1[L-2]<wt2[L-2]&&wt1[L-3]>=wt2[L-3])
    ||(wt1[L-3]<wt2[L-3]&&wt1[L-4]>=wt2[L-4]);
  return {
    wt1:wt1[L],wt2:wt2[L],
    freshBull,freshBear,recBull,recBear,
    bullMom:wt1[L]>wt2[L],bearMom:wt1[L]<wt2[L],
    os:wt1[L]<-53,os2:wt1[L]<-75,
    ob:wt1[L]>53,ob2:wt1[L]>75,
    goldBull:recBull&&wt1[L]<-40,
    goldBear:recBear&&wt1[L]>40,
  };
}
function calcRSI(cl){
  const r14=rsiOf(cl,14);
  const rPrev=cl.length>20?rsiOf(cl.slice(0,-3),14):r14;
  return {
    r14,rising:r14>rPrev,falling:r14<rPrev,
    os:r14<35,ob:r14>65,deepOs:r14<25,deepOb:r14>75,
    okLong:r14>=35&&r14<=68,okShort:r14<=65&&r14>=32,
  };
}
function calcATR(hi,lo,cl,p=14){
  if(cl.length<p+2) return null;
  const a=atrArr(hi,lo,cl,p);
  const v=a[a.length-1];
  return {atr:v,pct:(v/cl[cl.length-1])*100};
}
function calcBB(cl,p=20){
  if(cl.length<p+2) return null;
  const basis=sma(cl,p),L=cl.length-1,dev=stdv(cl,p);
  const upper=basis[L]+2*dev,lower=basis[L]-2*dev;
  const bw=(upper-lower)/basis[L]*100;
  const pctB=(cl[L]-lower)/(upper-lower||1);
  return {upper,lower,mid:basis[L],bw,pctB,
    nearLower:pctB<0.25,nearUpper:pctB>0.75,
    atLower:pctB<0.1,atUpper:pctB>0.9,squeezed:bw<1.0};
}
function calcSQZ(hi,lo,cl,len=20){
  if(cl.length<len+8) return null;
  const basis=sma(cl,len);
  const dev=cl.map((_,i)=>stdv(cl.slice(0,i+1),Math.min(i+1,len)));
  const bbU=basis.map((b,i)=>b+2*dev[i]);
  const bbL=basis.map((b,i)=>b-2*dev[i]);
  const atr=atrArr(hi,lo,cl,len),kcM=sma(cl,len);
  const kcU=kcM.map((m,i)=>m+1.5*atr[i]);
  const kcL=kcM.map((m,i)=>m-1.5*atr[i]);
  const L=cl.length-1;
  const sqzOn=bbU[L]<kcU[L]&&bbL[L]>kcL[L];
  const sqzOff=!sqzOn&&(bbU[L-1]<kcU[L-1]);
  const mom=cl.map((c,i)=>{
    if(i<len-1) return 0;
    const hh=Math.max(...hi.slice(Math.max(0,i-len+1),i+1));
    const ll=Math.min(...lo.slice(Math.max(0,i-len+1),i+1));
    return c-(hh+ll+kcM[i]*2)/4;
  });
  const mE=ema(mom,3);
  return {sqzOn,sqzOff,mom:mE[L],rising:mE[L]>mE[L-1],
    bullFire:sqzOff&&mE[L]>0,bearFire:sqzOff&&mE[L]<0};
}
function calcMCB(hi,lo,cl){
  const wt=calcWT(cl),sqz=calcSQZ(hi,lo,cl);
  if(!wt||!sqz) return null;
  const hlc3=cl.map((c,i)=>(hi[i]+lo[i]+c)/3);
  const mfi=(rsiOf(hlc3,14)-50)/50;
  const L=cl.length-1;
  const priceUp=cl[L]>cl[Math.max(0,L-6)];
  const wS=wt.wt1?[]:[];
  const bullDiv=false,bearDiv=false;
  return {...wt,sqzOn:sqz.sqzOn,sqzOff:sqz.sqzOff,
    mom:sqz.mom,rising:sqz.rising,
    bullFire:sqz.bullFire,bearFire:sqz.bearFire,
    mfi,bullDiv,bearDiv};
}
function calcVWAP(hi,lo,cl){
  if(cl.length<20) return null;
  const n=Math.min(cl.length,80);
  const hs=hi.slice(-n),ls=lo.slice(-n),cs=cl.slice(-n);
  let cpv=0,cv=0;
  for(let i=0;i<n;i++){
    const tp=(hs[i]+ls[i]+cs[i])/3,v=Math.max(hs[i]-ls[i],0.0001);
    cpv+=tp*v; cv+=v;
  }
  const vwap=cpv/cv,price=cs[cs.length-1];
  return {vwap,bull:price>vwap,bear:price<vwap,pct:((price-vwap)/vwap)*100};
}

// ---- Signal engine ----
function computeSignal(hi,lo,cl,openPos){
  if(cl.length<60) return {action:null,ls:0,ss:0,reason:'Loading'};
  const E=calcEMA(cl),S=calcST(hi,lo,cl),W=calcWT(cl);
  const M=calcMCB(hi,lo,cl),R=calcRSI(cl),A=calcATR(hi,lo,cl);
  const B=calcBB(cl),Q=calcSQZ(hi,lo,cl),V=calcVWAP(hi,lo,cl);
  if(!E||!S||!R||!A) return {action:null,ls:0,ss:0,reason:'Calc'};

  const L10=cl.length-1;
  const priceUp10=cl.length>=11&&cl[L10]>cl[L10-10]*1.003;
  const priceDown10=cl.length>=11&&cl[L10]<cl[L10-10]*0.997;

  // EXIT
  if(openPos){
    const d=openPos.dir;
    const trendIntactL=E.bullStack&&S.bull;
    const trendIntactS=E.bearStack&&S.bear;
    if(d==='LONG'&&S.flipDown)              return {action:'CLOSE',reason:'ST flip down'};
    if(d==='SHORT'&&S.flipUp)               return {action:'CLOSE',reason:'ST flip up'};
    if(d==='LONG'&&W&&W.freshBear&&R.r14>58) return {action:'CLOSE',reason:'WT cross down'};
    if(d==='SHORT'&&W&&W.freshBull&&R.r14<42) return {action:'CLOSE',reason:'WT cross up'};
    if(d==='LONG'&&B&&B.atUpper&&R.ob&&W&&W.bearMom&&!trendIntactL) return {action:'CLOSE',reason:'BB upper'};
    if(d==='SHORT'&&B&&B.atLower&&R.os&&W&&W.bullMom&&!trendIntactS) return {action:'CLOSE',reason:'BB lower'};
    if(d==='LONG'&&R.deepOb&&W&&W.bearMom&&!trendIntactL) return {action:'CLOSE',reason:'RSI deepOB'};
    if(d==='SHORT'&&R.deepOs&&W&&W.bullMom&&!trendIntactS) return {action:'CLOSE',reason:'RSI deepOS'};
    return {action:null,reason:'HOLD '+d,ls:0,ss:0};
  }

  // SCORE
  let ls=0,ss=0,lR=[],sR=[];
  if(E.bullStack){ls+=2.5;lR.push('EMA^');}else{ss+=0.5;}
  if(E.bearStack){ss+=2.5;sR.push('EMAv');}else{ls+=0.5;}
  if(E.aboveE200===true){ls+=1.5;lR.push('E200^');}
  if(E.belowE200===true){ss+=1.5;sR.push('E200v');}
  if(E.slope21>0.02){ls+=0.5;}else if(E.slope21<-0.02){ss+=0.5;}
  if(S.bull){ls+=2.5;lR.push('ST^');}
  if(S.bear){ss+=2.5;sR.push('STv');}
  if(S.flipUp){ls+=2;lR.push('ST-flip^');}
  if(S.flipDown){ss+=2;sR.push('ST-flipv');}
  if(V&&V.bull){ls+=1;lR.push('VWAP^');}
  if(V&&V.bear){ss+=1;sR.push('VWAPv');}

  if(W){
    if(W.goldBull){ls+=5;lR.push('WT-Gold^');}
    else if(W.freshBull){ls+=3;lR.push('WT^');}
    else if(W.recBull){ls+=2;lR.push('WT^~');}
    if(W.goldBear){ss+=5;sR.push('WT-Goldv');}
    else if(W.freshBear){ss+=3;sR.push('WTv');}
    else if(W.recBear){ss+=2;sR.push('WTv~');}
    if(W.bullMom){ls+=1;}
    if(W.bearMom){ss+=1;}
    if(W.os2){ls+=1.5;lR.push('VOS');}
    if(W.ob2){ss+=1.5;sR.push('VOB');}
  }
  if(M){
    if(M.bullDiv){ls+=2.5;lR.push('BullDiv');}
    if(M.bearDiv){ss+=2.5;sR.push('BearDiv');}
    if(M.bullFire){ls+=2;lR.push('SQZ-Fire^');}
    if(M.bearFire){ss+=2;sR.push('SQZ-Firev');}
    if(M.mfi>0.3){ls+=0.5;}
    if(M.mfi<-0.3){ss+=0.5;}
  }
  if(Q&&!M){
    if(Q.bullFire){ls+=2;lR.push('SQZ^');}
    if(Q.bearFire){ss+=2;sR.push('SQZv');}
  }

  const confL=(E.bullStack?1:0)+(S.bull?1:0)+(W?(W.bullMom?1:0):1);
  const confS=(E.bearStack?1:0)+(S.bear?1:0)+(W?(W.bearMom?1:0):1);
  const trendStrongL=confL>=2,trendStrongS=confS>=2;
  if(trendStrongL){ls+=2;lR.push('TrendCont^');}
  if(trendStrongS){ss+=2;sR.push('TrendContv');}

  if(R.okLong&&R.rising){ls+=2;lR.push('RSI^');}
  else if(trendStrongL&&R.r14>=50){ls+=1.5;lR.push('RSI-trend^');}
  else if(R.r14>50){ls+=0.5;}
  if(R.okShort&&R.falling){ss+=2;sR.push('RSIv');}
  else if(trendStrongS&&R.r14<=50){ss+=1.5;sR.push('RSI-trendv');}
  else if(R.r14<50){ss+=0.5;}
  if(R.deepOs&&W&&W.bullMom){ls+=1.5;lR.push('RSI-OS');}
  if(R.deepOb&&W&&W.bearMom){ss+=1.5;sR.push('RSI-OB');}
  if(B&&B.nearLower&&!B.squeezed){ls+=1;lR.push('BB-L');}
  if(B&&B.nearUpper&&!B.squeezed){ss+=1;sR.push('BB-U');}
  if(trendStrongL&&B&&B.atUpper){ls+=1;lR.push('BB-ride^');}
  if(trendStrongS&&B&&B.atLower){ss+=1;sR.push('BB-ridev');}
  if(priceUp10){ls+=2;lR.push('Price10^');}
  if(priceDown10){ss+=2;sR.push('Price10v');}

  const lRSI=trendStrongL?R.r14<90:R.r14<78;
  const lBB=trendStrongL?true:!(B&&B.atUpper);
  const sRSI=trendStrongS?R.r14>10:R.r14>22;
  const sBB=trendStrongS?true:!(B&&B.atLower);
  const lTrend=E.bullStack||S.bull;
  const lMom=W?(W.recBull||W.goldBull||W.os2||W.bullMom||priceUp10):(M?(M.bullDiv||M.bullFire||priceUp10):(Q?(Q.bullFire||priceUp10):priceUp10));
  const sTrend=E.bearStack||S.bear;
  const sMom=W?(W.recBear||W.goldBear||W.ob2||W.bearMom||priceDown10):(M?(M.bearDiv||M.bearFire||priceDown10):(Q?(Q.bearFire||priceDown10):priceDown10));

  const canL=ls>=5&&lTrend&&lMom&&lRSI&&lBB&&ls>ss;
  const canS=ss>=5&&sTrend&&sMom&&sRSI&&sBB&&ss>ls;

  if(canL) return {action:'OPEN_LONG',reason:lR.join('+'),strength:Math.min(Math.round(ls/2),6),ls,ss};
  if(canS) return {action:'OPEN_SHORT',reason:sR.join('+'),strength:Math.min(Math.round(ss/2),6),ls,ss};
  return {action:null,reason:'L:'+ls.toFixed(1)+' S:'+ss.toFixed(1),ls,ss};
}

// ---- Bot state ----
const INIT_EQUITY = 10000;
let state = {
  running:      false,
  pair:         'BTC/USDT',
  tf:           '1h',
  leverage:     5,
  capitalPct:   20,
  riskPct:      2,
  tpPct:        50,   // fixed TP target % (leveraged pnl)
  equity:       INIT_EQUITY,
  position:     null,
  trailMilestone: 0,
  stats:        {wins:0,losses:0,pnl:0,trades:0},
  trades:       [],
  log:          [],
  candles:      [],
  price:        0,
  connected:    false,
};

// ---- Logging ----
function addLog(msg, type='info'){
  const ts = new Date().toLocaleTimeString('bg-BG');
  state.log.unshift({msg,type,ts});
  if(state.log.length>150) state.log.pop();
  console.log('['+type.toUpperCase()+'] '+msg);
}

// ---- Binance ----
const BN_REST = 'https://api.binance.com/api/v3';
const BN_WS   = 'wss://stream.binance.com:9443/ws';
const toSym   = p => p.replace('/','').toLowerCase();

async function loadKlines(sym, interval, limit=260){
  const url=`${BN_REST}/klines?symbol=${sym.toUpperCase()}&interval=${interval}&limit=${limit}`;
  const r = await fetch(url);
  if(!r.ok) throw new Error('HTTP '+r.status);
  const raw = await r.json();
  return raw.map(k=>({
    open:parseFloat(k[1]),high:parseFloat(k[2]),
    low:parseFloat(k[3]),close:parseFloat(k[4]),time:k[0]
  }));
}

// WS management
let wsPrice=null, wsKline=null;
let reconnectTimer=null, heartbeatTimer=null, lastMsg=Date.now();
let wsActive=false;

function scheduleReconnect(){
  if(!wsActive) return;
  state.connected=false;
  clearTimeout(reconnectTimer);
  addLog('WS disconnected - reconnecting in 3s...','warn');
  reconnectTimer=setTimeout(connectBinance,3000);
}

function connectBinance(){
  if(!wsActive) return;
  clearTimeout(reconnectTimer);
  clearInterval(heartbeatTimer);
  if(wsPrice){try{wsPrice.close();}catch{}}
  if(wsKline){try{wsKline.close();}catch{}}

  const sym=toSym(state.pair);
  const interval=state.tf;

  // Load fresh candles
  loadKlines(sym,interval,260)
    .then(data=>{
      state.candles=data;
      state.price=data[data.length-1].close;
      addLog('Binance: '+data.length+' candles loaded ('+state.pair+' '+interval+')','info');
    })
    .catch(e=>addLog('REST error: '+e.message,'warn'));

  // Price WS
  wsPrice=new WebSocket(`${BN_WS}/${sym}@aggTrade`);
  wsPrice.on('open',()=>{
    state.connected=true;
    lastMsg=Date.now();
    addLog('Binance WS connected: '+state.pair,'info');
    // Heartbeat: reconnect if no data for 20s
    heartbeatTimer=setInterval(()=>{
      if(Date.now()-lastMsg>20000){
        addLog('No data for 20s - forcing reconnect','warn');
        connectBinance();
      }
    },5000);
  });
  wsPrice.on('message',raw=>{
    try{
      const price=parseFloat(JSON.parse(raw).p);
      if(!price||isNaN(price)) return;
      lastMsg=Date.now();
      state.price=price;
      const arr=state.candles;
      if(arr.length){
        const last=arr[arr.length-1];
        arr[arr.length-1]={...last,close:price,
          high:Math.max(last.high,price),low:Math.min(last.low,price)};
      }
    }catch{}
  });
  wsPrice.on('close',scheduleReconnect);
  wsPrice.on('error',scheduleReconnect);

  // Kline WS
  wsKline=new WebSocket(`${BN_WS}/${sym}@kline_${interval}`);
  wsKline.on('message',raw=>{
    try{
      const {k}=JSON.parse(raw);
      const c={open:parseFloat(k.o),high:parseFloat(k.h),
               low:parseFloat(k.l),close:parseFloat(k.c),time:k.t};
      const arr=state.candles;
      if(!arr.length) return;
      if(k.t===arr[arr.length-1].time) arr[arr.length-1]=c;
      else if(k.x){arr.push(c);if(arr.length>350)arr.shift();}
    }catch{}
  });
  wsKline.on('error',()=>{});
}

function startBinance(){
  wsActive=true;
  connectBinance();
}
function stopBinance(){
  wsActive=false;
  state.connected=false;
  clearTimeout(reconnectTimer);
  clearInterval(heartbeatTimer);
  if(wsPrice){try{wsPrice.close();}catch{}}
  if(wsKline){try{wsKline.close();}catch{}}
}

// ---- Trade management ----
function openTrade(dir,price){
  const {leverage:lev,capitalPct:cp,riskPct:rp,tpPct:tpp,equity:eq} = state;
  const margin   = eq*(cp/100);
  if(margin<5){addLog('Insufficient capital','warn');return;}
  const notional = margin*lev;
  const qty      = notional/price;
  // ATR-based SL
  const arr=state.candles;
  const cl=arr.map(c=>c.close),hi=arr.map(c=>c.high),lo=arr.map(c=>c.low);
  const atrVal=calcATR(hi,lo,cl)?.atr??0;
  const slD=atrVal>0?Math.max(atrVal*2.0,price*(rp/lev/100)):price*(rp/lev/100);
  // Fixed TP expressed as leveraged pnl %
  const tpD=(tpp/100/lev)*price;
  const sl=dir==='LONG'?price-slD:price+slD;
  const tp=dir==='LONG'?price+tpD:price-tpD;
  const liqP=dir==='LONG'?price*(1-0.9/lev):price*(1+0.9/lev);

  state.equity=eq-margin;
  state.trailMilestone=0;
  state.position={
    dir,pair:state.pair,entryPrice:price,qty,margin,leverage:lev,notional,
    sl,tp,origSL:sl,liqPrice:liqP,
    pnl:0,pnlPct:0,currentPrice:price,
    time:new Date().toLocaleTimeString('bg-BG'),reason:'',
  };
  addLog('['+dir+'] '+state.pair+' @ '+price.toFixed(4)+
    ' | SL:'+sl.toFixed(4)+' TP:'+tp.toFixed(4)+
    ' | '+lev+'x | Margin:'+margin.toFixed(2)+'$','success');
}

function closeTrade(price,reason){
  const pos=state.position;
  if(!pos) return;
  const raw=pos.dir==='LONG'?(price-pos.entryPrice)*pos.qty:(pos.entryPrice-price)*pos.qty;
  const pnl=raw*pos.leverage;
  const pct=(raw/pos.entryPrice)*100*pos.leverage;
  const win=pnl>=0;
  state.equity+=pos.margin+pnl;
  state.position=null;
  state.trailMilestone=0;
  state.stats.wins+=win?1:0;
  state.stats.losses+=win?0:1;
  state.stats.pnl+=pnl;
  state.stats.trades+=1;
  const trade={
    id:Date.now(),pair:pos.pair,dir:pos.dir,
    entry:pos.entryPrice,exit:price,
    qty:pos.qty,margin:pos.margin,lev:pos.leverage,
    pnl,pnlPct:pct,
    time:new Date().toLocaleTimeString('bg-BG'),reason
  };
  state.trades.unshift(trade);
  if(state.trades.length>100) state.trades.pop();
  addLog((win?'[OK]':'[LOSS]')+' '+pos.dir+' '+pos.pair+
    ' @ '+price.toFixed(4)+
    ' | PnL:'+(pnl>=0?'+':'')+pnl.toFixed(2)+
    '$ ('+pct.toFixed(2)+'%) | '+reason,win?'success':'warn');
}

// ---- Bot loop (every 2s) ----
let botTimer=null;
function runBotTick(){
  if(!state.running||!state.candles.length) return;
  const arr=state.candles;
  if(arr.length<65) return;
  const cl=arr.map(c=>c.close),hi=arr.map(c=>c.high),lo=arr.map(c=>c.low);
  const price=state.price||cl[cl.length-1];
  const pos=state.position;

  // Update floating PnL
  if(pos){
    const raw=pos.dir==='LONG'?(price-pos.entryPrice)*pos.qty:(pos.entryPrice-price)*pos.qty;
    pos.pnl=raw*pos.leverage;
    pos.pnlPct=(raw/pos.entryPrice)*100*pos.leverage;
    pos.currentPrice=price;
  }

  // TP / SL check
  if(pos){
    const hitTP=pos.dir==='LONG'?price>=pos.tp:price<=pos.tp;
    const hitSL=pos.dir==='LONG'?price<=pos.sl:price>=pos.sl;
    if(hitTP){ closeTrade(price,'Take Profit hit'); return; }
    if(hitSL){
      const isProfit=(pos.dir==='LONG'?price-pos.entryPrice:pos.entryPrice-price)>=0;
      closeTrade(price,isProfit?'Trailing SL (profit locked)':'Stop Loss hit');
      return;
    }

    // Progressive trailing SL:
    // +10% pnl -> SL locks +2%
    // +20% pnl -> SL locks +10%
    // +30% pnl -> SL locks +20%
    // every step thereafter: locks (milestone-1)*10%
    const rawMove=pos.dir==='LONG'?price-pos.entryPrice:pos.entryPrice-price;
    const pnlPctNow=(rawMove/pos.entryPrice)*100*pos.leverage;
    const milestone=Math.floor(pnlPctNow/10);   // integer steps at 10,20,30,...
    if(milestone>=1&&milestone>state.trailMilestone){
      state.trailMilestone=milestone;
      const lockPct=milestone===1?2:10*(milestone-1);
      const priceMove=(lockPct/100/pos.leverage)*pos.entryPrice;
      const newSL=pos.dir==='LONG'?pos.entryPrice+priceMove:pos.entryPrice-priceMove;
      const improves=pos.dir==='LONG'?newSL>pos.sl:newSL<pos.sl;
      if(improves){
        pos.sl=newSL;
        addLog('Trailing SL -> locks +'+lockPct+'% pnl @ '+newSL.toFixed(4)+
          '  (position at +'+pnlPctNow.toFixed(1)+'%)','info');
      }
    }
  }

  // Signal
  const sig=computeSignal(hi,lo,cl,pos);

  if(pos&&sig.action==='CLOSE'){
    closeTrade(price,sig.reason);
    return;
  }

  state.signal=sig;
  if(!pos&&(sig.action==='OPEN_LONG'||sig.action==='OPEN_SHORT')){
    openTrade(sig.action==='OPEN_LONG'?'LONG':'SHORT',price);
    state.position.reason=sig.reason;
  }
}

function startBot(){
  state.running=true;
  if(wsActive===false) startBinance();
  clearInterval(botTimer);
  botTimer=setInterval(runBotTick,2000);
  addLog('Bot started','success');
}
function stopBot(){
  state.running=false;
  clearInterval(botTimer);
  addLog('Bot stopped','warn');
}

module.exports={state,startBot,stopBot,startBinance,stopBinance};
