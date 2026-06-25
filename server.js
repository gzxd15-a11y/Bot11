'use strict';
const http   = require('http');
const fs     = require('fs');
const path   = require('path');
const bot    = require('./bot');

const PORT = process.env.PORT || 3000;
const HTML = fs.readFileSync(path.join(__dirname,'index.html'));

// SSE clients list
const clients = new Set();

// Push state to all connected browsers every second
setInterval(()=>{
  if(!clients.size) return;
  const data='data: '+JSON.stringify({
    running:    bot.state.running,
    pair:       bot.state.pair,
    tf:         bot.state.tf,
    leverage:   bot.state.leverage,
    capitalPct: bot.state.capitalPct,
    riskPct:    bot.state.riskPct,
    tpPct:      bot.state.tpPct,
    equity:     bot.state.equity,
    position:   bot.state.position,
    trailMile:  bot.state.trailMilestone,
    stats:      bot.state.stats,
    trades:     bot.state.trades.slice(0,50),
    log:        bot.state.log.slice(0,30),
    price:      bot.state.price,
    connected:  bot.state.connected,
    signal:     bot.state.signal,
  })+'\n\n';
  for(const res of clients){
    try{ res.write(data); }catch{ clients.delete(res); }
  }
},1000);

// HTTP server
const server=http.createServer((req,res)=>{
  const url=new URL(req.url,'http://localhost');

  // SSE endpoint — browser connects here to receive live state
  if(url.pathname==='/events'){
    res.writeHead(200,{
      'Content-Type':'text/event-stream',
      'Cache-Control':'no-cache',
      'Connection':'keep-alive',
      'Access-Control-Allow-Origin':'*',
    });
    res.write('retry: 3000\n\n');
    clients.add(res);
    req.on('close',()=>clients.delete(res));
    return;
  }

  // Control API — browser sends commands here
  if(url.pathname==='/control'&&req.method==='POST'){
    let body='';
    req.on('data',d=>body+=d);
    req.on('end',()=>{
      try{
        const cmd=JSON.parse(body);
        if(cmd.action==='start'){
          bot.state.pair       = cmd.pair||bot.state.pair;
          bot.state.tf         = cmd.tf||bot.state.tf;
          bot.state.leverage   = Number(cmd.leverage)||bot.state.leverage;
          bot.state.capitalPct = Number(cmd.capitalPct)||bot.state.capitalPct;
          bot.state.riskPct    = Number(cmd.riskPct)||bot.state.riskPct;
          bot.state.tpPct      = Number(cmd.tpPct)||bot.state.tpPct;
          bot.startBinance();
          bot.startBot();
        }
        if(cmd.action==='stop'){
          bot.stopBot();
          bot.stopBinance();
        }
        if(cmd.action==='settings'){
          bot.state.leverage   = Number(cmd.leverage)||bot.state.leverage;
          bot.state.capitalPct = Number(cmd.capitalPct)||bot.state.capitalPct;
          bot.state.riskPct    = Number(cmd.riskPct)||bot.state.riskPct;
          bot.state.tpPct      = Number(cmd.tpPct)||bot.state.tpPct;
        }
        res.writeHead(200,{'Content-Type':'application/json'});
        res.end('{"ok":true}');
      }catch(e){
        res.writeHead(400); res.end('{"ok":false}');
      }
    });
    return;
  }

  // Serve UI
  res.writeHead(200,{'Content-Type':'text/html; charset=utf-8'});
  res.end(HTML);
});

server.listen(PORT,'0.0.0.0',()=>{
  console.log('bot1 server running on port '+PORT);
});
