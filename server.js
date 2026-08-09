"use strict";
const express = require("express");
const http = require("http");
const path = require("path");
const fs = require("fs");
const crypto = require("crypto");
const { Server } = require("socket.io");

const APP_VERSION = "6.0.0";
const PORT = Number(process.env.PORT || 3000);
const ROOT = __dirname;
const DATA_FILE = path.join(ROOT, "data.json");
const ADMIN_USER = String(process.env.LAVENDER_ADMIN_USER || "admin");
const ADMIN_PASSWORD = String(process.env.LAVENDER_ADMIN_PASSWORD || "lavender123");
const COOKIE_SECURE = process.env.NODE_ENV === "production";
const SESSION_TTL = 12 * 60 * 60 * 1000;

const app = express();
const httpServer = http.createServer(app);
const io = new Server(httpServer, {
  transports: ["websocket", "polling"],
  pingInterval: 25000,
  pingTimeout: 20000
});
const sessions = new Map();

app.disable("x-powered-by");
app.use(express.json({ limit: "4mb" }));
app.use(express.static(ROOT, {
  index: false,
  etag: true,
  maxAge: process.env.NODE_ENV === "production" ? "5m" : 0
}));

function baseData(){
  return {
    version:1,
    settings:{siteName:"LAVENDER",tagline:"FREE FIRE COMMUNITY",accent:"#b46cff"},
    guilds:[],players:[],matches:[],tournaments:[],news:[],
    overlay:{activeMatchId:null,visible:true,accent:"#b46cff",position:"bottom",showPlayers:true,showStats:true,customText:"LAVENDER • LIVE"}
  };
}
function normalize(d){
  const b = baseData();
  if(!d || typeof d!=="object") d={};
  d.version = Number(d.version)||1;
  d.settings = {...b.settings, ...(d.settings||{})};
  for(const k of ["guilds","players","matches","tournaments","news"]) if(!Array.isArray(d[k])) d[k]=[];
  d.overlay = {...b.overlay, ...(d.overlay||{})};
  return d;
}
function readData(){
  try { return normalize(JSON.parse(fs.readFileSync(DATA_FILE,"utf8"))); }
  catch(err){
    console.error("DATA read failed:", err.message);
    const d=baseData(); atomicWrite(d); return d;
  }
}
function atomicWrite(d){
  const tmp = DATA_FILE + ".tmp";
  fs.writeFileSync(tmp, JSON.stringify(normalize(d), null, 2), "utf8");
  fs.renameSync(tmp, DATA_FILE);
}
function nextId(arr){ return arr.length ? Math.max(...arr.map(x=>Number(x.id)||0))+1 : 1; }
function rankForElo(elo){
  const e=Number(elo)||0;
  return e>=2000?"S":e>=1800?"A":e>=1600?"B":e>=1400?"C":e>=1200?"D":e>=1000?"E":"F";
}
function cleanText(v,max=120){ return String(v??"").trim().slice(0,max); }
function cleanImage(v,fallback){
  v=String(v||"").trim();
  if(!v) return fallback;
  if(v.startsWith("data:image/")) return v.length <= 3_000_000 ? v : fallback;
  return v.slice(0,64);
}
function parseCookies(req){
  const out={};
  String(req.headers.cookie||"").split(";").forEach(part=>{
    const i=part.indexOf("=");
    if(i>0) out[part.slice(0,i).trim()]=decodeURIComponent(part.slice(i+1).trim());
  });
  return out;
}
function sessionToken(req){ return parseCookies(req).lavender_session || ""; }
function isAdmin(req){
  const token=sessionToken(req), s=sessions.get(token);
  if(!s) return false;
  if(Date.now()-s.createdAt>SESSION_TTL){sessions.delete(token);return false}
  return true;
}
function requireAdmin(req,res,next){
  if(!isAdmin(req)) return res.status(401).json({error:"Нужен вход администратора"});
  next();
}
function publicData(d=readData()){
  const guildMap=Object.fromEntries(d.guilds.map(g=>[Number(g.id),g]));
  const players=d.players.map(p=>{
    const wins=Number(p.wins)||0, losses=Number(p.losses)||0, kills=Number(p.kills)||0, deaths=Number(p.deaths)||0;
    return {...p,guild:guildMap[Number(p.guildId)]||null,rank:rankForElo(p.elo),kd:deaths?Number((kills/deaths).toFixed(2)):kills,winrate:(wins+losses)?Math.round(wins/(wins+losses)*100):0};
  }).sort((a,b)=>(Number(b.elo)||0)-(Number(a.elo)||0));
  const playerMap=Object.fromEntries(players.map(p=>[Number(p.id),p]));
  const guilds=d.guilds.map(g=>{
    const roster=players.filter(p=>Number(p.guildId)===Number(g.id));
    const wins=Number(g.wins)||0,losses=Number(g.losses)||0;
    return {...g,rank:rankForElo(g.elo),memberCount:roster.length,roster,winrate:(wins+losses)?Math.round(wins/(wins+losses)*100):0};
  }).sort((a,b)=>(Number(b.elo)||0)-(Number(a.elo)||0));
  const matches=d.matches.map(m=>({...m,guildA:guildMap[Number(m.guildAId)]||null,guildB:guildMap[Number(m.guildBId)]||null,playerA:playerMap[Number(m.playerAId)]||null,playerB:playerMap[Number(m.playerBId)]||null})).sort((a,b)=>Number(b.id)-Number(a.id));
  const tournaments=d.tournaments.map(t=>({...t,participants:(t.guildIds||[]).map(x=>guildMap[Number(x)]).filter(Boolean)})).sort((a,b)=>Number(b.id)-Number(a.id));
  return {...d,players,guilds,matches,tournaments};
}
function overlayState(){
  const d=publicData();
  const m=d.matches.find(x=>Number(x.id)===Number(d.overlay.activeMatchId)) || d.matches[0] || null;
  return {overlay:d.overlay,match:m,settings:d.settings,version:APP_VERSION};
}
function broadcast(){
  io.emit("overlay:update", overlayState());
  io.emit("site:update", {at:Date.now()});
}
function ok(res,data){res.json(data)}
function fail(res,msg,code=400){res.status(code).json({error:msg})}

app.get("/health",(req,res)=>res.json({ok:true,version:APP_VERSION,uptime:Math.round(process.uptime())}));
app.get("/api/all",(req,res)=>ok(res,publicData()));
app.get("/api/overlay",(req,res)=>ok(res,overlayState()));
app.get("/api/auth/status",(req,res)=>ok(res,{authenticated:isAdmin(req),user:isAdmin(req)?ADMIN_USER:null,version:APP_VERSION}));
app.post("/api/auth/login",(req,res)=>{
  const username=String(req.body?.username||"");
  const password=String(req.body?.password||"");
  const userOk=crypto.timingSafeEqual(Buffer.from(username.padEnd(Math.max(username.length,ADMIN_USER.length))),Buffer.from(ADMIN_USER.padEnd(Math.max(username.length,ADMIN_USER.length))));
  const passOk=crypto.timingSafeEqual(Buffer.from(password.padEnd(Math.max(password.length,ADMIN_PASSWORD.length))),Buffer.from(ADMIN_PASSWORD.padEnd(Math.max(password.length,ADMIN_PASSWORD.length))));
  if(!userOk||!passOk) return fail(res,"Неверный логин или пароль",401);
  const token=crypto.randomBytes(32).toString("hex");
  sessions.set(token,{createdAt:Date.now()});
  res.setHeader("Set-Cookie",`lavender_session=${token}; HttpOnly; SameSite=Lax; Path=/; Max-Age=43200${COOKIE_SECURE?"; Secure":""}`);
  ok(res,{ok:true});
});
app.post("/api/auth/logout",(req,res)=>{
  sessions.delete(sessionToken(req));
  res.setHeader("Set-Cookie",`lavender_session=; HttpOnly; SameSite=Lax; Path=/; Max-Age=0${COOKIE_SECURE?"; Secure":""}`);
  ok(res,{ok:true});
});

app.patch("/api/settings",requireAdmin,(req,res)=>{
  const d=readData();
  d.settings.siteName=cleanText(req.body?.siteName||d.settings.siteName,40)||"LAVENDER";
  d.settings.tagline=cleanText(req.body?.tagline||d.settings.tagline,100);
  d.settings.accent=/^#[0-9a-f]{6}$/i.test(req.body?.accent||"")?req.body.accent:d.settings.accent;
  atomicWrite(d);broadcast();ok(res,d.settings);
});

app.post("/api/guilds",requireAdmin,(req,res)=>{
  const d=readData(),b=req.body||{},name=cleanText(b.name,50),tag=cleanText(b.tag,12).toUpperCase();
  if(!name||!tag) return fail(res,"Укажи название и тег");
  if(d.guilds.some(g=>String(g.tag).toUpperCase()===tag)) return fail(res,"Такой тег уже существует");
  const g={id:nextId(d.guilds),name,tag,logo:cleanImage(b.logo,"🪻"),region:cleanText(b.region||"Кыргызстан",40),elo:Number(b.elo)||1200,wins:Number(b.wins)||0,losses:Number(b.losses)||0,description:cleanText(b.description,400),captain:cleanText(b.captain,50)};
  d.guilds.push(g);atomicWrite(d);broadcast();ok(res,g);
});
app.patch("/api/guilds/:id",requireAdmin,(req,res)=>{
  const d=readData(),g=d.guilds.find(x=>Number(x.id)===Number(req.params.id));
  if(!g)return fail(res,"Гильдия не найдена",404);
  const b=req.body||{};
  if("name"in b)g.name=cleanText(b.name,50); if("tag"in b)g.tag=cleanText(b.tag,12).toUpperCase();
  if("logo"in b)g.logo=cleanImage(b.logo,g.logo||"🪻"); if("region"in b)g.region=cleanText(b.region,40);
  if("elo"in b)g.elo=Number(b.elo)||0;if("wins"in b)g.wins=Number(b.wins)||0;if("losses"in b)g.losses=Number(b.losses)||0;
  if("description"in b)g.description=cleanText(b.description,400);if("captain"in b)g.captain=cleanText(b.captain,50);
  atomicWrite(d);broadcast();ok(res,g);
});
app.delete("/api/guilds/:id",requireAdmin,(req,res)=>{
  const d=readData(),gid=Number(req.params.id);
  d.guilds=d.guilds.filter(g=>Number(g.id)!==gid);
  d.players.forEach(p=>{if(Number(p.guildId)===gid)p.guildId=null});
  d.matches.forEach(m=>{if(Number(m.guildAId)===gid)m.guildAId=null;if(Number(m.guildBId)===gid)m.guildBId=null});
  atomicWrite(d);broadcast();ok(res,{ok:true});
});

app.post("/api/players",requireAdmin,(req,res)=>{
  const d=readData(),b=req.body||{},nickname=cleanText(b.nickname,50);
  if(!nickname)return fail(res,"Укажи ник игрока");
  const p={id:nextId(d.players),nickname,gameId:cleanText(b.gameId,40),avatar:cleanImage(b.avatar,"👤"),guildId:b.guildId?Number(b.guildId):null,elo:Number(b.elo)||1200,wins:Number(b.wins)||0,losses:Number(b.losses)||0,kills:Number(b.kills)||0,deaths:Number(b.deaths)||0,role:cleanText(b.role||"Player",30),country:cleanText(b.country||"Кыргызстан",40)};
  d.players.push(p);atomicWrite(d);broadcast();ok(res,p);
});
app.patch("/api/players/:id",requireAdmin,(req,res)=>{
  const d=readData(),p=d.players.find(x=>Number(x.id)===Number(req.params.id));
  if(!p)return fail(res,"Игрок не найден",404);
  const b=req.body||{};
  if("nickname"in b)p.nickname=cleanText(b.nickname,50);if("gameId"in b)p.gameId=cleanText(b.gameId,40);if("avatar"in b)p.avatar=cleanImage(b.avatar,p.avatar||"👤");
  if("guildId"in b)p.guildId=b.guildId?Number(b.guildId):null;
  for(const k of ["elo","wins","losses","kills","deaths"])if(k in b)p[k]=Number(b[k])||0;
  if("role"in b)p.role=cleanText(b.role,30);if("country"in b)p.country=cleanText(b.country,40);
  atomicWrite(d);broadcast();ok(res,p);
});
app.delete("/api/players/:id",requireAdmin,(req,res)=>{
  const d=readData(),pid=Number(req.params.id);d.players=d.players.filter(p=>Number(p.id)!==pid);
  d.matches.forEach(m=>{if(Number(m.playerAId)===pid)m.playerAId=null;if(Number(m.playerBId)===pid)m.playerBId=null});
  atomicWrite(d);broadcast();ok(res,{ok:true});
});

app.post("/api/matches",requireAdmin,(req,res)=>{
  const d=readData(),b=req.body||{};
  const m={id:nextId(d.matches),tournament:cleanText(b.tournament||"LAVENDER CUP",80),title:cleanText(b.title||"LIVE MATCH",80),subtitle:cleanText(b.subtitle||"FREE FIRE",80),guildAId:b.guildAId?Number(b.guildAId):null,guildBId:b.guildBId?Number(b.guildBId):null,scoreA:Number(b.scoreA)||0,scoreB:Number(b.scoreB)||0,roundText:cleanText(b.roundText||"ROUND 1",30),status:cleanText(b.status||"SCHEDULED",20).toUpperCase(),format:cleanText(b.format||"BO7",20),playerAId:b.playerAId?Number(b.playerAId):null,playerBId:b.playerBId?Number(b.playerBId):null};
  d.matches.push(m);d.overlay.activeMatchId=m.id;atomicWrite(d);broadcast();ok(res,m);
});
app.patch("/api/matches/:id",requireAdmin,(req,res)=>{
  const d=readData(),m=d.matches.find(x=>Number(x.id)===Number(req.params.id));if(!m)return fail(res,"Матч не найден",404);
  const b=req.body||{};
  for(const k of ["tournament","title","subtitle","roundText","status","format"])if(k in b)m[k]=cleanText(b[k],80);
  for(const k of ["guildAId","guildBId","playerAId","playerBId"])if(k in b)m[k]=b[k]?Number(b[k]):null;
  if("scoreA"in b)m.scoreA=Math.max(0,Number(b.scoreA)||0);if("scoreB"in b)m.scoreB=Math.max(0,Number(b.scoreB)||0);
  m.status=String(m.status||"LIVE").toUpperCase();
  atomicWrite(d);broadcast();ok(res,m);
});
app.delete("/api/matches/:id",requireAdmin,(req,res)=>{
  const d=readData(),mid=Number(req.params.id);d.matches=d.matches.filter(m=>Number(m.id)!==mid);
  if(Number(d.overlay.activeMatchId)===mid)d.overlay.activeMatchId=d.matches[0]?.id||null;
  atomicWrite(d);broadcast();ok(res,{ok:true});
});

app.post("/api/tournaments",requireAdmin,(req,res)=>{
  const d=readData(),b=req.body||{},name=cleanText(b.name,80);if(!name)return fail(res,"Укажи название турнира");
  const t={id:nextId(d.tournaments),name,status:cleanText(b.status||"UPCOMING",20).toUpperCase(),date:cleanText(b.date,30),format:cleanText(b.format||"BO7",20),prize:cleanText(b.prize,80),description:cleanText(b.description,500),guildIds:Array.isArray(b.guildIds)?b.guildIds.map(Number):[]};
  d.tournaments.push(t);atomicWrite(d);broadcast();ok(res,t);
});
app.patch("/api/tournaments/:id",requireAdmin,(req,res)=>{
  const d=readData(),t=d.tournaments.find(x=>Number(x.id)===Number(req.params.id));if(!t)return fail(res,"Турнир не найден",404);
  Object.assign(t,req.body||{});t.guildIds=Array.isArray(t.guildIds)?t.guildIds.map(Number):[];t.status=String(t.status||"UPCOMING").toUpperCase();
  atomicWrite(d);broadcast();ok(res,t);
});
app.delete("/api/tournaments/:id",requireAdmin,(req,res)=>{const d=readData();d.tournaments=d.tournaments.filter(t=>Number(t.id)!==Number(req.params.id));atomicWrite(d);broadcast();ok(res,{ok:true})});

app.post("/api/news",requireAdmin,(req,res)=>{
  const d=readData(),b=req.body||{},title=cleanText(b.title,120);if(!title)return fail(res,"Укажи заголовок");
  const n={id:nextId(d.news),title,body:cleanText(b.body,1500),pinned:!!b.pinned,createdAt:new Date().toISOString()};d.news.unshift(n);atomicWrite(d);broadcast();ok(res,n);
});
app.patch("/api/news/:id",requireAdmin,(req,res)=>{const d=readData(),n=d.news.find(x=>Number(x.id)===Number(req.params.id));if(!n)return fail(res,"Новость не найдена",404);if("title"in(req.body||{}))n.title=cleanText(req.body.title,120);if("body"in(req.body||{}))n.body=cleanText(req.body.body,1500);if("pinned"in(req.body||{}))n.pinned=!!req.body.pinned;atomicWrite(d);broadcast();ok(res,n)});
app.delete("/api/news/:id",requireAdmin,(req,res)=>{const d=readData();d.news=d.news.filter(n=>Number(n.id)!==Number(req.params.id));atomicWrite(d);broadcast();ok(res,{ok:true})});

app.patch("/api/overlay",requireAdmin,(req,res)=>{
  const d=readData(),b=req.body||{};
  if("activeMatchId"in b)d.overlay.activeMatchId=b.activeMatchId?Number(b.activeMatchId):null;
  if("visible"in b)d.overlay.visible=!!b.visible;if("accent"in b&&/^#[0-9a-f]{6}$/i.test(b.accent))d.overlay.accent=b.accent;
  if("position"in b)d.overlay.position=b.position==="top"?"top":"bottom";if("showPlayers"in b)d.overlay.showPlayers=!!b.showPlayers;if("showStats"in b)d.overlay.showStats=!!b.showStats;
  if("customText"in b)d.overlay.customText=cleanText(b.customText,80);
  atomicWrite(d);broadcast();ok(res,overlayState());
});

app.get("/api/admin/export",requireAdmin,(req,res)=>{
  res.setHeader("Content-Disposition",'attachment; filename="lavender-backup.json"');
  res.type("application/json").send(JSON.stringify(readData(),null,2));
});
app.post("/api/admin/import",requireAdmin,(req,res)=>{
  const d=normalize(req.body);atomicWrite(d);broadcast();ok(res,{ok:true});
});

io.on("connection",socket=>socket.emit("overlay:update",overlayState()));

app.get("/",(req,res)=>res.sendFile(path.join(ROOT,"index.html")));
app.get("/overlay.html",(req,res)=>res.sendFile(path.join(ROOT,"overlay.html")));
app.get("*",(req,res)=>res.sendFile(path.join(ROOT,"index.html")));

setInterval(()=>{const n=Date.now();for(const [k,v] of sessions)if(n-v.createdAt>SESSION_TTL)sessions.delete(k)},60*60*1000).unref();

httpServer.listen(PORT,"0.0.0.0",()=>console.log(`LAVENDER PRO ${APP_VERSION} listening on ${PORT}`));
