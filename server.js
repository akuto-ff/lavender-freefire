const express = require("express");
const http = require("http");
const path = require("path");
const fs = require("fs");
const crypto = require("crypto");
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);
const io = new Server(server);
const PORT = Number(process.env.PORT || 3000);
const DATA_FILE = path.join(__dirname, "data.json");

const ADMIN_USER = process.env.LAVENDER_ADMIN_USER || "admin";
const ADMIN_PASSWORD = process.env.LAVENDER_ADMIN_PASSWORD || "lavender123";
const sessions = new Map();

app.use(express.json({limit:"10mb"}));
app.use(express.static(__dirname));

function parseCookies(req){
  const out={};
  String(req.headers.cookie||"").split(";").forEach(part=>{
    const i=part.indexOf("=");
    if(i>0) out[part.slice(0,i).trim()]=decodeURIComponent(part.slice(i+1).trim());
  });
  return out;
}
function isAdmin(req){
  const token=parseCookies(req).lavender_session;
  return !!token && sessions.has(token);
}
function requireAdmin(req,res,next){
  if(!isAdmin(req)) return res.status(401).json({error:"Нужен вход администратора"});
  next();
}
function nextId(items){return items.length?Math.max(...items.map(x=>Number(x.id)||0))+1:1}
function rankForElo(elo){
  elo=Number(elo)||0;
  if(elo>=2000)return "S";
  if(elo>=1800)return "A";
  if(elo>=1600)return "B";
  if(elo>=1400)return "C";
  if(elo>=1200)return "D";
  if(elo>=1000)return "E";
  return "F";
}
function cleanImage(value,fallback=""){
  value=String(value||"").trim();
  if(!value)return fallback;
  if(value.startsWith("data:image/")){
    if(value.length>8*1024*1024)return fallback;
    return value;
  }
  return value.slice(0,128);
}
function now(){return new Date().toISOString()}
function readData(){
  const d=JSON.parse(fs.readFileSync(DATA_FILE,"utf8"));
  d.settings ||= {siteName:"LAVENDER",accent:"#b46cff",tagline:"FREE FIRE COMMUNITY"};
  d.guilds ||= [];
  d.players ||= [];
  d.matches ||= [];
  d.overlay ||= {activeMatchId:null,visible:true,accent:"#b46cff",position:"bottom",layout:"full",showPlayers:true,showElo:true,showRank:true,customText:"LAVENDER • LIVE"};
  d.news ||= [];
  d.tournaments ||= [];
  d.activity ||= [];
  d.settings.tagline ||= "FREE FIRE COMMUNITY";
  d.settings.discord ||= "";
  d.settings.telegram ||= "";
  d.settings.youtube ||= "";
  return d;
}
function writeData(data){
  fs.writeFileSync(DATA_FILE,JSON.stringify(data,null,2),"utf8");
}
function activity(data,type,text,meta={}){
  data.activity.unshift({id:nextId(data.activity),type,text,meta,createdAt:now()});
  data.activity=data.activity.slice(0,100);
}
function enriched(data){
  const guildMap=Object.fromEntries(data.guilds.map(g=>[g.id,g]));
  const players=data.players.map(p=>({
    ...p,
    guild:guildMap[p.guildId]||null,
    rank:rankForElo(p.elo),
    kd:p.deaths?Number((Number(p.kills||0)/Number(p.deaths||1)).toFixed(2)):Number(p.kills||0),
    winrate:(Number(p.wins||0)+Number(p.losses||0))?Math.round(Number(p.wins||0)/(Number(p.wins||0)+Number(p.losses||0))*100):0
  })).sort((a,b)=>b.elo-a.elo);
  const playerMap=Object.fromEntries(players.map(p=>[p.id,p]));
  const guilds=data.guilds.map(g=>{
    const roster=players.filter(p=>p.guildId===g.id);
    return {
      ...g,
      rank:rankForElo(g.elo),
      memberCount:roster.length,
      roster,
      winrate:(Number(g.wins||0)+Number(g.losses||0))?Math.round(Number(g.wins||0)/(Number(g.wins||0)+Number(g.losses||0))*100):0
    };
  }).sort((a,b)=>b.elo-a.elo);
  const matches=data.matches.map(m=>({
    ...m,
    guildA:guildMap[m.guildAId]||null,
    guildB:guildMap[m.guildBId]||null,
    playerA:playerMap[m.playerAId]||null,
    playerB:playerMap[m.playerBId]||null
  })).sort((a,b)=>b.id-a.id);
  const tournaments=data.tournaments.map(t=>({
    ...t,
    participants:(t.guildIds||[]).map(id=>guildMap[id]).filter(Boolean)
  })).sort((a,b)=>b.id-a.id);
  return {...data,players,guilds,matches,tournaments};
}
function overlayState(data=readData()){
  const full=enriched(data);
  const match=full.matches.find(x=>x.id===full.overlay.activeMatchId)||full.matches[0]||null;
  return {overlay:full.overlay,match,settings:full.settings};
}
function broadcast(){
  io.emit("overlay:update",overlayState());
  io.emit("site:update");
}

app.get("/api/all",(req,res)=>res.json(enriched(readData())));
app.get("/api/overlay",(req,res)=>res.json(overlayState()));
app.get("/api/auth/status",(req,res)=>res.json({authenticated:isAdmin(req),user:isAdmin(req)?ADMIN_USER:null}));

app.post("/api/auth/login",(req,res)=>{
  const {username,password}=req.body||{};
  if(String(username)!==ADMIN_USER||String(password)!==ADMIN_PASSWORD)
    return res.status(401).json({error:"Неверный логин или пароль"});
  const token=crypto.randomBytes(32).toString("hex");
  sessions.set(token,{createdAt:Date.now()});
  res.setHeader("Set-Cookie",`lavender_session=${token}; HttpOnly; SameSite=Strict; Path=/; Max-Age=43200`);
  res.json({ok:true,user:ADMIN_USER});
});
app.post("/api/auth/logout",(req,res)=>{
  const token=parseCookies(req).lavender_session;
  if(token)sessions.delete(token);
  res.setHeader("Set-Cookie","lavender_session=; HttpOnly; SameSite=Strict; Path=/; Max-Age=0");
  res.json({ok:true});
});

app.patch("/api/settings",requireAdmin,(req,res)=>{
  const data=readData();
  Object.assign(data.settings,req.body||{});
  data.settings.siteName=String(data.settings.siteName||"LAVENDER").slice(0,40);
  data.settings.tagline=String(data.settings.tagline||"").slice(0,100);
  activity(data,"settings","Обновлены настройки сайта");
  writeData(data);broadcast();res.json(data.settings);
});

app.post("/api/guilds",requireAdmin,(req,res)=>{
  const data=readData(),b=req.body||{};
  const name=String(b.name||"").trim(),tag=String(b.tag||"").trim().toUpperCase();
  if(!name||!tag)return res.status(400).json({error:"Название и тег обязательны"});
  if(data.guilds.some(g=>String(g.tag).toUpperCase()===tag))return res.status(400).json({error:"Такой тег уже существует"});
  const g={id:nextId(data.guilds),name,tag,logo:cleanImage(b.logo,"🪻"),region:String(b.region||"Кыргызстан"),elo:Number(b.elo)||1200,wins:Number(b.wins)||0,losses:Number(b.losses)||0,description:String(b.description||""),captain:String(b.captain||""),founded:String(b.founded||""),social:String(b.social||"")};
  data.guilds.push(g);activity(data,"guild",`Добавлена гильдия ${g.name}`,{id:g.id});writeData(data);broadcast();res.json(g);
});
app.patch("/api/guilds/:id",requireAdmin,(req,res)=>{
  const data=readData(),g=data.guilds.find(x=>x.id===Number(req.params.id));
  if(!g)return res.status(404).json({error:"Гильдия не найдена"});
  Object.assign(g,req.body||{});
  if("logo" in (req.body||{}))g.logo=cleanImage(req.body.logo,g.logo||"🪻");
  g.elo=Number(g.elo)||0;g.wins=Number(g.wins)||0;g.losses=Number(g.losses)||0;g.tag=String(g.tag||"").toUpperCase();
  activity(data,"guild",`Обновлена гильдия ${g.name}`,{id:g.id});writeData(data);broadcast();res.json(g);
});
app.delete("/api/guilds/:id",requireAdmin,(req,res)=>{
  const data=readData(),id=Number(req.params.id),g=data.guilds.find(x=>x.id===id);
  if(!g)return res.status(404).json({error:"Гильдия не найдена"});
  data.players.forEach(p=>{if(p.guildId===id)p.guildId=null});
  data.matches.forEach(m=>{if(m.guildAId===id)m.guildAId=null;if(m.guildBId===id)m.guildBId=null});
  data.tournaments.forEach(t=>t.guildIds=(t.guildIds||[]).filter(x=>x!==id));
  data.guilds=data.guilds.filter(x=>x.id!==id);
  activity(data,"guild",`Удалена гильдия ${g.name}`);writeData(data);broadcast();res.json({ok:true});
});

app.post("/api/players",requireAdmin,(req,res)=>{
  const data=readData(),b=req.body||{};
  const nickname=String(b.nickname||"").trim();
  if(!nickname)return res.status(400).json({error:"Укажи ник игрока"});
  const p={id:nextId(data.players),nickname,gameId:String(b.gameId||""),avatar:cleanImage(b.avatar,"👤"),guildId:b.guildId?Number(b.guildId):null,elo:Number(b.elo)||1200,wins:Number(b.wins)||0,losses:Number(b.losses)||0,kills:Number(b.kills)||0,deaths:Number(b.deaths)||0,role:String(b.role||"Player"),country:String(b.country||"Кыргызстан"),bio:String(b.bio||"")};
  data.players.push(p);activity(data,"player",`Добавлен игрок ${p.nickname}`,{id:p.id});writeData(data);broadcast();res.json(p);
});
app.patch("/api/players/:id",requireAdmin,(req,res)=>{
  const data=readData(),p=data.players.find(x=>x.id===Number(req.params.id));
  if(!p)return res.status(404).json({error:"Игрок не найден"});
  Object.assign(p,req.body||{});
  if("avatar" in (req.body||{}))p.avatar=cleanImage(req.body.avatar,p.avatar||"👤");
  p.guildId=p.guildId?Number(p.guildId):null;
  ["elo","wins","losses","kills","deaths"].forEach(k=>p[k]=Number(p[k])||0);
  activity(data,"player",`Обновлён игрок ${p.nickname}`,{id:p.id});writeData(data);broadcast();res.json(p);
});
app.delete("/api/players/:id",requireAdmin,(req,res)=>{
  const data=readData(),id=Number(req.params.id),p=data.players.find(x=>x.id===id);
  if(!p)return res.status(404).json({error:"Игрок не найден"});
  data.matches.forEach(m=>{if(m.playerAId===id)m.playerAId=null;if(m.playerBId===id)m.playerBId=null});
  data.players=data.players.filter(x=>x.id!==id);
  activity(data,"player",`Удалён игрок ${p.nickname}`);writeData(data);broadcast();res.json({ok:true});
});

app.post("/api/matches",requireAdmin,(req,res)=>{
  const data=readData(),b=req.body||{};
  const m={id:nextId(data.matches),tournament:String(b.tournament||"LAVENDER CUP"),title:String(b.title||"LIVE MATCH"),subtitle:String(b.subtitle||"FREE FIRE"),guildAId:b.guildAId?Number(b.guildAId):null,guildBId:b.guildBId?Number(b.guildBId):null,scoreA:Number(b.scoreA)||0,scoreB:Number(b.scoreB)||0,roundText:String(b.roundText||"ROUND 1"),status:String(b.status||"SCHEDULED").toUpperCase(),format:String(b.format||"BO7"),playerAId:b.playerAId?Number(b.playerAId):null,playerBId:b.playerBId?Number(b.playerBId):null,scheduledAt:String(b.scheduledAt||"")};
  data.matches.push(m);data.overlay.activeMatchId=m.id;activity(data,"match",`Создан матч ${m.title}`,{id:m.id});writeData(data);broadcast();res.json(m);
});
app.patch("/api/matches/:id",requireAdmin,(req,res)=>{
  const data=readData(),m=data.matches.find(x=>x.id===Number(req.params.id));
  if(!m)return res.status(404).json({error:"Матч не найден"});
  Object.assign(m,req.body||{});
  ["guildAId","guildBId","playerAId","playerBId"].forEach(k=>m[k]=m[k]?Number(m[k]):null);
  m.scoreA=Number(m.scoreA)||0;m.scoreB=Number(m.scoreB)||0;m.status=String(m.status||"LIVE").toUpperCase();
  activity(data,"match",`Обновлён матч ${m.title}`,{id:m.id});writeData(data);broadcast();res.json(m);
});
app.delete("/api/matches/:id",requireAdmin,(req,res)=>{
  const data=readData(),id=Number(req.params.id),m=data.matches.find(x=>x.id===id);
  if(!m)return res.status(404).json({error:"Матч не найден"});
  data.matches=data.matches.filter(x=>x.id!==id);
  if(data.overlay.activeMatchId===id)data.overlay.activeMatchId=data.matches[0]?.id||null;
  activity(data,"match",`Удалён матч ${m.title}`);writeData(data);broadcast();res.json({ok:true});
});

app.post("/api/news",requireAdmin,(req,res)=>{
  const data=readData(),b=req.body||{};
  const title=String(b.title||"").trim();if(!title)return res.status(400).json({error:"Укажи заголовок"});
  const n={id:nextId(data.news),title,body:String(b.body||""),type:String(b.type||"news"),pinned:!!b.pinned,createdAt:now()};
  data.news.unshift(n);activity(data,"news",`Опубликована новость ${n.title}`);writeData(data);broadcast();res.json(n);
});
app.patch("/api/news/:id",requireAdmin,(req,res)=>{
  const data=readData(),n=data.news.find(x=>x.id===Number(req.params.id));if(!n)return res.status(404).json({error:"Новость не найдена"});
  Object.assign(n,req.body||{});activity(data,"news",`Обновлена новость ${n.title}`);writeData(data);broadcast();res.json(n);
});
app.delete("/api/news/:id",requireAdmin,(req,res)=>{
  const data=readData(),id=Number(req.params.id);data.news=data.news.filter(x=>x.id!==id);writeData(data);broadcast();res.json({ok:true});
});

app.post("/api/tournaments",requireAdmin,(req,res)=>{
  const data=readData(),b=req.body||{};
  const name=String(b.name||"").trim();if(!name)return res.status(400).json({error:"Укажи название турнира"});
  const t={id:nextId(data.tournaments),name,status:String(b.status||"UPCOMING").toUpperCase(),startDate:String(b.startDate||""),prize:String(b.prize||""),format:String(b.format||"BO7"),guildIds:(b.guildIds||[]).map(Number),description:String(b.description||"")};
  data.tournaments.push(t);activity(data,"tournament",`Создан турнир ${t.name}`);writeData(data);broadcast();res.json(t);
});
app.patch("/api/tournaments/:id",requireAdmin,(req,res)=>{
  const data=readData(),t=data.tournaments.find(x=>x.id===Number(req.params.id));if(!t)return res.status(404).json({error:"Турнир не найден"});
  Object.assign(t,req.body||{});t.guildIds=(t.guildIds||[]).map(Number);activity(data,"tournament",`Обновлён турнир ${t.name}`);writeData(data);broadcast();res.json(t);
});
app.delete("/api/tournaments/:id",requireAdmin,(req,res)=>{
  const data=readData(),id=Number(req.params.id);data.tournaments=data.tournaments.filter(x=>x.id!==id);writeData(data);broadcast();res.json({ok:true});
});

app.patch("/api/overlay",requireAdmin,(req,res)=>{
  const data=readData();Object.assign(data.overlay,req.body||{});
  if(data.overlay.activeMatchId)data.overlay.activeMatchId=Number(data.overlay.activeMatchId);
  ["visible","showPlayers","showElo","showRank"].forEach(k=>data.overlay[k]=!!data.overlay[k]);
  writeData(data);broadcast();res.json(overlayState(data));
});

app.get("/api/admin/export",requireAdmin,(req,res)=>{
  res.setHeader("Content-Disposition",'attachment; filename="lavender-backup.json"');
  res.setHeader("Content-Type","application/json; charset=utf-8");
  res.send(JSON.stringify(readData(),null,2));
});
app.post("/api/admin/import",requireAdmin,(req,res)=>{
  const incoming=req.body;
  if(!incoming||!Array.isArray(incoming.players)||!Array.isArray(incoming.guilds))return res.status(400).json({error:"Некорректный backup"});
  incoming.settings ||= {siteName:"LAVENDER",accent:"#b46cff"};
  incoming.matches ||= [];incoming.overlay ||= {};incoming.news ||= [];incoming.tournaments ||= [];incoming.activity ||= [];
  activity(incoming,"backup","Импортирован backup");
  writeData(incoming);broadcast();res.json({ok:true});
});

io.on("connection",socket=>socket.emit("overlay:update",overlayState()));
app.get("*",(req,res)=>res.sendFile(path.join(__dirname,"public","index.html")));

server.listen(PORT,"0.0.0.0",()=>{
  console.log("");
  console.log("======================================");
  console.log(" LAVENDER PRO 4.0");
  console.log(` Site: http://localhost:${PORT}`);
  console.log(` OBS : http://localhost:${PORT}/overlay.html`);
  console.log(" Admin: admin / lavender123");
  console.log("======================================");
  console.log("");
});
