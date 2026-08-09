const express=require("express");
const http=require("http");
const path=require("path");
const fs=require("fs");
const crypto=require("crypto");
const {Server}=require("socket.io");

const app=express();
const server=http.createServer(app);
const io=new Server(server);
const PORT=Number(process.env.PORT||3000);
const DATA=path.join(__dirname,"data.json");
const ADMIN_USER=process.env.LAVENDER_ADMIN_USER||"admin";
const ADMIN_PASSWORD=process.env.LAVENDER_ADMIN_PASSWORD||"lavender123";
const sessions=new Map();

app.use(express.json({limit:"10mb"}));
app.use(express.static(__dirname));

function read(){return JSON.parse(fs.readFileSync(DATA,"utf8"))}
function write(d){fs.writeFileSync(DATA,JSON.stringify(d,null,2),"utf8")}
function id(a){return a.length?Math.max(...a.map(x=>+x.id||0))+1:1}
function rank(e){e=+e||0;return e>=2000?"S":e>=1800?"A":e>=1600?"B":e>=1400?"C":e>=1200?"D":e>=1000?"E":"F"}
function img(v,f=""){v=String(v||"").trim();if(!v)return f;if(v.startsWith("data:image/"))return v.length<8*1024*1024?v:f;return v.slice(0,128)}
function cookies(req){const o={};String(req.headers.cookie||"").split(";").forEach(p=>{const i=p.indexOf("=");if(i>0)o[p.slice(0,i).trim()]=decodeURIComponent(p.slice(i+1).trim())});return o}
function isAdmin(req){const t=cookies(req).lavender_session;return !!t&&sessions.has(t)}
function admin(req,res,next){if(!isAdmin(req))return res.status(401).json({error:"Нужен вход администратора"});next()}
function enrich(d){
 const gm=Object.fromEntries(d.guilds.map(g=>[g.id,g]));
 const players=d.players.map(p=>({...p,guild:gm[p.guildId]||null,rank:rank(p.elo),kd:p.deaths?+(p.kills/p.deaths).toFixed(2):+p.kills||0,winrate:(+p.wins+ +p.losses)?Math.round(+p.wins/(+p.wins+ +p.losses)*100):0})).sort((a,b)=>b.elo-a.elo);
 const pm=Object.fromEntries(players.map(p=>[p.id,p]));
 const guilds=d.guilds.map(g=>{const roster=players.filter(p=>p.guildId===g.id);return {...g,rank:rank(g.elo),memberCount:roster.length,roster,winrate:(+g.wins+ +g.losses)?Math.round(+g.wins/(+g.wins+ +g.losses)*100):0}}).sort((a,b)=>b.elo-a.elo);
 const matches=d.matches.map(m=>({...m,guildA:gm[m.guildAId]||null,guildB:gm[m.guildBId]||null,playerA:pm[m.playerAId]||null,playerB:pm[m.playerBId]||null})).sort((a,b)=>b.id-a.id);
 const tournaments=d.tournaments.map(t=>({...t,participants:(t.guildIds||[]).map(x=>gm[x]).filter(Boolean)})).sort((a,b)=>b.id-a.id);
 return {...d,players,guilds,matches,tournaments}
}
function overlayState(){const d=enrich(read());return {overlay:d.overlay,match:d.matches.find(m=>m.id===d.overlay.activeMatchId)||d.matches[0]||null,settings:d.settings}}
function emit(){io.emit("overlay:update",overlayState());io.emit("site:update")}

app.get("/api/all",(q,r)=>r.json(enrich(read())));
app.get("/api/overlay",(q,r)=>r.json(overlayState()));
app.get("/api/auth/status",(q,r)=>r.json({authenticated:isAdmin(q),user:isAdmin(q)?ADMIN_USER:null}));
app.post("/api/auth/login",(q,r)=>{const {username,password}=q.body||{};if(String(username)!==ADMIN_USER||String(password)!==ADMIN_PASSWORD)return r.status(401).json({error:"Неверный логин или пароль"});const t=crypto.randomBytes(32).toString("hex");sessions.set(t,Date.now());r.setHeader("Set-Cookie",`lavender_session=${t}; HttpOnly; SameSite=Strict; Path=/; Max-Age=43200`);r.json({ok:true})});
app.post("/api/auth/logout",(q,r)=>{const t=cookies(q).lavender_session;if(t)sessions.delete(t);r.setHeader("Set-Cookie","lavender_session=; HttpOnly; SameSite=Strict; Path=/; Max-Age=0");r.json({ok:true})});

app.patch("/api/settings",admin,(q,r)=>{const d=read();Object.assign(d.settings,q.body||{});write(d);emit();r.json(d.settings)});

app.post("/api/guilds",admin,(q,r)=>{const d=read(),b=q.body||{};if(!String(b.name||"").trim()||!String(b.tag||"").trim())return r.status(400).json({error:"Название и тег обязательны"});const g={id:id(d.guilds),name:String(b.name).trim(),tag:String(b.tag).trim().toUpperCase(),logo:img(b.logo,"🪻"),region:String(b.region||"Кыргызстан"),elo:+b.elo||1200,wins:+b.wins||0,losses:+b.losses||0,description:String(b.description||""),captain:String(b.captain||""),founded:String(b.founded||""),social:String(b.social||"")};d.guilds.push(g);write(d);emit();r.json(g)});
app.patch("/api/guilds/:id",admin,(q,r)=>{const d=read(),g=d.guilds.find(x=>x.id===+q.params.id);if(!g)return r.status(404).json({error:"Не найдено"});Object.assign(g,q.body||{});g.logo=img(g.logo,"🪻");g.elo=+g.elo||0;g.wins=+g.wins||0;g.losses=+g.losses||0;write(d);emit();r.json(g)});
app.delete("/api/guilds/:id",admin,(q,r)=>{const d=read(),x=+q.params.id;d.players.forEach(p=>{if(p.guildId===x)p.guildId=null});d.guilds=d.guilds.filter(g=>g.id!==x);write(d);emit();r.json({ok:true})});

app.post("/api/players",admin,(q,r)=>{const d=read(),b=q.body||{};if(!String(b.nickname||"").trim())return r.status(400).json({error:"Укажи ник"});const p={id:id(d.players),nickname:String(b.nickname).trim(),gameId:String(b.gameId||""),avatar:img(b.avatar,"👤"),guildId:b.guildId?+b.guildId:null,elo:+b.elo||1200,wins:+b.wins||0,losses:+b.losses||0,kills:+b.kills||0,deaths:+b.deaths||0,role:String(b.role||"Player"),country:String(b.country||"Кыргызстан"),bio:String(b.bio||"")};d.players.push(p);write(d);emit();r.json(p)});
app.patch("/api/players/:id",admin,(q,r)=>{const d=read(),p=d.players.find(x=>x.id===+q.params.id);if(!p)return r.status(404).json({error:"Не найдено"});Object.assign(p,q.body||{});p.avatar=img(p.avatar,"👤");p.guildId=p.guildId?+p.guildId:null;["elo","wins","losses","kills","deaths"].forEach(k=>p[k]=+p[k]||0);write(d);emit();r.json(p)});
app.delete("/api/players/:id",admin,(q,r)=>{const d=read(),x=+q.params.id;d.players=d.players.filter(p=>p.id!==x);write(d);emit();r.json({ok:true})});

app.post("/api/matches",admin,(q,r)=>{const d=read(),b=q.body||{};const m={id:id(d.matches),tournament:String(b.tournament||"LAVENDER CUP"),title:String(b.title||"LIVE MATCH"),subtitle:String(b.subtitle||"FREE FIRE"),guildAId:b.guildAId?+b.guildAId:null,guildBId:b.guildBId?+b.guildBId:null,scoreA:+b.scoreA||0,scoreB:+b.scoreB||0,roundText:String(b.roundText||"ROUND 1"),status:String(b.status||"SCHEDULED").toUpperCase(),format:String(b.format||"BO7"),playerAId:b.playerAId?+b.playerAId:null,playerBId:b.playerBId?+b.playerBId:null,scheduledAt:String(b.scheduledAt||"")};d.matches.push(m);d.overlay.activeMatchId=m.id;write(d);emit();r.json(m)});
app.patch("/api/matches/:id",admin,(q,r)=>{const d=read(),m=d.matches.find(x=>x.id===+q.params.id);if(!m)return r.status(404).json({error:"Не найдено"});Object.assign(m,q.body||{});["guildAId","guildBId","playerAId","playerBId"].forEach(k=>m[k]=m[k]?+m[k]:null);m.scoreA=+m.scoreA||0;m.scoreB=+m.scoreB||0;m.status=String(m.status||"LIVE").toUpperCase();write(d);emit();r.json(m)});
app.delete("/api/matches/:id",admin,(q,r)=>{const d=read(),x=+q.params.id;d.matches=d.matches.filter(m=>m.id!==x);if(d.overlay.activeMatchId===x)d.overlay.activeMatchId=d.matches[0]?.id||null;write(d);emit();r.json({ok:true})});

app.post("/api/news",admin,(q,r)=>{const d=read(),b=q.body||{};const n={id:id(d.news),title:String(b.title||"").trim(),body:String(b.body||""),type:String(b.type||"news"),pinned:!!b.pinned,createdAt:new Date().toISOString()};if(!n.title)return r.status(400).json({error:"Укажи заголовок"});d.news.unshift(n);write(d);emit();r.json(n)});
app.patch("/api/news/:id",admin,(q,r)=>{const d=read(),n=d.news.find(x=>x.id===+q.params.id);if(!n)return r.status(404).json({error:"Не найдено"});Object.assign(n,q.body||{});write(d);emit();r.json(n)});
app.delete("/api/news/:id",admin,(q,r)=>{const d=read();d.news=d.news.filter(n=>n.id!==+q.params.id);write(d);emit();r.json({ok:true})});

app.post("/api/tournaments",admin,(q,r)=>{const d=read(),b=q.body||{};const t={id:id(d.tournaments),name:String(b.name||"").trim(),status:String(b.status||"UPCOMING").toUpperCase(),startDate:String(b.startDate||""),prize:String(b.prize||""),format:String(b.format||"BO7"),guildIds:(b.guildIds||[]).map(Number),description:String(b.description||"")};if(!t.name)return r.status(400).json({error:"Укажи название"});d.tournaments.push(t);write(d);emit();r.json(t)});
app.patch("/api/tournaments/:id",admin,(q,r)=>{const d=read(),t=d.tournaments.find(x=>x.id===+q.params.id);if(!t)return r.status(404).json({error:"Не найдено"});Object.assign(t,q.body||{});t.guildIds=(t.guildIds||[]).map(Number);write(d);emit();r.json(t)});
app.delete("/api/tournaments/:id",admin,(q,r)=>{const d=read();d.tournaments=d.tournaments.filter(t=>t.id!==+q.params.id);write(d);emit();r.json({ok:true})});

app.patch("/api/overlay",admin,(q,r)=>{const d=read();Object.assign(d.overlay,q.body||{});d.overlay.activeMatchId=d.overlay.activeMatchId?+d.overlay.activeMatchId:null;["visible","showPlayers","showElo","showRank"].forEach(k=>d.overlay[k]=!!d.overlay[k]);write(d);emit();r.json(overlayState())});

io.on("connection",s=>s.emit("overlay:update",overlayState()));
app.get("/",(q,r)=>r.sendFile(path.join(__dirname,"index.html")));
app.get("/overlay.html",(q,r)=>r.sendFile(path.join(__dirname,"overlay.html")));
app.get("*",(q,r)=>r.sendFile(path.join(__dirname,"index.html")));
server.listen(PORT,"0.0.0.0",()=>console.log(`LAVENDER PRO 5.0 live on ${PORT}`));
