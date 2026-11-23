'use strict';
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = process.env.PORT || 3000;
const MAX_PARTICIPANTS = 9;
const rooms = new Map();
const clients = new WeakMap();
const mime = {'.html':'text/html; charset=utf-8','.css':'text/css; charset=utf-8','.js':'text/javascript; charset=utf-8','.svg':'image/svg+xml'};
const publicDir = path.join(__dirname, 'public');

const server = http.createServer((req, res) => {
  const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  if (url.pathname === '/health') { res.writeHead(200, {'Content-Type':'application/json'}); return res.end(JSON.stringify({ok:true})); }
  const safePath = url.pathname === '/' ? '/index.html' : url.pathname;
  const file = path.resolve(publicDir, '.' + safePath);
  if (!file.startsWith(publicDir + path.sep)) { res.writeHead(403); return res.end('Forbidden'); }
  fs.readFile(file, (err, content) => {
    if (err) { res.writeHead(404); return res.end('Not found'); }
    res.writeHead(200, {'Content-Type':mime[path.extname(file)] || 'application/octet-stream','Cache-Control':'no-cache'});
    res.end(content);
  });
});
const wss = new WebSocketServer({ server, path: '/ws' });
const send = (ws, payload) => { if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(payload)); };
function broadcast(room, payload) { for (const ws of room.connections) send(ws, payload); }
function clean(v, max=32) { return String(v || '').trim().replace(/[<>\x00-\x1f]/g, '').slice(0,max); }
function state(room) {
  return {type:'state', room:room.id, names:room.names, participants:[...room.members.values()].map(x=>({id:x.id,name:x.name})), history:room.history, spinning:room.spin && Date.now() < room.spin.endAt ? room.spin : null};
}
function getRoom(id) {
  if (!rooms.has(id)) rooms.set(id, {id,names:['Person 1','Person 2','Person 3','Person 4','Person 5','Person 6','Person 7','Person 8','Person 9'],members:new Map(),connections:new Set(),history:[],spin:null,lastActive:Date.now()});
  return rooms.get(id);
}
function publish(room) { broadcast(room,state(room)); }
function error(ws, message) { send(ws,{type:'error',message}); }
function finishSpin(room) {
  if (!room.spin || Date.now() < room.spin.endAt) return;
  const s=room.spin;
  if (!room.history.some(h => h.id === s.id)) room.history.unshift({id:s.id,name:s.winner,by:s.by,at:s.endAt});
  room.history=room.history.slice(0,30);
  room.spin=null;
  publish(room);
}
function leave(ws) {
  const info=clients.get(ws); if(!info) return;
  clients.delete(ws);
  const room=rooms.get(info.room);if(!room)return;
  room.connections.delete(ws);
  // remove this participant only if they have no other live sockets
  if (![...room.connections].some(c => clients.get(c)?.id===info.id)) room.members.delete(info.id);
  room.lastActive=Date.now(); publish(room);
}
wss.on('connection', ws => {
  ws.on('message', raw => {
    if (raw.length > 4096) return error(ws,'Message too long');
    let msg;try {msg=JSON.parse(raw.toString());}catch {return error(ws,'Invalid message');}
    if (msg.type==='join') {
      if (clients.has(ws)) return error(ws,'Already joined');
      const roomId=clean(msg.room,10).toUpperCase();
      const name=clean(msg.name,24);
      if (!/^[A-Z0-9]{4,10}$/.test(roomId) || !name) return error(ws,'Enter a room code and your name');
      const room=getRoom(roomId);
      const id=crypto.randomUUID();
      if(room.members.size>=MAX_PARTICIPANTS) return error(ws,'This room already has 9 people.');
      room.members.set(id,{id,name});room.connections.add(ws);room.lastActive=Date.now();
      clients.set(ws,{id,room:roomId});
      send(ws,{type:'joined',id,room:roomId,serverTime:Date.now()});publish(room);
      return;
    }
    const info=clients.get(ws);if(!info)return error(ws,'Join a room first');
    const room=rooms.get(info.room);if(!room)return error(ws,'Room unavailable');
    room.lastActive=Date.now();finishSpin(room);
    if (msg.type==='spin') {
      if(room.spin)return error(ws,'A spin is already underway');
      if(room.names.length<2)return error(ws,'Add at least two wheel entries');
      const index=crypto.randomInt(room.names.length);
      const startAt=Date.now()+650;
      room.spin={id:crypto.randomUUID(),index,winner:room.names[index],by:room.members.get(info.id)?.name||'Guest',startAt,endAt:startAt+6000,entries:[...room.names],turns:6+crypto.randomInt(3)};
      broadcast(room,{type:'spin',spin:room.spin,serverTime:Date.now()});
      setTimeout(()=>finishSpin(room),room.spin.endAt-Date.now()+25);
      return;
    }
    if(msg.type==='setNames') {
      if(room.spin)return error(ws,'Wait until the spin is finished');
      if(!Array.isArray(msg.names))return error(ws,'Invalid entries');
      const names=msg.names.map(v=>clean(v,36)).filter(Boolean).slice(0,60);
      if(names.length<2)return error(ws,'Use at least two names');
      room.names=names;room.history=[];publish(room);return;
    }
    if(msg.type==='clearHistory') {if(room.spin)return error(ws,'Wait for spin to end');room.history=[];publish(room);return;}
    if(msg.type==='ping')send(ws,{type:'pong',serverTime:Date.now(),sentAt:msg.sentAt});
  });
  ws.on('close',()=>leave(ws));ws.on('error',()=>leave(ws));
});
setInterval(() => {
  for(const [id,room] of rooms) {
    finishSpin(room);
    if(room.connections.size===0 && Date.now()-room.lastActive > 6*60*60*1000) rooms.delete(id);
  }
},60000).unref();
server.listen(PORT,()=>console.log(`Live wheel listening on ${PORT}`));
