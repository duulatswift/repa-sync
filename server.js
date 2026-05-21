const http = require("http");
const WebSocket = require("ws");

const PORT = process.env.PORT || 3000;
const rooms = new Map();
const browsers = new Set();

function generateCode() {
  const chars = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
  let code = "";
  for (let i = 0; i < 4; i++) code += chars[Math.floor(Math.random() * chars.length)];
  return code;
}

function generateClientId() {
  return Math.random().toString(36).slice(2) + Date.now().toString(36);
}

const httpServer = http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("repa sync server running");
});

const wss = new WebSocket.Server({ server: httpServer });

wss.on("connection", (ws) => {
  ws.id = generateClientId();
  ws.room = null;
  ws.isAlive = true;
  ws.displayName = "?";
  ws.role = null;

  ws.on("pong", () => { ws.isAlive = true; });

  ws.on("message", (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    switch (msg.type) {
      case "browse": {
        ws.displayName = msg.name || "Guest";
        ws.role = msg.role || null;
        browsers.add(ws);
        sendRoomList(ws);
        break;
      }

      case "create": {
        browsers.delete(ws);

        let code;
        do { code = generateCode(); } while (rooms.has(code));

        ws.displayName = msg.name || "Host";
        ws.role = msg.role || null;

        rooms.set(code, {
          code,
          host: ws,
          hostName: ws.displayName,
          hostRole: ws.role,
          clients: [ws]
        });

        ws.room = code;
        ws.send(JSON.stringify({
          type: "created",
          room: code,
          clientId: ws.id,
          peers: peerListAll(rooms.get(code))
        }));

        broadcastRoomList();
        break;
      }

      case "join": {
        browsers.delete(ws);

        const code = (msg.room || "").toUpperCase();
        const room = rooms.get(code);

        if (!room) {
          ws.send(JSON.stringify({ type: "error", message: "Room not found" }));
          break;
        }

        if (room.clients.length >= 5) {
          ws.send(JSON.stringify({ type: "error", message: "Room full" }));
          break;
        }

        ws.displayName = msg.name || "Guest";
        ws.role = msg.role || null;
        ws.room = code;

        room.clients.push(ws);

        ws.send(JSON.stringify({
          type: "joined",
          room: code,
          clientId: ws.id,
          peers: peerListAll(room)
        }));

        relay(ws, { type: "peerJoined", peers: peerListAll(room) });
        broadcastRoomList();
        break;
      }

      case "ping": {
        const t2 = Date.now();
        ws.send(JSON.stringify({ type: "pong", t1: msg.t1, t2, t3: Date.now() }));
        break;
      }

      default: {
        relay(ws, msg);
      }
    }
  });

  ws.on("close", () => {
    browsers.delete(ws);
    removeFromRoom(ws);
    broadcastRoomList();
  });
});

function sendRoomList(ws) {
  if (ws.readyState !== WebSocket.OPEN) return;

  ws.send(JSON.stringify({
    type: "roomList",
    rooms: Array.from(rooms.values()).map((room) => ({
      code: room.code,
      hostName: room.hostName || "Host",
      hostRole: room.hostRole || null,
      peerCount: room.clients.filter((c) => c.readyState === WebSocket.OPEN).length
    }))
  }));
}

function broadcastRoomList() {
  for (const ws of browsers) {
    sendRoomList(ws);
  }
}

function peerListAll(room) {
  return room.clients
    .filter((c) => c.readyState === WebSocket.OPEN)
    .map((c) => ({
      id: c.id,
      name: c.displayName || "?",
      role: c.role || null
    }));
}

function relay(sender, msg) {
  if (!sender.room) return;
  const room = rooms.get(sender.room);
  if (!room) return;

  const data = JSON.stringify({
    ...msg,
    senderPeerID: sender.id
  });

  for (const c of room.clients) {
    if (c !== sender && c.readyState === WebSocket.OPEN) {
      c.send(data);
    }
  }
}

function removeFromRoom(ws) {
  if (!ws.room) return;

  const room = rooms.get(ws.room);
  if (!room) return;

  room.clients = room.clients.filter((c) => c !== ws);

  if (room.clients.length === 0 || room.host === ws) {
    rooms.delete(ws.room);
  } else {
    const list = peerListAll(room);
    for (const c of room.clients) {
      if (c.readyState === WebSocket.OPEN) {
        c.send(JSON.stringify({ type: "peerLeft", peers: list }));
      }
    }
  }

  ws.room = null;
}

setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) {
      browsers.delete(ws);
      removeFromRoom(ws);
      broadcastRoomList();
      return ws.terminate();
    }

    ws.isAlive = false;
    ws.ping();
  });
}, 30000);

httpServer.listen(PORT, () => {
  console.log(`Repa sync server running on port ${PORT}`);
});
