const http = require("http");
const WebSocket = require("ws");

const PORT = process.env.PORT || 3000;
const rooms = new Map();

function generateCode() {
  const chars = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
  let code = "";
  for (let i = 0; i < 4; i++) code += chars[Math.floor(Math.random() * chars.length)];
  return code;
}

const httpServer = http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("repa sync server running");
});

const wss = new WebSocket.Server({ server: httpServer });

wss.on("connection", (ws) => {
  ws.room = null;
  ws.isAlive = true;

  ws.on("pong", () => { ws.isAlive = true; });

  ws.on("message", (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    switch (msg.type) {
      case "create": {
        let code;
        do { code = generateCode(); } while (rooms.has(code));
        ws.displayName = msg.name || "Host";
        rooms.set(code, { clients: [ws] });
        ws.room = code;
        ws.send(JSON.stringify({ type: "created", room: code }));
        break;
      }

      case "join": {
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
        room.clients.push(ws);
        ws.room = code;
        ws.send(JSON.stringify({ type: "joined", room: code, peers: peerList(room, ws) }));
        relay(ws, { type: "peerJoined", peers: peerListAll(room) });
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
    removeFromRoom(ws);
  });
});

function peerList(room, exclude) {
  return room.clients
    .filter((c) => c !== exclude && c.readyState === WebSocket.OPEN)
    .map((c) => c.displayName || "?");
}

function peerListAll(room) {
  return room.clients
    .filter((c) => c.readyState === WebSocket.OPEN)
    .map((c) => c.displayName || "?");
}

function relay(sender, msg) {
  if (!sender.room) return;
  const room = rooms.get(sender.room);
  if (!room) return;
  const data = JSON.stringify(msg);
  for (const c of room.clients) {
    if (c !== sender && c.readyState === WebSocket.OPEN) c.send(data);
  }
}

function removeFromRoom(ws) {
  if (!ws.room) return;
  const room = rooms.get(ws.room);
  if (!room) return;
  room.clients = room.clients.filter((c) => c !== ws);
  if (room.clients.length === 0) {
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
    if (!ws.isAlive) { removeFromRoom(ws); return ws.terminate(); }
    ws.isAlive = false;
    ws.ping();
  });
}, 30000);

httpServer.listen(PORT, () => {
  console.log(`Repa sync server running on port ${PORT}`);
});
