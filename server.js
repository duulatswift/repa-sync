const http = require("http");
const WebSocket = require("ws");

const PORT = process.env.PORT || 3000;
const SERVER_VERSION = "room-list-v2";

const rooms = new Map();
const browsers = new Set();

console.log(`SERVER VERSION: ${SERVER_VERSION}`);

function generateCode() {
  const chars = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
  let code = "";
  for (let i = 0; i < 4; i++) {
    code += chars[Math.floor(Math.random() * chars.length)];
  }
  return code;
}

function generateClientId() {
  return `${Math.random().toString(36).slice(2)}${Date.now().toString(36)}`;
}

const httpServer = http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end(`repa sync server running ${SERVER_VERSION}`);
});

const wss = new WebSocket.Server({ server: httpServer });

wss.on("connection", (ws) => {
  ws.id = generateClientId();
  ws.room = null;
  ws.isAlive = true;
  ws.displayName = "?";
  ws.role = null;

  ws.on("pong", () => {
    ws.isAlive = true;
  });

  ws.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw);
    } catch {
      return;
    }

    switch (msg.type) {
      case "browse": {
        ws.displayName = msg.name || "Guest";
        ws.role = msg.role || null;
        ws.room = null;
        browsers.add(ws);

        console.log("browse received", ws.displayName, "rooms", rooms.size);
        sendRoomList(ws);
        break;
      }

      case "create": {
        browsers.delete(ws);

        let code;
        do {
          code = generateCode();
        } while (rooms.has(code));

        ws.displayName = msg.name || "Host";
        ws.role = msg.role || null;
        ws.room = code;

        const room = {
          code,
          host: ws,
          hostName: ws.displayName,
          hostRole: ws.role,
          clients: [ws]
        };

        rooms.set(code, room);

        console.log("room created", code, "host", ws.displayName, "rooms", rooms.size);

        ws.send(JSON.stringify({
          type: "created",
          room: code,
          clientId: ws.id,
          peers: peerListAll(room)
        }));

        broadcastRoomList();
        break;
      }

      case "join": {
        browsers.delete(ws);

        const code = String(msg.room || "").toUpperCase();
        const room = rooms.get(code);

        if (!room) {
          ws.send(JSON.stringify({ type: "error", message: "Room not found" }));
          break;
        }

        const openClients = room.clients.filter((client) => client.readyState === WebSocket.OPEN);
        if (openClients.length >= 5) {
          ws.send(JSON.stringify({ type: "error", message: "Room full" }));
          break;
        }

        ws.displayName = msg.name || "Guest";
        ws.role = msg.role || null;
        ws.room = code;

        room.clients = openClients;
        room.clients.push(ws);

        console.log("room joined", code, "guest", ws.displayName, "peers", room.clients.length);

        ws.send(JSON.stringify({
          type: "joined",
          room: code,
          clientId: ws.id,
          peers: peerListAll(room)
        }));

        broadcastToRoom(room, {
          type: "peerJoined",
          peers: peerListAll(room)
        });

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

  ws.on("error", (error) => {
    console.error("websocket error", error.message);
  });
});

function sendRoomList(ws) {
  if (ws.readyState !== WebSocket.OPEN) return;

  const list = Array.from(rooms.values()).map((room) => ({
    code: room.code,
    hostName: room.hostName || "Host",
    hostRole: room.hostRole || null,
    peerCount: room.clients.filter((client) => client.readyState === WebSocket.OPEN).length
  }));

  console.log("send roomList", list.length, "to", ws.displayName || "?");

  ws.send(JSON.stringify({
    type: "roomList",
    rooms: list
  }));
}

function broadcastRoomList() {
  for (const ws of browsers) {
    sendRoomList(ws);
  }
}

function peerListAll(room) {
  return room.clients
    .filter((client) => client.readyState === WebSocket.OPEN)
    .map((client) => ({
      id: client.id,
      name: client.displayName || "?",
      role: client.role || null
    }));
}

function broadcastToRoom(room, msg) {
  const data = JSON.stringify(msg);

  for (const client of room.clients) {
    if (client.readyState === WebSocket.OPEN) {
      client.send(data);
    }
  }
}

function relay(sender, msg) {
  if (!sender.room) return;

  const room = rooms.get(sender.room);
  if (!room) return;

  const data = JSON.stringify({
    ...msg,
    senderPeerID: sender.id
  });

  for (const client of room.clients) {
    if (client !== sender && client.readyState === WebSocket.OPEN) {
      client.send(data);
    }
  }
}

function removeFromRoom(ws) {
  if (!ws.room) return;

  const room = rooms.get(ws.room);
  if (!room) {
    ws.room = null;
    return;
  }

  room.clients = room.clients.filter((client) => client !== ws && client.readyState === WebSocket.OPEN);

  if (room.host === ws || room.clients.length === 0) {
    console.log("room closed", room.code);
    rooms.delete(room.code);
  } else {
    console.log("peer left", room.code, "remaining", room.clients.length);
    broadcastToRoom(room, {
      type: "peerLeft",
      peers: peerListAll(room)
    });
  }

  ws.room = null;
}

setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) {
      browsers.delete(ws);
      removeFromRoom(ws);
      ws.terminate();
      continue;
    }

    ws.isAlive = false;
    ws.ping();
  }

  broadcastRoomList();
}, 30000);

httpServer.listen(PORT, () => {
  console.log(`Repa sync server running on port ${PORT}`);
});
