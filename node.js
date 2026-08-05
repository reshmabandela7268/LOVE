const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: "*", methods: ["GET", "POST"] }
});

// Serve static files (the HTML client)
app.use(express.static(__dirname));

// In-memory storage (use MongoDB/PostgreSQL for production)
const users = new Map();
const messages = new Map(); // chatId -> [messages]
const groups = new Map();
const statuses = new Map();

io.on('connection', (socket) => {
  console.log('User connected:', socket.id);

  // Register user
  socket.on('register', (data) => {
    const { username, displayName, publicKey } = data;
    users.set(username, {
      username,
      displayName: displayName || username,
      publicKey,
      socketId: socket.id,
      online: true,
      lastSeen: Date.now()
    });
    socket.username = username;
    socket.broadcast.emit('user_online', { username, displayName });
    socket.emit('register_success', { username });
    console.log('Registered:', username);
  });

  // Login user
  socket.on('login', (data) => {
    const { username } = data;
    const user = users.get(username);
    if (user) {
      user.socketId = socket.id;
      user.online = true;
      user.lastSeen = Date.now();
      socket.username = username;
      socket.emit('login_success', { username, displayName: user.displayName });
      socket.broadcast.emit('user_online', { username, displayName: user.displayName });
      
      // Send pending messages
      for (const [chatId, msgs] of messages) {
        const pending = msgs.filter(m => m.recipient === username && !m.delivered);
        pending.forEach(m => {
          socket.emit('message', m);
          m.delivered = true;
        });
      }
    } else {
      socket.emit('login_error', 'User not found');
    }
  });

  // Send message
  socket.on('send_message', (data) => {
    const msg = {
      id: data.id || 'msg_' + Date.now(),
      chatId: data.chatId,
      sender: socket.username,
      recipient: data.recipient,
      type: data.type || 'text',
      content: data.content,
      fileName: data.fileName,
      fileSize: data.fileSize,
      duration: data.duration,
      replyTo: data.replyTo,
      timestamp: Date.now(),
      delivered: false,
      read: false,
      reactions: {}
    };

    // Store message
    if (!messages.has(msg.chatId)) messages.set(msg.chatId, []);
    messages.get(msg.chatId).push(msg);

    // Send to recipient if online
    const recipient = users.get(data.recipient);
    if (recipient && recipient.online) {
      io.to(recipient.socketId).emit('message', msg);
      msg.delivered = true;
    }

    // Confirm to sender
    socket.emit('message_sent', { id: msg.id, chatId: msg.chatId });
  });

  // Group messages
  socket.on('send_group_message', (data) => {
    const group = groups.get(data.chatId);
    if (!group) return;

    const msg = {
      id: 'msg_' + Date.now(),
      chatId: data.chatId,
      sender: socket.username,
      type: data.type || 'text',
      content: data.content,
      timestamp: Date.now(),
      reactions: {}
    };

    if (!messages.has(msg.chatId)) messages.set(msg.chatId, []);
    messages.get(msg.chatId).push(msg);

    // Broadcast to all group members
    group.members.forEach(member => {
      const user = users.get(member);
      if (user && user.online && member !== socket.username) {
        io.to(user.socketId).emit('message', msg);
      }
    });

    socket.emit('message_sent', { id: msg.id, chatId: msg.chatId });
  });

  // Typing indicator
  socket.on('typing', (data) => {
    const recipient = users.get(data.recipient);
    if (recipient && recipient.online) {
      io.to(recipient.socketId).emit('typing', { sender: socket.username });
    }
  });

  // Read receipts
  socket.on('read_receipt', (data) => {
    const msgs = messages.get(data.chatId) || [];
    msgs.forEach(m => {
      if (m.sender === data.reader && !m.read) {
        m.read = true;
      }
    });
    const sender = users.get(data.reader);
    if (sender && sender.online) {
      io.to(sender.socketId).emit('read_receipt', { chatId: data.chatId, reader: socket.username });
    }
  });

  // Reactions
  socket.on('reaction', (data) => {
    const msgs = messages.get(data.chatId) || [];
    const msg = msgs.find(m => m.id === data.msgId);
    if (msg) {
      if (!msg.reactions[data.emoji]) msg.reactions[data.emoji] = [];
      const idx = msg.reactions[data.emoji].indexOf(socket.username);
      if (idx > -1) msg.reactions[data.emoji].splice(idx, 1);
      else msg.reactions[data.emoji].push(socket.username);
      
      // Notify other participant
      const other = msg.sender === socket.username ? msg.recipient : msg.sender;
      const otherUser = users.get(other);
      if (otherUser && otherUser.online) {
        io.to(otherUser.socketId).emit('reaction', data);
      }
    }
  });

  // Create group
  socket.on('create_group', (data) => {
    const group = {
      id: data.id,
      name: data.name,
      description: data.description,
      members: data.members,
      admins: [socket.username],
      createdBy: socket.username,
      createdAt: Date.now()
    };
    groups.set(data.id, group);
    
    // Notify members
    data.members.forEach(member => {
      const user = users.get(member);
      if (user && user.online) {
        io.to(user.socketId).emit('group_invite', group);
      }
    });
    
    socket.emit('group_created', group);
  });

  // Status updates
  socket.on('status_update', (data) => {
    const status = {
      username: socket.username,
      type: data.type,
      content: data.content,
      timestamp: Date.now()
    };
    statuses.set(socket.username, status);
    socket.broadcast.emit('status_update', status);
  });

  // Get online users
  socket.on('get_online_users', () => {
    const online = Array.from(users.values())
      .filter(u => u.online)
      .map(u => ({ username: u.username, displayName: u.displayName }));
    socket.emit('online_users', online);
  });

  // WebRTC Signaling
  socket.on('call_offer', (data) => {
    const recipient = users.get(data.recipient);
    if (recipient && recipient.online) {
      io.to(recipient.socketId).emit('call_offer', {
        offer: data.offer,
        callType: data.callType,
        sender: socket.username
      });
    }
  });

  socket.on('call_answer', (data) => {
    const recipient = users.get(data.recipient);
    if (recipient && recipient.online) {
      io.to(recipient.socketId).emit('call_answer', { answer: data.answer, sender: socket.username });
    }
  });

  socket.on('call_ice', (data) => {
    const recipient = users.get(data.recipient);
    if (recipient && recipient.online) {
      io.to(recipient.socketId).emit('call_ice', { candidate: data.candidate, sender: socket.username });
    }
  });

  socket.on('call_end', (data) => {
    const recipient = users.get(data.recipient);
    if (recipient && recipient.online) {
      io.to(recipient.socketId).emit('call_end', { sender: socket.username });
    }
  });

  socket.on('call_reject', (data) => {
    const recipient = users.get(data.recipient);
    if (recipient && recipient.online) {
      io.to(recipient.socketId).emit('call_reject', { sender: socket.username });
    }
  });

  // Disconnect
  socket.on('disconnect', () => {
    console.log('User disconnected:', socket.id);
    if (socket.username) {
      const user = users.get(socket.username);
      if (user) {
        user.online = false;
        user.lastSeen = Date.now();
        socket.broadcast.emit('user_offline', { username: socket.username });
      }
    }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`SecureChat Server running on port ${PORT}`);
  console.log(`Open http://localhost:${PORT} in your browser`);
});
