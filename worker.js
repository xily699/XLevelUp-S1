/**
 * XLevelUp Standalone VLESS Worker
 * این ورکر خودش پروتکل VLESS رو parse و هندل می‌کنه — بدون نیاز به بک‌اند.
 * از TCP Sockets API واقعی کلودفلر (cloudflare:sockets) استفاده می‌کنه.
 * اندپوینت: /vlees   |   دیپلوی: wrangler deploy
 *
 * ⚠️ قبل از دیپلوی: USER_ID رو با UUID واقعی خودت جایگزین کن.
 * ⚠️ محدودیت پلن رایگان: CPU-time هر رکوئست محدوده؛ برای ترافیک سنگین به Railway فالبک کن.
 */
import { connect } from 'cloudflare:sockets';

const USER_ID = 'REPLACE-WITH-YOUR-UUID';
const WS_PATH = '/vlees';

export default {
  async fetch(request, env, ctx) {
    try {
      const url = new URL(request.url);

      if (url.pathname === '/api/ping') {
        return jsonResp({ ok: true, ts: Date.now(), node: 'cf-worker-vless' });
      }
      if (url.pathname === '/api/status') {
        return jsonResp({
          worker: 'online',
          mode: 'standalone-vless',
          path: WS_PATH,
          timestamp: new Date().toISOString(),
        });
      }

      const upgrade = request.headers.get('Upgrade');
      if (upgrade && upgrade.toLowerCase() === 'websocket' && url.pathname === WS_PATH) {
        return await vlessOverWSHandler(request);
      }

      return new Response('XLevelUp Edge — Standalone VLESS Worker', { status: 200 });
    } catch (err) {
      return new Response('worker error: ' + err.toString(), { status: 500 });
    }
  },
};

function jsonResp(obj) {
  return new Response(JSON.stringify(obj), { headers: { 'content-type': 'application/json' } });
}

// --- هندلر اصلی WS<->VLESS<->TCP ---
async function vlessOverWSHandler(request) {
  const { 0: client, 1: server } = new WebSocketPair();
  server.accept();

  const remoteSocket = { value: null };
  let vlessResponseHeader = null;
  let headerParsed = false;

  const earlyDataHeader = request.headers.get('sec-websocket-protocol') || '';
  const readable = makeWebSocketReadable(server, earlyDataHeader);

  readable
    .pipeTo(
      new WritableStream({
        async write(chunk) {
          if (headerParsed && remoteSocket.value) {
            const writer = remoteSocket.value.writable.getWriter();
            await writer.write(chunk);
            writer.releaseLock();
            return;
          }

          const parsed = parseVlessHeader(chunk, USER_ID);
          if (parsed.hasError) {
            safeCloseWebSocket(server);
            throw new Error('VLESS header error: ' + parsed.message);
          }
          if (parsed.isUDP) {
            safeCloseWebSocket(server);
            throw new Error('UDP در این نسخه پشتیبانی نمی‌شه (فقط TCP)');
          }

          headerParsed = true;
          vlessResponseHeader = new Uint8Array([parsed.vlessVersion[0], 0]);
          const rawClientData = chunk.slice(parsed.rawDataIndex);

          const tcpSocket = connect({ hostname: parsed.addressRemote, port: parsed.portRemote });
          remoteSocket.value = tcpSocket;

          const writer = tcpSocket.writable.getWriter();
          await writer.write(rawClientData);
          writer.releaseLock();

          pipeRemoteToWebSocket(tcpSocket, server, vlessResponseHeader);
        },
        close() {
          safeCloseWebSocket(server);
        },
        abort() {
          safeCloseWebSocket(server);
        },
      })
    )
    .catch(() => {
      safeCloseWebSocket(server);
    });

  return new Response(null, { status: 101, webSocket: client });
}

function pipeRemoteToWebSocket(remoteSocket, webSocket, vlessResponseHeader) {
  let headerSent = false;
  remoteSocket.readable
    .pipeTo(
      new WritableStream({
        write(chunk) {
          if (webSocket.readyState !== 1) return;
          if (!headerSent) {
            const combined = new Uint8Array(vlessResponseHeader.length + chunk.byteLength);
            combined.set(vlessResponseHeader, 0);
            combined.set(new Uint8Array(chunk), vlessResponseHeader.length);
            webSocket.send(combined);
            headerSent = true;
          } else {
            webSocket.send(chunk);
          }
        },
        close() {
          safeCloseWebSocket(webSocket);
        },
        abort() {
          safeCloseWebSocket(webSocket);
        },
      })
    )
    .catch(() => {
      safeCloseWebSocket(webSocket);
    });
}

// --- تبدیل رویدادهای WebSocket به ReadableStream (پشتیبانی از 0-RTT early data) ---
function makeWebSocketReadable(ws, earlyDataHeader) {
  let cancelled = false;
  return new ReadableStream({
    start(controller) {
      ws.addEventListener('message', (event) => {
        if (cancelled) return;
        controller.enqueue(event.data);
      });
      ws.addEventListener('close', () => {
        if (cancelled) return;
        controller.close();
      });
      ws.addEventListener('error', (err) => {
        controller.error(err);
      });

      const { earlyData, error } = base64ToArrayBuffer(earlyDataHeader);
      if (error) controller.error(error);
      else if (earlyData) controller.enqueue(earlyData);
    },
    cancel() {
      cancelled = true;
      safeCloseWebSocket(ws);
    },
  });
}

function base64ToArrayBuffer(base64Str) {
  if (!base64Str) return { earlyData: null, error: null };
  try {
    base64Str = base64Str.replace(/-/g, '+').replace(/_/g, '/');
    const decoded = atob(base64Str);
    const bytes = Uint8Array.from(decoded, (c) => c.charCodeAt(0));
    return { earlyData: bytes.buffer, error: null };
  } catch (error) {
    return { earlyData: null, error };
  }
}

function safeCloseWebSocket(socket) {
  try {
    if (socket.readyState === 1 || socket.readyState === 2) socket.close();
  } catch (_) {}
}

// --- پارس هدر واقعی پروتکل VLESS ---
function parseVlessHeader(buffer, userID) {
  if (buffer.byteLength < 24) {
    return { hasError: true, message: 'داده خیلی کوتاهه' };
  }
  const view = new DataView(buffer);
  const version = new Uint8Array(buffer.slice(0, 1));

  const uuidBytes = new Uint8Array(buffer.slice(1, 17));
  const uuidStr = bytesToUUID(uuidBytes);
  if (uuidStr !== userID) {
    return { hasError: true, message: 'UUID نامعتبر است' };
  }

  const optLength = new Uint8Array(buffer.slice(17, 18))[0];
  const command = new Uint8Array(buffer.slice(18 + optLength, 19 + optLength))[0];

  let isUDP = false;
  if (command === 2) isUDP = true;
  else if (command !== 1) {
    return { hasError: true, message: `دستور پشتیبانی‌نشده: ${command}` };
  }

  const portIndex = 19 + optLength;
  const portRemote = view.getUint16(portIndex);

  const addressIndex = portIndex + 2;
  const addressType = new Uint8Array(buffer.slice(addressIndex, addressIndex + 1))[0];

  let addressLength = 0;
  let addressValueIndex = addressIndex + 1;
  let addressValue = '';

  switch (addressType) {
    case 1: // IPv4
      addressLength = 4;
      addressValue = Array.from(
        new Uint8Array(buffer.slice(addressValueIndex, addressValueIndex + addressLength))
      ).join('.');
      break;
    case 2: // Domain
      addressLength = new Uint8Array(buffer.slice(addressValueIndex, addressValueIndex + 1))[0];
      addressValueIndex += 1;
      addressValue = new TextDecoder().decode(
        buffer.slice(addressValueIndex, addressValueIndex + addressLength)
      );
      break;
    case 3: { // IPv6
      addressLength = 16;
      const dv = new DataView(buffer.slice(addressValueIndex, addressValueIndex + addressLength));
      const parts = [];
      for (let i = 0; i < 8; i++) parts.push(dv.getUint16(i * 2).toString(16));
      addressValue = parts.join(':');
      break;
    }
    default:
      return { hasError: true, message: `نوع آدرس پشتیبانی‌نشده: ${addressType}` };
  }

  if (!addressValue) {
    return { hasError: true, message: 'آدرس مقصد خالیه' };
  }

  return {
    hasError: false,
    addressRemote: addressValue,
    addressType,
    portRemote,
    rawDataIndex: addressValueIndex + addressLength,
    vlessVersion: version,
    isUDP,
  };
}

function bytesToUUID(bytes) {
  const hex = [...bytes].map((b) => b.toString(16).padStart(2, '0'));
  return `${hex.slice(0, 4).join('')}-${hex.slice(4, 6).join('')}-${hex.slice(6, 8).join('')}-${hex
    .slice(8, 10)
    .join('')}-${hex.slice(10, 16).join('')}`;
}
