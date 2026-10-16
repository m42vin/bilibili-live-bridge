// 使用本地平台桩验证 Worker 的签名、交付和场次清理，不连接真实平台。
import assert from "node:assert/strict";
import { test } from "node:test";
import { createServer } from "node:http";
import { once } from "node:events";
import { readFileSync } from "node:fs";
import { spawn } from "node:child_process";
import { createHash, createHmac } from "node:crypto";
import { Miniflare } from "miniflare";
import { WebSocketServer } from "ws";

function packet(op, data) {
  const body = Buffer.from(
      typeof data === "string" ? data : JSON.stringify(data),
    ),
    result = Buffer.alloc(16 + body.length);
  result.writeUInt32BE(result.length);
  result.writeUInt16BE(16, 4);
  result.writeUInt32BE(op, 8);
  body.copy(result, 16);
  return result;
}

function inbox(ws) {
  const queue = [];
  let wake;
  ws.addEventListener("message", (event) => {
    const message = JSON.parse(event.data);
    if (message.type === "ping") {
      ws.send(JSON.stringify({ type: "pong", nonce: message.nonce }));
      return;
    }
    queue.push(message);
    wake?.();
  });
  return async () => {
    while (!queue.length)
      await Promise.race([
        new Promise((r) => (wake = r)),
        new Promise((_, reject) => {
          setTimeout(() => reject(Error("message timeout")), 5000).unref();
        }),
      ]);
    return queue.shift();
  };
}

test(
  "HTTP / Room DO / signed upstream / ACK replay / alarms / cleanup",
  { timeout: 120000 },
  async () => {
    let starts = 0,
      beats = 0,
      wsBeats = 0;
    const ended = [],
      sockets = [];
    const server = createServer(async (req, res) => {
      const chunks = [];
      for await (const c of req) chunks.push(c);
      const body = Buffer.concat(chunks).toString();
      assert.equal(
        req.headers["x-bili-content-md5"],
        createHash("md5").update(body).digest("hex"),
      );
      const canonical = Object.entries(req.headers)
        .filter(([k]) => k.startsWith("x-bili-"))
        .sort(([a], [b]) => a.localeCompare(b))
        .map(([k, v]) => `${k}:${v}`)
        .join("\n");
      assert.equal(
        req.headers.authorization,
        createHmac("sha256", "test-secret").update(canonical).digest("hex"),
      );
      const input = JSON.parse(body);
      let data = null;
      if (req.url === "/v2/app/start") {
        starts++;
        data = {
          game_info: { game_id: `game-${starts}` },
          websocket_info: {
            auth_body: input.code === "reject" ? "reject" : "auth",
            wss_link: [`ws://127.0.0.1:${server.address().port}/upstream`],
          },
          anchor_info: {
            room_id:
              { unused: 456, reject: 789, restart: 321 }[input.code] ?? 123,
          },
        };
      } else if (req.url === "/v2/app/heartbeat") beats++;
      else if (req.url === "/v2/app/end") ended.push(input.game_id);
      else assert.fail(req.url);
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ code: 0, data }));
      server.emit("platform-progress");
    });
    const wss = new WebSocketServer({ noServer: true });
    server.on("upgrade", (req, socket, head) =>
      wss.handleUpgrade(req, socket, head, (ws) => wss.emit("connection", ws)),
    );
    wss.on("connection", (ws) => {
      sockets.push(ws);
      ws.on("message", (bytes) => {
        const op = bytes.readUInt32BE(8);
        if (op === 7) {
          ws.send(
            packet(8, {
              code: bytes.subarray(16).toString() === "reject" ? -1 : 0,
            }),
          );
        } else if (op === 2) {
          wsBeats++;
          ws.send(packet(3, ""));
          server.emit("platform-progress");
        } else assert.fail(`operation ${op}`);
      });
    });
    server.listen(0, "127.0.0.1");
    await once(server, "listening");
    // 注册通知前先检查事实；同一事件循环内检查与注册之间没有异步让出。
    const waitForPlatform = async (predicate) => {
      const signal = AbortSignal.timeout(60000);
      while (!predicate()) {
        await once(server, "platform-progress", { signal });
      }
    };

    let mf;
    try {
      const child = spawn(
        "worker-build",
        ["--dev", "--no-opt", "--out-dir", "build/test"],
        {
          env: {
            ...process.env,
            CARGO_NET_OFFLINE: "true",
            BRIDGE_WORKER_TEST_API_ORIGIN: `http://127.0.0.1:${server.address().port}`,
          },
          stdio: "inherit",
        },
      );
      assert.equal((await once(child, "exit"))[0], 0);
      const text = (value) => ({ type: "text", value });
      mf = new Miniflare({
        telemetry: { enabled: false },
        workers: [
          {
            config: {
              name: "bridge-test",
              compatibilityDate: "2026-10-06",
              compatibilityFlags: [
                "no_websocket_standard_binary_type",
                "no_nodejs_global_timers",
              ],
              manifest: {
                mainModule: "index.js",
                modules: {
                  "index.js": {
                    type: "esm",
                    contents: readFileSync("build/test/index.js", "utf8"),
                  },
                  "index_bg.wasm": {
                    type: "wasm",
                    contents: new Uint8Array(
                      readFileSync("build/test/index_bg.wasm"),
                    ),
                  },
                },
              },
              env: {
                BILIBILI_APP_ID: text("1"),
                BILIBILI_ACCESS_KEY_ID: text("test-key"),
                BILIBILI_ACCESS_KEY_SECRET: text("test-secret"),
                ADMISSIONS: {
                  type: "durable-object",
                  worker: "bridge-test",
                  exportName: "Admission",
                },
                ROOMS: {
                  type: "durable-object",
                  worker: "bridge-test",
                  exportName: "Room",
                },
              },
              exports: {
                Admission: { type: "durable-object", storage: "sqlite" },
                Room: { type: "durable-object", storage: "sqlite" },
              },
            },
          },
        ],
      });
      const admit = (body) =>
        mf.dispatchFetch("http://bridge.test/admit", {
          method: "POST",
          body: JSON.stringify(body),
        });
      const upgrade = (url) =>
        mf.dispatchFetch(url.replace(/^ws/, "http"), {
          headers: { Upgrade: "websocket" },
        });
      assert.equal((await admit({ code: "" })).status, 400);
      assert.equal(starts, 0);
      assert.equal(
        (
          await admit({
            code: "first",
            ack: true,
            resume_after: { stream_id: "missing", seq: 1 },
          })
        ).status,
        409,
      );
      assert.equal(starts, 0);

      const response = await admit({ code: "first", ack: true, ack_window: 1 });
      assert.equal(response.status, 200, await response.clone().text());
      const granted = await response.json();
      assert(!granted.websocket_url.includes("first"));
      const connection = await upgrade(granted.websocket_url);
      assert.equal(connection.status, 101);
      const ws = connection.webSocket,
        next = inbox(ws);
      ws.accept();
      const ready = await next();
      assert.equal(ready.type, "ready");
      assert.equal(ready.ack_window, 1);
      assert.equal(starts, 1);
      assert.equal((await upgrade(granted.websocket_url)).status, 401);

      sockets[0].send(packet(5, { cmd: "TEST", data: { n: 1 } }));
      sockets[0].send(packet(5, { cmd: "TEST", data: { n: 2 } }));
      assert.equal((await next()).seq, 1);
      ws.send(
        JSON.stringify({ type: "ack", stream_id: ready.stream_id, seq: 1 }),
      );
      assert.equal((await next()).seq, 2);
      ws.close();

      const resumedResponse = await admit({
        code: "first",
        ack: true,
        resume_after: { stream_id: ready.stream_id, seq: 1 },
      });
      assert.equal(resumedResponse.status, 200);
      const resumed = await resumedResponse.json(),
        connection2 = await upgrade(resumed.websocket_url),
        ws2 = connection2.webSocket,
        next2 = inbox(ws2);
      ws2.accept();
      assert.equal((await next2()).resumed, true);
      assert.equal((await next2()).seq, 2);
      ws2.send(
        JSON.stringify({ type: "ack", stream_id: ready.stream_id, seq: 2 }),
      );
      assert.equal(starts, 1);

      assert.equal((await admit({ code: "alias", ack: true })).status, 200);
      assert.equal(starts, 2);
      assert(ended.includes("game-2"));
      assert.equal(sockets.length, 1);

      const unusedResponse = await admit({ code: "unused", ack: true });
      assert.equal(unusedResponse.status, 200);
      const unused = await unusedResponse.json();
      const unusedGame = `game-${starts}`;
      // alarm 由真实运行时触发，等待双心跳和未使用凭证的场次实际结束。
      await waitForPlatform(
        () => beats > 0 && wsBeats > 0 && ended.includes(unusedGame),
      );
      assert(beats > 0);
      assert(wsBeats > 0);
      assert(
        ended.includes(unusedGame),
        "unused credential must release its upstream game",
      );
      assert([401, 410].includes((await upgrade(unused.websocket_url)).status));

      sockets[0].send(
        packet(5, {
          cmd: "LIVE_OPEN_PLATFORM_INTERACTION_END",
          data: { game_id: "game-1", timestamp: 1 },
        }),
      );
      const terminal = await next2();
      assert.equal(terminal.type, "event");
      ws2.send(
        JSON.stringify({
          type: "ack",
          stream_id: ready.stream_id,
          seq: terminal.seq,
        }),
      );
      assert.equal((await next2()).type, "closed");
      await waitForPlatform(() => ended.includes("game-1"));
      assert(ended.includes("game-1"));

      // 对象重建丢弃内存日志，旧游标不得静默触发新场次。
      const beforeReset = starts;
      await mf.unsafeEvictDurableObject("bridge-test", "Room", {
        name: "1:123",
        webSockets: "close",
      });
      assert.equal(
        (
          await admit({
            code: "first",
            ack: true,
            resume_after: { stream_id: ready.stream_id, seq: 2 },
          })
        ).status,
        409,
      );
      assert.equal(starts, beforeReset);

      // 即使从未连接下游，对象重建后也必须履行已知场次的清理责任。
      assert.equal((await admit({ code: "restart", ack: true })).status, 200);
      const restartGame = `game-${starts}`;
      await mf.unsafeEvictDurableObject("bridge-test", "Room", {
        name: "1:321",
        webSockets: "close",
      });
      assert.equal(
        (
          await admit({
            code: "restart",
            ack: true,
            resume_after: { stream_id: "old", seq: 0 },
          })
        ).status,
        409,
      );
      assert(ended.includes(restartGame));

      // 上游明确拒绝鉴权时不得发布 ready。
      const rejected = await (
        await admit({ code: "reject", ack: true })
      ).json();
      const rejectedGame = `game-${starts}`;
      const badConnection = await upgrade(rejected.websocket_url),
        badWs = badConnection.webSocket,
        badNext = inbox(badWs);
      badWs.accept();
      assert.equal((await badNext()).type, "error");
      await waitForPlatform(() => ended.includes(rejectedGame));
      assert(ended.includes(rejectedGame));
    } finally {
      await mf?.dispose();
      for (const ws of sockets) ws.terminate();
      wss.close();
      server.close();
    }
  },
);
