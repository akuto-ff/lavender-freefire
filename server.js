"use strict";

const express = require("express");
const http = require("http");
const path = require("path");
const fs = require("fs");
const crypto = require("crypto");
const { promisify } = require("util");
const { Server } = require("socket.io");

const scryptAsync = promisify(crypto.scrypt);

const APP_VERSION = "7.3.5";
const PORT = Number(process.env.PORT || 3000);
const ROOT = __dirname;
const DATA_FILE = path.join(ROOT, "data.json");

const ADMIN_USER = String(process.env.LAVENDER_ADMIN_USER || "admin");
const ADMIN_PASSWORD = String(process.env.LAVENDER_ADMIN_PASSWORD || "lavender123");
const AUTH_SECRET = String(
  process.env.LAVENDER_SESSION_SECRET ||
    ADMIN_USER + ":" + ADMIN_PASSWORD + ":lavender-v7"
);

const COOKIE_SECURE = process.env.NODE_ENV === "production";
const TOKEN_TTL = 12 * 60 * 60 * 1000;

const app = express();
const httpServer = http.createServer(app);

const io = new Server(httpServer, {
  transports: ["websocket", "polling"],
  pingInterval: 25000,
  pingTimeout: 20000
});

app.disable("x-powered-by");

app.use(express.json({ limit: "4mb" }));

app.use(
  express.static(ROOT, {
    index: false,
    etag: true,
    maxAge: process.env.NODE_ENV === "production" ? "5m" : 0
  })
);

function baseData() {
  return {
    version: 1,

    settings: {
      siteName: "LAVENDER",
      tagline: "FREE FIRE COMMUNITY",
      accent: "#b46cff"
    },

    guilds: [],
    players: [],
    matches: [],
    tournaments: [],
    news: [],

    streamers: [],
    streamerStreams: [],

    overlay: {
      activeMatchId: null,
      visible: true,
      accent: "#b46cff",
      position: "bottom",
      showPlayers: true,
      showStats: true,
      customText: "LAVENDER • LIVE"
    }
  };
}

function normalize(d) {
  const b = baseData();

  if (!d || typeof d !== "object") {
    d = {};
  }

  d.version = Number(d.version) || 1;

  d.settings = {
    ...b.settings,
    ...(d.settings || {})
  };

  for (const k of [
    "guilds",
    "players",
    "matches",
    "tournaments",
    "news",
    "streamers",
    "streamerStreams"
  ]) {
    if (!Array.isArray(d[k])) {
      d[k] = [];
    }
  }

  d.overlay = {
    ...b.overlay,
    ...(d.overlay || {})
  };

  return d;
}

function readData() {
  try {
    return normalize(
      JSON.parse(
        fs.readFileSync(DATA_FILE, "utf8")
      )
    );
  } catch (err) {
    console.error("DATA read failed:", err.message);

    const d = baseData();

    atomicWrite(d);

    return d;
  }
}

function atomicWrite(d) {
  const tmp = DATA_FILE + ".tmp";

  fs.writeFileSync(
    tmp,
    JSON.stringify(normalize(d), null, 2),
    "utf8"
  );

  fs.renameSync(tmp, DATA_FILE);
}

function nextId(arr) {
  return arr.length
    ? Math.max(
        ...arr.map(x => Number(x.id) || 0)
      ) + 1
    : 1;
}

function rankForElo(elo) {
  const e = Number(elo) || 0;

  if (e >= 2000) return "S";
  if (e >= 1800) return "A";
  if (e >= 1600) return "B";
  if (e >= 1400) return "C";
  if (e >= 1200) return "D";
  if (e >= 1000) return "E";

  return "F";
}

/* ================================
   ELO
================================ */

function eloExpected(rA, rB) {
  return 1 / (
    1 +
    Math.pow(
      10,
      (rB - rA) / 400
    )
  );
}

function eloDelta(
  rA,
  rB,
  scoreA,
  k = 32
) {
  return Math.round(
    k *
      (
        scoreA -
        eloExpected(rA, rB)
      )
  );
}

function applyMatchElo(d, m) {
  // чтобы один матч нельзя было посчитать два раза
  if (m.eloApplied) {
    return null;
  }

  const a = d.players.find(
    p =>
      Number(p.id) ===
      Number(m.playerAId)
  );

  const b = d.players.find(
    p =>
      Number(p.id) ===
      Number(m.playerBId)
  );

  if (!a || !b) {
    return null;
  }

  const sa = Number(m.scoreA);
  const sb = Number(m.scoreB);

  if (sa === sb) {
    return null;
  }

  const oldA =
    Number(a.elo) || 1200;

  const oldB =
    Number(b.elo) || 1200;

  const scoreA =
    sa > sb ? 1 : 0;

  const delta =
    eloDelta(
      oldA,
      oldB,
      scoreA,
      32
    );

  a.elo =
    Math.max(
      0,
      oldA + delta
    );

  b.elo =
    Math.max(
      0,
      oldB - delta
    );

  if (scoreA === 1) {
    a.wins =
      (Number(a.wins) || 0) + 1;

    b.losses =
      (Number(b.losses) || 0) + 1;
  } else {
    b.wins =
      (Number(b.wins) || 0) + 1;

    a.losses =
      (Number(a.losses) || 0) + 1;
  }

  a.updatedAt =
    new Date().toISOString();

  b.updatedAt =
    new Date().toISOString();

  m.eloApplied = true;

  m.eloChange = {
    playerAId: a.id,
    playerBId: b.id,

    beforeA: oldA,
    beforeB: oldB,

    afterA: a.elo,
    afterB: b.elo,

    deltaA: delta,
    deltaB: -delta,

    appliedAt:
      new Date().toISOString()
  };

  return m.eloChange;
}

/* ================================
   HELPERS
================================ */

function cleanText(
  v,
  max = 120
) {
  return String(v ?? "")
    .trim()
    .slice(0, max);
}

function cleanImage(
  v,
  fallback
) {
  v = String(v || "").trim();

  if (!v) {
    return fallback;
  }

  if (
    v.startsWith("data:image/")
  ) {
    return v.length <= 3000000
      ? v
      : fallback;
  }

  return v.slice(0, 64);
}

function parseCookies(req) {
  const out = {};

  String(
    req.headers.cookie || ""
  )
    .split(";")
    .forEach(part => {
      const i =
        part.indexOf("=");

      if (i > 0) {
        out[
          part
            .slice(0, i)
            .trim()
        ] =
          decodeURIComponent(
            part
              .slice(i + 1)
              .trim()
          );
      }
    });

  return out;
}

/* ================================
   AUTH
================================ */

function sign(value) {
  return crypto
    .createHmac(
      "sha256",
      AUTH_SECRET
    )
    .update(value)
    .digest("base64url");
}

function makeToken(payloadObj) {
  const payload =
    Buffer.from(
      JSON.stringify({
        ...payloadObj,
        t: Date.now()
      }),
      "utf8"
    ).toString("base64url");

  return (
    payload +
    "." +
    sign(payload)
  );
}

function verifyToken(token) {
  try {
    const [payload, sig] =
      String(token || "")
        .split(".");

    if (!payload || !sig) {
      return null;
    }

    const expected =
      sign(payload);

    const a =
      Buffer.from(sig);

    const b =
      Buffer.from(expected);

    if (
      a.length !== b.length ||
      !crypto.timingSafeEqual(
        a,
        b
      )
    ) {
      return null;
    }

    const obj =
      JSON.parse(
        Buffer.from(
          payload,
          "base64url"
        ).toString("utf8")
      );

    if (
      !Number.isFinite(obj.t) ||
      Date.now() - obj.t >
        TOKEN_TTL
    ) {
      return null;
    }

    return obj;
  } catch {
    return null;
  }
}

function authInfo(req) {
  return verifyToken(
    parseCookies(req)
      .lavender_session || ""
  );
}

function isAdmin(req) {
  return (
    authInfo(req)?.role ===
    "admin"
  );
}

function requireAdmin(
  req,
  res,
  next
) {
  if (!isAdmin(req)) {
    return res
      .status(401)
      .json({
        error:
          "Нужен вход администратора"
      });
  }

  next();
}

function requireStreamer(
  req,
  res,
  next
) {
  const a =
    authInfo(req);

  if (
    !a ||
    a.role !== "streamer"
  ) {
    return res
      .status(401)
      .json({
        error:
          "Нужен вход стримера"
      });
  }

  const d = readData();

  const streamer =
    d.streamers.find(
      x =>
        Number(x.id) ===
          Number(
            a.streamerId
          ) &&
        x.active !== false
    );

  if (!streamer) {
    return res
      .status(401)
      .json({
        error:
          "Аккаунт стримера отключён"
      });
  }

  req.streamer =
    streamer;

  next();
}

function requireEditor(
  req,
  res,
  next
) {
  const a =
    authInfo(req);

  if (!a) {
    return res
      .status(401)
      .json({
        error:
          "Нужен вход администратора или стримера"
      });
  }

  if (
    a.role === "admin"
  ) {
    req.editor = {
      role: "admin",
      id: null,
      name: ADMIN_USER
    };

    return next();
  }

  if (
    a.role ===
    "streamer"
  ) {
    const d =
      readData();

    const st =
      d.streamers.find(
        x =>
          Number(x.id) ===
            Number(
              a.streamerId
            ) &&
          x.active !== false
      );

    if (!st) {
      return res
        .status(401)
        .json({
          error:
            "Аккаунт стримера отключён"
        });
    }

    req.editor = {
      role: "streamer",
      id: st.id,
      name:
        st.displayName ||
        st.username
    };

    return next();
  }

  return res
    .status(403)
    .json({
      error:
        "Нет прав редактора"
    });
}

async function hashPassword(
  password
) {
  const salt =
    crypto
      .randomBytes(16)
      .toString("hex");

  const derived =
    await scryptAsync(
      String(password),
      salt,
      64
    );

  return (
    salt +
    ":" +
    derived.toString("hex")
  );
}

async function verifyPassword(
  password,
  stored
) {
  try {
    const [
      salt,
      keyHex
    ] =
      String(stored || "")
        .split(":");

    if (
      !salt ||
      !keyHex
    ) {
      return false;
    }

    const derived =
      await scryptAsync(
        String(password),
        salt,
        64
      );

    const key =
      Buffer.from(
        keyHex,
        "hex"
      );

    return (
      key.length ===
        derived.length &&
      crypto.timingSafeEqual(
        key,
        derived
      )
    );
  } catch {
    return false;
  }
}

/* ================================
   PUBLIC DATA
================================ */

function publicData(
  d = readData()
) {
  const guildMap =
    Object.fromEntries(
      d.guilds.map(
        g => [
          Number(g.id),
          g
        ]
      )
    );

  const players =
    d.players
      .map(p => {
        const wins =
          Number(p.wins) || 0;

        const losses =
          Number(p.losses) || 0;

        const kills =
          Number(p.kills) || 0;

        const deaths =
          Number(p.deaths) || 0;

        return {
          ...p,

          guild:
            guildMap[
              Number(
                p.guildId
              )
            ] || null,

          rank:
            rankForElo(
              p.elo
            ),

          kd:
            deaths
              ? Number(
                  (
                    kills /
                    deaths
                  ).toFixed(2)
                )
              : kills,

          winrate:
            wins + losses
              ? Math.round(
                  wins /
                    (
                      wins +
                      losses
                    ) *
                    100
                )
              : 0
        };
      })
      .sort(
        (a, b) =>
          (Number(b.elo) ||
            0) -
          (Number(a.elo) ||
            0)
      );

  const playerMap =
    Object.fromEntries(
      players.map(
        p => [
          Number(p.id),
          p
        ]
      )
    );

  const guilds =
    d.guilds
      .map(g => {
        const roster =
          players.filter(
            p =>
              Number(
                p.guildId
              ) ===
              Number(g.id)
          );

        const wins =
          Number(g.wins) ||
          0;

        const losses =
          Number(
            g.losses
          ) || 0;

        return {
          ...g,

          rank:
            rankForElo(
              g.elo
            ),

          memberCount:
            roster.length,

          roster,

          winrate:
            wins + losses
              ? Math.round(
                  wins /
                    (
                      wins +
                      losses
                    ) *
                    100
                )
              : 0
        };
      })
      .sort(
        (a, b) =>
          (Number(b.elo) ||
            0) -
          (Number(a.elo) ||
            0)
      );

  const matches =
    d.matches
      .map(m => ({
        ...m,

        guildA:
          guildMap[
            Number(
              m.guildAId
            )
          ] || null,

        guildB:
          guildMap[
            Number(
              m.guildBId
            )
          ] || null,

        playerA:
          playerMap[
            Number(
              m.playerAId
            )
          ] || null,

        playerB:
          playerMap[
            Number(
              m.playerBId
            )
          ] || null
      }))
      .sort(
        (a, b) =>
          Number(b.id) -
          Number(a.id)
      );

  const tournaments =
    d.tournaments
      .map(t => ({
        ...t,

        participants:
          (t.guildIds || [])
            .map(
              x =>
                guildMap[
                  Number(x)
                ]
            )
            .filter(Boolean)
      }))
      .sort(
        (a, b) =>
          Number(b.id) -
          Number(a.id)
      );

  const publicStreamers =
    d.streamers
      .filter(
        s =>
          s.active !== false
      )
      .map(
        ({
          passwordHash,
          ...s
        }) => {
          const stream =
            d.streamerStreams.find(
              x =>
                Number(
                  x.streamerId
                ) ===
                Number(s.id)
            ) || null;

          return {
            ...s,
            stream
          };
        }
      );

  return {
    ...d,
    players,
    guilds,
    matches,
    tournaments,
    streamers:
      publicStreamers,
    streamerStreams:
      undefined
  };
}

function globalOverlayState() {
  const d =
    publicData();

  const m =
    d.matches.find(
      x =>
        Number(x.id) ===
        Number(
          d.overlay
            .activeMatchId
        )
    ) ||
    d.matches[0] ||
    null;

  return {
    overlay:
      d.overlay,
    match: m,
    settings:
      d.settings,
    version:
      APP_VERSION
  };
}

function streamerOverlayState(
  streamerId
) {
  const d =
    publicData();

  const streamer =
    d.streamers.find(
      s =>
        Number(s.id) ===
        Number(streamerId)
    );

  if (!streamer) {
    return null;
  }

  const st =
    streamer.stream || {};

  const baseMatch =
    d.matches.find(
      x =>
        Number(x.id) ===
        Number(st.matchId)
    ) ||
    d.matches[0] ||
    null;

  let m =
    baseMatch
      ? { ...baseMatch }
      : null;

  if (m) {
    const pA =
      d.players.find(
        p =>
          Number(p.id) ===
          Number(
            st.playerAId
          )
      );

    const pB =
      d.players.find(
        p =>
          Number(p.id) ===
          Number(
            st.playerBId
          )
      );

    if (pA) {
      m.playerAId =
        pA.id;

      m.playerA =
        pA;
    }

    if (pB) {
      m.playerBId =
        pB.id;

      m.playerB =
        pB;
    }
  }

  return {
    streamer,

    overlay: {
      visible:
        st.status ===
        "LIVE",

      accent:
        st.accent ||
        d.settings.accent ||
        "#b46cff",

      position:
        st.position ||
        "bottom",

      showPlayers:
        st.showPlayers !==
        false,

      showStats:
        st.showStats !==
        false,

      customText:
        st.customText ||
        `${
          streamer.displayName ||
          streamer.username
        } • LIVE`
    },

    match: m,

    settings:
      d.settings,

    version:
      APP_VERSION
  };
}

function broadcastGlobal() {
  io.emit(
    "overlay:update",
    globalOverlayState()
  );

  io.emit(
    "site:update",
    {
      at: Date.now()
    }
  );
}

function broadcastStreamer(id) {
  io
    .to(
      "streamer:" + id
    )
    .emit(
      "streamer-overlay:update",
      streamerOverlayState(id)
    );

  io.emit(
    "site:update",
    {
      at: Date.now()
    }
  );
}

function ok(
  res,
  data
) {
  res.json(data);
}

function fail(
  res,
  msg,
  code = 400
) {
  res
    .status(code)
    .json({
      error: msg
    });
}

/* ================================
   BASIC API
================================ */

app.get(
  "/health",
  (req, res) =>
    ok(res, {
      ok: true,
      version:
        APP_VERSION,
      uptime:
        Math.round(
          process.uptime()
        )
    })
);

app.get(
  "/api/all",
  (req, res) =>
    ok(
      res,
      publicData()
    )
);

app.get(
  "/api/overlay",
  (req, res) =>
    ok(
      res,
      globalOverlayState()
    )
);

app.get(
  "/api/streamer-overlay/:id",
  (req, res) => {
    const x =
      streamerOverlayState(
        req.params.id
      );

    if (!x) {
      return fail(
        res,
        "Стример не найден",
        404
      );
    }

    ok(res, x);
  }
);

/* ================================
   LOGIN
================================ */

app.get(
  "/api/auth/status",
  (req, res) => {
    const a =
      authInfo(req);

    if (!a) {
      return ok(
        res,
        {
          authenticated:
            false,
          role: null,
          version:
            APP_VERSION
        }
      );
    }

    if (
      a.role ===
      "admin"
    ) {
      return ok(
        res,
        {
          authenticated:
            true,
          role:
            "admin",
          user:
            ADMIN_USER,
          version:
            APP_VERSION
        }
      );
    }

    const d =
      readData();

    const s =
      d.streamers.find(
        x =>
          Number(x.id) ===
            Number(
              a.streamerId
            ) &&
          x.active !== false
      );

    if (!s) {
      return ok(
        res,
        {
          authenticated:
            false,
          role: null,
          version:
            APP_VERSION
        }
      );
    }

    ok(res, {
      authenticated:
        true,

      role:
        "streamer",

      streamer: {
        id: s.id,
        username:
          s.username,
        displayName:
          s.displayName,
        avatar:
          s.avatar
      },

      version:
        APP_VERSION
    });
  }
);

app.post(
  "/api/auth/login",
  async (
    req,
    res
  ) => {
    const username =
      String(
        req.body
          ?.username ||
          ""
      ).trim();

    const password =
      String(
        req.body
          ?.password ||
          ""
      );

    if (
      username ===
        ADMIN_USER &&
      password ===
        ADMIN_PASSWORD
    ) {
      const token =
        makeToken({
          role: "admin",
          u: ADMIN_USER
        });

      res.setHeader(
        "Set-Cookie",
        `lavender_session=${token}; HttpOnly; SameSite=Lax; Path=/; Max-Age=43200${
          COOKIE_SECURE
            ? "; Secure"
            : ""
        }`
      );

      return ok(
        res,
        {
          ok: true,
          role:
            "admin"
        }
      );
    }

    const d =
      readData();

    const s =
      d.streamers.find(
        x =>
          x.active !==
            false &&
          String(
            x.username
          ).toLowerCase() ===
            username.toLowerCase()
      );

    if (
      !s ||
      !(await verifyPassword(
        password,
        s.passwordHash
      ))
    ) {
      return fail(
        res,
        "Неверный логин или пароль",
        401
      );
    }

    const token =
      makeToken({
        role:
          "streamer",

        streamerId:
          s.id,

        u:
          s.username
      });

    res.setHeader(
      "Set-Cookie",
      `lavender_session=${token}; HttpOnly; SameSite=Lax; Path=/; Max-Age=43200${
        COOKIE_SECURE
          ? "; Secure"
          : ""
      }`
    );

    ok(res, {
      ok: true,
      role:
        "streamer",
      streamerId:
        s.id
    });
  }
);

app.post(
  "/api/auth/logout",
  (req, res) => {
    res.setHeader(
      "Set-Cookie",
      `lavender_session=; HttpOnly; SameSite=Lax; Path=/; Max-Age=0${
        COOKIE_SECURE
          ? "; Secure"
          : ""
      }`
    );

    ok(res, {
      ok: true
    });
  }
);

app.get(
  "/api/admin/ping",
  requireAdmin,
  (req, res) =>
    ok(res, {
      ok: true,
      version:
        APP_VERSION
    })
);

app.get(
  "/api/streamer/ping",
  requireStreamer,
  (req, res) =>
    ok(res, {
      ok: true,
      streamerId:
        req.streamer.id,
      version:
        APP_VERSION
    })
);

/* ================================
   STREAMER MANAGEMENT
================================ */

app.get(
  "/api/admin/streamers",
  requireAdmin,
  (req, res) => {
    const d =
      readData();

    ok(
      res,
      d.streamers.map(
        ({
          passwordHash,
          ...s
        }) => ({
          ...s,

          stream:
            d.streamerStreams.find(
              x =>
                Number(
                  x.streamerId
                ) ===
                Number(s.id)
            ) || null
        })
      )
    );
  }
);

app.post(
  "/api/admin/streamers",
  requireAdmin,
  async (
    req,
    res
  ) => {
    const d =
      readData();

    const b =
      req.body || {};

    const username =
      cleanText(
        b.username,
        40
      );

    const displayName =
      cleanText(
        b.displayName ||
          username,
        60
      );

    const password =
      String(
        b.password || ""
      );

    if (
      !username ||
      password.length < 4
    ) {
      return fail(
        res,
        "Укажи логин и пароль минимум 4 символа"
      );
    }

    if (
      d.streamers.some(
        s =>
          String(
            s.username
          ).toLowerCase() ===
          username.toLowerCase()
      )
    ) {
      return fail(
        res,
        "Такой логин уже существует"
      );
    }

    const streamer = {
      id:
        nextId(
          d.streamers
        ),

      username,

      displayName,

      avatar:
        cleanImage(
          b.avatar,
          "🎥"
        ),

      active: true,

      passwordHash:
        await hashPassword(
          password
        ),

      createdAt:
        new Date()
          .toISOString()
    };

    d.streamers.push(
      streamer
    );

    d.streamerStreams.push({
      id:
        nextId(
          d.streamerStreams
        ),

      streamerId:
        streamer.id,

      status:
        "OFFLINE",

      title:
        "Мой стрим",

      platform:
        "YouTube",

      streamUrl:
        "",

      matchId:
        d.matches[0]?.id ||
        null,

      playerAId:
        d.matches[0]
          ?.playerAId ||
        null,

      playerBId:
        d.matches[0]
          ?.playerBId ||
        null,

      accent:
        d.settings.accent ||
        "#b46cff",

      position:
        "bottom",

      showPlayers:
        true,

      showStats:
        true,

      customText:
        `${displayName} • LIVE`,

      updatedAt:
        new Date()
          .toISOString()
    });

    atomicWrite(d);

    broadcastStreamer(
      streamer.id
    );

    ok(res, {
      id:
        streamer.id,
      username:
        streamer.username,
      displayName:
        streamer.displayName,
      avatar:
        streamer.avatar,
      active:
        streamer.active
    });
  }
);

app.patch(
  "/api/admin/streamers/:id",
  requireAdmin,
  async (
    req,
    res
  ) => {
    const d =
      readData();

    const streamer =
      d.streamers.find(
        x =>
          Number(x.id) ===
          Number(
            req.params.id
          )
      );

    if (!streamer) {
      return fail(
        res,
        "Стример не найден",
        404
      );
    }

    const b =
      req.body || {};

    if (
      "username" in b
    ) {
      streamer.username =
        cleanText(
          b.username,
          40
        );
    }

    if (
      "displayName" in b
    ) {
      streamer.displayName =
        cleanText(
          b.displayName,
          60
        );
    }

    if (
      "avatar" in b
    ) {
      streamer.avatar =
        cleanImage(
          b.avatar,
          streamer.avatar ||
            "🎥"
        );
    }

    if (
      "active" in b
    ) {
      streamer.active =
        !!b.active;
    }

    if (b.password) {
      streamer.passwordHash =
        await hashPassword(
          String(
            b.password
          )
        );
    }

    atomicWrite(d);

    broadcastStreamer(
      streamer.id
    );

    ok(res, {
      id:
        streamer.id,
      username:
        streamer.username,
      displayName:
        streamer.displayName,
      avatar:
        streamer.avatar,
      active:
        streamer.active
    });
  }
);

app.delete(
  "/api/admin/streamers/:id",
  requireAdmin,
  (req, res) => {
    const d =
      readData();

    const id =
      Number(
        req.params.id
      );

    d.streamers =
      d.streamers.filter(
        s =>
          Number(s.id) !==
          id
      );

    d.streamerStreams =
      d.streamerStreams.filter(
        s =>
          Number(
            s.streamerId
          ) !== id
      );

    atomicWrite(d);

    io
      .to(
        "streamer:" + id
      )
      .emit(
        "streamer-removed"
      );

    io.emit(
      "site:update",
      {
        at: Date.now()
      }
    );

    ok(res, {
      ok: true
    });
  }
);

/* ================================
   STREAMER ZONE
================================ */

app.get(
  "/api/streamer/me",
  requireStreamer,
  (req, res) => {
    const d =
      readData();

    const st =
      d.streamerStreams.find(
        x =>
          Number(
            x.streamerId
          ) ===
          Number(
            req.streamer.id
          )
      ) || null;

    const pub =
      publicData(d);

    ok(res, {
      streamer: {
        id:
          req.streamer.id,

        username:
          req.streamer
            .username,

        displayName:
          req.streamer
            .displayName,

        avatar:
          req.streamer
            .avatar
      },

      stream: st,

      matches:
        pub.matches,

      players:
        pub.players,

      guilds:
        pub.guilds
    });
  }
);

app.patch(
  "/api/streamer/me",
  requireStreamer,
  (req, res) => {
    const d =
      readData();

    const b =
      req.body || {};

    let st =
      d.streamerStreams.find(
        x =>
          Number(
            x.streamerId
          ) ===
          Number(
            req.streamer.id
          )
      );

    if (!st) {
      st = {
        id:
          nextId(
            d.streamerStreams
          ),

        streamerId:
          req.streamer.id,

        status:
          "OFFLINE",

        title:
          "Мой стрим",

        platform:
          "YouTube",

        streamUrl:
          "",

        matchId:
          d.matches[0]?.id ||
          null,

        playerAId:
          d.matches[0]
            ?.playerAId ||
          null,

        playerBId:
          d.matches[0]
            ?.playerBId ||
          null,

        accent:
          d.settings
            .accent ||
          "#b46cff",

        position:
          "bottom",

        showPlayers:
          true,

        showStats:
          true,

        customText:
          `${
            req.streamer
              .displayName ||
            req.streamer
              .username
          } • LIVE`
      };

      d.streamerStreams.push(
        st
      );
    }

    if (
      "status" in b
    ) {
      st.status =
        [
          "LIVE",
          "OFFLINE",
          "PAUSED"
        ].includes(
          String(
            b.status
          ).toUpperCase()
        )
          ? String(
              b.status
            ).toUpperCase()
          : st.status;
    }

    if (
      "title" in b
    ) {
      st.title =
        cleanText(
          b.title,
          100
        );
    }

    if (
      "platform" in b
    ) {
      st.platform =
        cleanText(
          b.platform,
          30
        );
    }

    if (
      "streamUrl" in b
    ) {
      st.streamUrl =
        cleanText(
          b.streamUrl,
          300
        );
    }

    if (
      "matchId" in b
    ) {
      st.matchId =
        b.matchId
          ? Number(
              b.matchId
            )
          : null;
    }

    if (
      "playerAId" in b
    ) {
      st.playerAId =
        b.playerAId
          ? Number(
              b.playerAId
            )
          : null;
    }

    if (
      "playerBId" in b
    ) {
      st.playerBId =
        b.playerBId
          ? Number(
              b.playerBId
            )
          : null;
    }

    if (
      "accent" in b &&
      /^#[0-9a-f]{6}$/i.test(
        b.accent
      )
    ) {
      st.accent =
        b.accent;
    }

    if (
      "position" in b
    ) {
      st.position =
        b.position === "top"
          ? "top"
          : "bottom";
    }

    if (
      "showPlayers" in b
    ) {
      st.showPlayers =
        !!b.showPlayers;
    }

    if (
      "showStats" in b
    ) {
      st.showStats =
        !!b.showStats;
    }

    if (
      "customText" in b
    ) {
      st.customText =
        cleanText(
          b.customText,
          80
        );
    }

    st.updatedAt =
      new Date()
        .toISOString();

    atomicWrite(d);

    broadcastStreamer(
      req.streamer.id
    );

    ok(res, st);
  }
);

/* ================================
   SETTINGS
================================ */

app.patch(
  "/api/settings",
  requireAdmin,
  (req, res) => {
    const d =
      readData();

    d.settings.siteName =
      cleanText(
        req.body
          ?.siteName ||
          d.settings.siteName,
        40
      ) ||
      "LAVENDER";

    d.settings.tagline =
      cleanText(
        req.body
          ?.tagline ||
          d.settings.tagline,
        100
      );

    if (
      /^#[0-9a-f]{6}$/i.test(
        req.body
          ?.accent ||
          ""
      )
    ) {
      d.settings.accent =
        req.body.accent;
    }

    atomicWrite(d);

    broadcastGlobal();

    ok(
      res,
      d.settings
    );
  }
);

/* ================================
   GUILDS
================================ */

app.post(
  "/api/guilds",
  requireEditor,
  (req, res) => {
    const d =
      readData();

    const b =
      req.body || {};

    const name =
      cleanText(
        b.name,
        50
      );

    const tag =
      cleanText(
        b.tag,
        12
      ).toUpperCase();

    if (
      !name ||
      !tag
    ) {
      return fail(
        res,
        "Укажи название и тег"
      );
    }

    if (
      d.guilds.some(
        g =>
          String(
            g.tag
          ).toUpperCase() ===
          tag
      )
    ) {
      return fail(
        res,
        "Такой тег уже существует"
      );
    }

    const g = {
      id:
        nextId(
          d.guilds
        ),

      name,
      tag,

      logo:
        cleanImage(
          b.logo,
          "🪻"
        ),

      region:
        cleanText(
          b.region ||
            "Кыргызстан",
          40
        ),

      elo:
        Number(b.elo) ||
        1200,

      wins:
        Number(b.wins) ||
        0,

      losses:
        Number(
          b.losses
        ) || 0,

      description:
        cleanText(
          b.description,
          400
        ),

      captain:
        cleanText(
          b.captain,
          50
        ),

      createdByRole:
        req.editor.role,

      createdById:
        req.editor.id,

      createdByName:
        req.editor.name,

      createdAt:
        new Date()
          .toISOString()
    };

    d.guilds.push(g);

    atomicWrite(d);

    broadcastGlobal();

    ok(res, g);
  }
);

app.patch(
  "/api/guilds/:id",
  requireEditor,
  (req, res) => {
    const d =
      readData();

    const g =
      d.guilds.find(
        x =>
          Number(x.id) ===
          Number(
            req.params.id
          )
      );

    if (!g) {
      return fail(
        res,
        "Гильдия не найдена",
        404
      );
    }

    const b =
      req.body || {};

    if (
      "name" in b
    ) {
      g.name =
        cleanText(
          b.name,
          50
        );
    }

    if (
      "tag" in b
    ) {
      g.tag =
        cleanText(
          b.tag,
          12
        ).toUpperCase();
    }

    if (
      "logo" in b
    ) {
      g.logo =
        cleanImage(
          b.logo,
          g.logo ||
            "🪻"
        );
    }

    if (
      "region" in b
    ) {
      g.region =
        cleanText(
          b.region,
          40
        );
    }

    if (
      "elo" in b
    ) {
      g.elo =
        Number(
          b.elo
        ) || 0;
    }

    if (
      "wins" in b
    ) {
      g.wins =
        Number(
          b.wins
        ) || 0;
    }

    if (
      "losses" in b
    ) {
      g.losses =
        Number(
          b.losses
        ) || 0;
    }

    if (
      "description" in b
    ) {
      g.description =
        cleanText(
          b.description,
          400
        );
    }

    if (
      "captain" in b
    ) {
      g.captain =
        cleanText(
          b.captain,
          50
        );
    }

    g.updatedByRole =
      req.editor.role;

    g.updatedById =
      req.editor.id;

    g.updatedByName =
      req.editor.name;

    g.updatedAt =
      new Date()
        .toISOString();

    atomicWrite(d);

    broadcastGlobal();

    ok(res, g);
  }
);

app.delete(
  "/api/guilds/:id",
  requireEditor,
  (req, res) => {
    const d =
      readData();

    const gid =
      Number(
        req.params.id
      );

    d.guilds =
      d.guilds.filter(
        g =>
          Number(g.id) !==
          gid
      );

    d.players.forEach(
      p => {
        if (
          Number(
            p.guildId
          ) === gid
        ) {
          p.guildId =
            null;
        }
      }
    );

    d.matches.forEach(
      m => {
        if (
          Number(
            m.guildAId
          ) === gid
        ) {
          m.guildAId =
            null;
        }

        if (
          Number(
            m.guildBId
          ) === gid
        ) {
          m.guildBId =
            null;
        }
      }
    );

    atomicWrite(d);

    broadcastGlobal();

    ok(res, {
      ok: true
    });
  }
);

/* ================================
   PLAYERS
================================ */

app.post(
  "/api/players",
  requireEditor,
  (req, res) => {
    const d =
      readData();

    const b =
      req.body || {};

    const nickname =
      cleanText(
        b.nickname,
        50
      );

    if (!nickname) {
      return fail(
        res,
        "Укажи ник игрока"
      );
    }

    const p = {
      id:
        nextId(
          d.players
        ),

      nickname,

      gameId:
        cleanText(
          b.gameId,
          40
        ),

      avatar:
        cleanImage(
          b.avatar,
          "👤"
        ),

      guildId:
        b.guildId
          ? Number(
              b.guildId
            )
          : null,

      elo:
        Number(b.elo) ||
        1200,

      wins:
        Number(b.wins) ||
        0,

      losses:
        Number(
          b.losses
        ) || 0,

      kills:
        Number(
          b.kills
        ) || 0,

      deaths:
        Number(
          b.deaths
        ) || 0,

      role:
        cleanText(
          b.role ||
            "Player",
          30
        ),

      country:
        cleanText(
          b.country ||
            "Кыргызстан",
          40
        ),

      createdByRole:
        req.editor.role,

      createdById:
        req.editor.id,

      createdByName:
        req.editor.name,

      createdAt:
        new Date()
          .toISOString()
    };

    d.players.push(p);

    atomicWrite(d);

    broadcastGlobal();

    ok(res, p);
  }
);

app.patch(
  "/api/players/:id",
  requireEditor,
  (req, res) => {
    const d =
      readData();

    const p =
      d.players.find(
        x =>
          Number(x.id) ===
          Number(
            req.params.id
          )
      );

    if (!p) {
      return fail(
        res,
        "Игрок не найден",
        404
      );
    }

    const b =
      req.body || {};

    if (
      "nickname" in b
    ) {
      p.nickname =
        cleanText(
          b.nickname,
          50
        );
    }

    if (
      "gameId" in b
    ) {
      p.gameId =
        cleanText(
          b.gameId,
          40
        );
    }

    if (
      "avatar" in b
    ) {
      p.avatar =
        cleanImage(
          b.avatar,
          p.avatar ||
            "👤"
        );
    }

    if (
      "guildId" in b
    ) {
      p.guildId =
        b.guildId
          ? Number(
              b.guildId
            )
          : null;
    }

    for (const k of [
      "elo",
      "wins",
      "losses",
      "kills",
      "deaths"
    ]) {
      if (
        k in b
      ) {
        p[k] =
          Number(b[k]) ||
          0;
      }
    }

    p.updatedByRole =
      req.editor.role;

    p.updatedById =
      req.editor.id;

    p.updatedByName =
      req.editor.name;

    p.updatedAt =
      new Date()
        .toISOString();

    atomicWrite(d);

    broadcastGlobal();

    ok(res, p);
  }
);

app.delete(
  "/api/players/:id",
  requireEditor,
  (req, res) => {
    const d =
      readData();

    const pid =
      Number(
        req.params.id
      );

    d.players =
      d.players.filter(
        p =>
          Number(p.id) !==
          pid
      );

    d.matches.forEach(
      m => {
        if (
          Number(
            m.playerAId
          ) === pid
        ) {
          m.playerAId =
            null;
        }

        if (
          Number(
            m.playerBId
          ) === pid
        ) {
          m.playerBId =
            null;
        }
      }
    );

    atomicWrite(d);

    broadcastGlobal();

    ok(res, {
      ok: true
    });
  }
);

/* ================================
   MATCHES
================================ */

app.post(
  "/api/matches",
  requireEditor,
  (req, res) => {
    const d =
      readData();

    const b =
      req.body || {};

    const m = {
      id:
        nextId(
          d.matches
        ),

      tournament:
        cleanText(
          b.tournament ||
            "LAVENDER CUP",
          80
        ),

      title:
        cleanText(
          b.title ||
            "LIVE MATCH",
          80
        ),

      subtitle:
        cleanText(
          b.subtitle ||
            "FREE FIRE",
          80
        ),

      guildAId:
        b.guildAId
          ? Number(
              b.guildAId
            )
          : null,

      guildBId:
        b.guildBId
          ? Number(
              b.guildBId
            )
          : null,

      scoreA:
        Number(
          b.scoreA
        ) || 0,

      scoreB:
        Number(
          b.scoreB
        ) || 0,

      roundText:
        cleanText(
          b.roundText ||
            "ROUND 1",
          30
        ),

      status:
        cleanText(
          b.status ||
            "SCHEDULED",
          20
        ).toUpperCase(),

      format:
        cleanText(
          b.format ||
            "BO7",
          20
        ),

      playerAId:
        b.playerAId
          ? Number(
              b.playerAId
            )
          : null,

      playerBId:
        b.playerBId
          ? Number(
              b.playerBId
            )
          : null,

      createdByRole:
        req.editor.role,

      createdById:
        req.editor.id,

      createdByName:
        req.editor.name,

      createdAt:
        new Date()
          .toISOString()
    };

    d.matches.push(m);

    d.overlay.activeMatchId =
      m.id;

    if (
      m.status ===
      "FINAL"
    ) {
      applyMatchElo(
        d,
        m
      );
    }

    atomicWrite(d);

    broadcastGlobal();

    ok(res, m);
  }
);

app.patch(
  "/api/matches/:id",
  requireEditor,
  (req, res) => {
    const d =
      readData();

    const m =
      d.matches.find(
        x =>
          Number(x.id) ===
          Number(
            req.params.id
          )
      );

    if (!m) {
      return fail(
        res,
        "Матч не найден",
        404
      );
    }

    const b =
      req.body || {};

    for (const k of [
      "tournament",
      "title",
      "subtitle",
      "roundText",
      "status",
      "format"
    ]) {
      if (
        k in b
      ) {
        m[k] =
          cleanText(
            b[k],
            80
          );
      }
    }

    for (const k of [
      "guildAId",
      "guildBId",
      "playerAId",
      "playerBId"
    ]) {
      if (
        k in b
      ) {
        m[k] =
          b[k]
            ? Number(
                b[k]
              )
            : null;
      }
    }

    if (
      "scoreA" in b
    ) {
      m.scoreA =
        Math.max(
          0,
          Number(
            b.scoreA
          ) || 0
        );
    }

    if (
      "scoreB" in b
    ) {
      m.scoreB =
        Math.max(
          0,
          Number(
            b.scoreB
          ) || 0
        );
    }

    m.status =
      String(
        m.status ||
          "LIVE"
      ).toUpperCase();

    m.updatedByRole =
      req.editor.role;

    m.updatedById =
      req.editor.id;

    m.updatedByName =
      req.editor.name;

    m.updatedAt =
      new Date()
        .toISOString();

    if (
      m.status ===
      "FINAL"
    ) {
      applyMatchElo(
        d,
        m
      );
    }

    atomicWrite(d);

    broadcastGlobal();

    d.streamers.forEach(
      streamer =>
        broadcastStreamer(
          streamer.id
        )
    );

    ok(res, m);
  }
);

/* ================================
   FINISH BY SCORE
================================ */

app.post(
  "/api/matches/:id/finish",
  requireEditor,
  (req, res) => {
    const d =
      readData();

    const m =
      d.matches.find(
        x =>
          Number(x.id) ===
          Number(
            req.params.id
          )
      );

    if (!m) {
      return fail(
        res,
        "Матч не найден",
        404
      );
    }

    const b =
      req.body || {};

    if (
      "scoreA" in b
    ) {
      m.scoreA =
        Math.max(
          0,
          Number(
            b.scoreA
          ) || 0
        );
    }

    if (
      "scoreB" in b
    ) {
      m.scoreB =
        Math.max(
          0,
          Number(
            b.scoreB
          ) || 0
        );
    }

    if (
      "playerAId" in b
    ) {
      m.playerAId =
        b.playerAId
          ? Number(
              b.playerAId
            )
          : null;
    }

    if (
      "playerBId" in b
    ) {
      m.playerBId =
        b.playerBId
          ? Number(
              b.playerBId
            )
          : null;
    }

    if (
      "roundText" in b
    ) {
      m.roundText =
        cleanText(
          b.roundText,
          30
        );
    }

    if (
      "format" in b
    ) {
      m.format =
        cleanText(
          b.format,
          20
        );
    }

    if (
      Number(
        m.scoreA
      ) ===
      Number(
        m.scoreB
      )
    ) {
      return fail(
        res,
        "Нельзя завершить матч при равном счёте"
      );
    }

    if (
      !m.playerAId ||
      !m.playerBId
    ) {
      return fail(
        res,
        "Выбери двух игроков перед завершением матча"
      );
    }

    const pA =
      d.players.find(
        p =>
          Number(p.id) ===
          Number(
            m.playerAId
          )
      );

    const pB =
      d.players.find(
        p =>
          Number(p.id) ===
          Number(
            m.playerBId
          )
      );

    if (
      !pA ||
      !pB
    ) {
      return fail(
        res,
        "Один из игроков не найден"
      );
    }

    if (
      Number(pA.id) ===
      Number(pB.id)
    ) {
      return fail(
        res,
        "Нельзя выбрать одного игрока с двух сторон"
      );
    }

    m.status =
      "FINAL";

    m.updatedByRole =
      req.editor.role;

    m.updatedById =
      req.editor.id;

    m.updatedByName =
      req.editor.name;

    m.updatedAt =
      new Date()
        .toISOString();

    const change =
      applyMatchElo(
        d,
        m
      );

    atomicWrite(d);

    broadcastGlobal();

    d.streamers.forEach(
      st =>
        broadcastStreamer(
          st.id
        )
    );

    ok(res, {
      ok: true,

      match: m,

      eloChange:
        change ||
        m.eloChange ||
        null,

      players: {
        A: {
          id: pA.id,
          nickname:
            pA.nickname,
          elo:
            pA.elo
        },

        B: {
          id: pB.id,
          nickname:
            pB.nickname,
          elo:
            pB.elo
        }
      }
    });
  }
);

/* ================================
   CHOOSE WINNER -> AUTO ELO
================================ */

app.post(
  "/api/matches/:id/set-winner",
  requireEditor,
  (req, res) => {
    const d =
      readData();

    const m =
      d.matches.find(
        x =>
          Number(x.id) ===
          Number(
            req.params.id
          )
      );

    if (!m) {
      return fail(
        res,
        "Матч не найден",
        404
      );
    }

    const winnerId =
      Number(
        req.body
          ?.winnerId
      );

    const playerAId =
      Number(
        req.body
          ?.playerAId ||
          m.playerAId
      );

    const playerBId =
      Number(
        req.body
          ?.playerBId ||
          m.playerBId
      );

    if (
      !playerAId ||
      !playerBId
    ) {
      return fail(
        res,
        "Выбери двух игроков"
      );
    }

    if (
      playerAId ===
      playerBId
    ) {
      return fail(
        res,
        "Нельзя выбрать одного игрока дважды"
      );
    }

    if (
      winnerId !==
        playerAId &&
      winnerId !==
        playerBId
    ) {
      return fail(
        res,
        "Выбери победителя из этих двух игроков"
      );
    }

    m.playerAId =
      playerAId;

    m.playerBId =
      playerBId;

    m.scoreA =
      winnerId ===
      playerAId
        ? 1
        : 0;

    m.scoreB =
      winnerId ===
      playerBId
        ? 1
        : 0;

    m.status =
      "FINAL";

    m.updatedByRole =
      req.editor.role;

    m.updatedById =
      req.editor.id;

    m.updatedByName =
      req.editor.name;

    m.updatedAt =
      new Date()
        .toISOString();

    const change =
      applyMatchElo(
        d,
        m
      );

    atomicWrite(d);

    broadcastGlobal();

    d.streamers.forEach(
      st =>
        broadcastStreamer(
          st.id
        )
    );

    const pA =
      d.players.find(
        p =>
          Number(p.id) ===
          playerAId
      );

    const pB =
      d.players.find(
        p =>
          Number(p.id) ===
          playerBId
      );

    ok(res, {
      ok: true,

      match: m,

      eloChange:
        change ||
        m.eloChange ||
        null,

      winnerId,

      players: {
        A: {
          id: pA.id,
          nickname:
            pA.nickname,
          elo:
            pA.elo
        },

        B: {
          id: pB.id,
          nickname:
            pB.nickname,
          elo:
            pB.elo
        }
      }
    });
  }
);

app.delete(
  "/api/matches/:id",
  requireEditor,
  (req, res) => {
    const d =
      readData();

    const mid =
      Number(
        req.params.id
      );

    d.matches =
      d.matches.filter(
        m =>
          Number(m.id) !==
          mid
      );

    if (
      Number(
        d.overlay
          .activeMatchId
      ) === mid
    ) {
      d.overlay.activeMatchId =
        d.matches[0]?.id ||
        null;
    }

    d.streamerStreams.forEach(
      st => {
        if (
          Number(
            st.matchId
          ) === mid
        ) {
          st.matchId =
            d.matches[0]?.id ||
            null;
        }
      }
    );

    atomicWrite(d);

    broadcastGlobal();

    ok(res, {
      ok: true
    });
  }
);

/* ================================
   TOURNAMENTS
================================ */

app.post(
  "/api/tournaments",
  requireAdmin,
  (req, res) => {
    const d =
      readData();

    const b =
      req.body || {};

    const name =
      cleanText(
        b.name,
        80
      );

    if (!name) {
      return fail(
        res,
        "Укажи название турнира"
      );
    }

    const t = {
      id:
        nextId(
          d.tournaments
        ),

      name,

      status:
        cleanText(
          b.status ||
            "UPCOMING",
          20
        ).toUpperCase(),

      date:
        cleanText(
          b.date,
          30
        ),

      format:
        cleanText(
          b.format ||
            "BO7",
          20
        ),

      prize:
        cleanText(
          b.prize,
          80
        ),

      description:
        cleanText(
          b.description,
          500
        ),

      guildIds:
        Array.isArray(
          b.guildIds
        )
          ? b.guildIds.map(
              Number
            )
          : []
    };

    d.tournaments.push(
      t
    );

    atomicWrite(d);

    broadcastGlobal();

    ok(res, t);
  }
);

app.patch(
  "/api/tournaments/:id",
  requireAdmin,
  (req, res) => {
    const d =
      readData();

    const t =
      d.tournaments.find(
        x =>
          Number(x.id) ===
          Number(
            req.params.id
          )
      );

    if (!t) {
      return fail(
        res,
        "Турнир не найден",
        404
      );
    }

    Object.assign(
      t,
      req.body || {}
    );

    t.guildIds =
      Array.isArray(
        t.guildIds
      )
        ? t.guildIds.map(
            Number
          )
        : [];

    t.status =
      String(
        t.status ||
          "UPCOMING"
      ).toUpperCase();

    atomicWrite(d);

    broadcastGlobal();

    ok(res, t);
  }
);

app.delete(
  "/api/tournaments/:id",
  requireAdmin,
  (req, res) => {
    const d =
      readData();

    d.tournaments =
      d.tournaments.filter(
        t =>
          Number(t.id) !==
          Number(
            req.params.id
          )
      );

    atomicWrite(d);

    broadcastGlobal();

    ok(res, {
      ok: true
    });
  }
);

/* ================================
   NEWS
================================ */

app.post(
  "/api/news",
  requireAdmin,
  (req, res) => {
    const d =
      readData();

    const b =
      req.body || {};

    const title =
      cleanText(
        b.title,
        120
      );

    if (!title) {
      return fail(
        res,
        "Укажи заголовок"
      );
    }

    const n = {
      id:
        nextId(
          d.news
        ),

      title,

      body:
        cleanText(
          b.body,
          1500
        ),

      pinned:
        !!b.pinned,

      createdAt:
        new Date()
          .toISOString()
    };

    d.news.unshift(n);

    atomicWrite(d);

    broadcastGlobal();

    ok(res, n);
  }
);

app.patch(
  "/api/news/:id",
  requireAdmin,
  (req, res) => {
    const d =
      readData();

    const n =
      d.news.find(
        x =>
          Number(x.id) ===
          Number(
            req.params.id
          )
      );

    if (!n) {
      return fail(
        res,
        "Новость не найдена",
        404
      );
    }

    if (
      "title" in
      (req.body || {})
    ) {
      n.title =
        cleanText(
          req.body.title,
          120
        );
    }

    if (
      "body" in
      (req.body || {})
    ) {
      n.body =
        cleanText(
          req.body.body,
          1500
        );
    }

    if (
      "pinned" in
      (req.body || {})
    ) {
      n.pinned =
        !!req.body.pinned;
    }

    atomicWrite(d);

    broadcastGlobal();

    ok(res, n);
  }
);

app.delete(
  "/api/news/:id",
  requireAdmin,
  (req, res) => {
    const d =
      readData();

    d.news =
      d.news.filter(
        n =>
          Number(n.id) !==
          Number(
            req.params.id
          )
      );

    atomicWrite(d);

    broadcastGlobal();

    ok(res, {
      ok: true
    });
  }
);

/* ================================
   OVERLAY
================================ */

app.patch(
  "/api/overlay",
  requireAdmin,
  (req, res) => {
    const d =
      readData();

    const b =
      req.body || {};

    if (
      "activeMatchId" in b
    ) {
      d.overlay.activeMatchId =
        b.activeMatchId
          ? Number(
              b.activeMatchId
            )
          : null;
    }

    if (
      "visible" in b
    ) {
      d.overlay.visible =
        !!b.visible;
    }

    if (
      "accent" in b &&
      /^#[0-9a-f]{6}$/i.test(
        b.accent
      )
    ) {
      d.overlay.accent =
        b.accent;
    }

    if (
      "position" in b
    ) {
      d.overlay.position =
        b.position === "top"
          ? "top"
          : "bottom";
    }

    if (
      "showPlayers" in b
    ) {
      d.overlay.showPlayers =
        !!b.showPlayers;
    }

    if (
      "showStats" in b
    ) {
      d.overlay.showStats =
        !!b.showStats;
    }

    if (
      "customText" in b
    ) {
      d.overlay.customText =
        cleanText(
          b.customText,
          80
        );
    }

    atomicWrite(d);

    broadcastGlobal();

    ok(
      res,
      globalOverlayState()
    );
  }
);

/* ================================
   BACKUP
================================ */

app.get(
  "/api/admin/export",
  requireAdmin,
  (req, res) => {
    res.setHeader(
      "Content-Disposition",
      'attachment; filename="lavender-backup.json"'
    );

    res
      .type(
        "application/json"
      )
      .send(
        JSON.stringify(
          readData(),
          null,
          2
        )
      );
  }
);

app.post(
  "/api/admin/import",
  requireAdmin,
  (req, res) => {
    const d =
      normalize(
        req.body
      );

    atomicWrite(d);

    broadcastGlobal();

    ok(res, {
      ok: true
    });
  }
);

/* ================================
   SOCKET.IO
================================ */

io.on(
  "connection",
  socket => {
    socket.emit(
      "overlay:update",
      globalOverlayState()
    );

    socket.on(
      "watch-streamer",
      id => {
        const sid =
          Number(id);

        if (!sid) {
          return;
        }

        socket.join(
          "streamer:" +
            sid
        );

        socket.emit(
          "streamer-overlay:update",
          streamerOverlayState(
            sid
          )
        );
      }
    );
  }
);

/* ================================
   PAGES
================================ */

app.get(
  "/",
  (req, res) =>
    res.sendFile(
      path.join(
        ROOT,
        "index.html"
      )
    )
);

app.get(
  "/overlay.html",
  (req, res) =>
    res.sendFile(
      path.join(
        ROOT,
        "overlay.html"
      )
    )
);

app.get(
  "*",
  (req, res) =>
    res.sendFile(
      path.join(
        ROOT,
        "index.html"
      )
    )
);

httpServer.listen(
  PORT,
  "0.0.0.0",
  () => {
    console.log(
      `LAVENDER PRO ${APP_VERSION} listening on ${PORT}`
    );
  }
);
