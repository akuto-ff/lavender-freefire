"use strict";

const express = require("express");
const http = require("http");
const path = require("path");
const fs = require("fs");
const crypto = require("crypto");
const { promisify } = require("util");
const { Server } = require("socket.io");
const { Pool } = require("pg");

const scryptAsync = promisify(crypto.scrypt);

const APP_VERSION = "8.0.0-postgres";
const PORT = Number(process.env.PORT || 3000);
const ROOT = __dirname;
const DATA_FILE = path.join(ROOT, "data.json");

const ADMIN_USER = String(
  process.env.LAVENDER_ADMIN_USER || "admin"
);

const ADMIN_PASSWORD = String(
  process.env.LAVENDER_ADMIN_PASSWORD || "lavender123"
);

const AUTH_SECRET = String(
  process.env.LAVENDER_SESSION_SECRET ||
  `${ADMIN_USER}:${ADMIN_PASSWORD}:lavender-v8`
);

const DATABASE_URL = String(
  process.env.DATABASE_URL || ""
);

const COOKIE_SECURE =
  process.env.NODE_ENV === "production";

const TOKEN_TTL =
  12 * 60 * 60 * 1000;

let DB = null;
let DATA_CACHE = null;
let WRITE_CHAIN = Promise.resolve();

const app = express();
const server = http.createServer(app);

const io = new Server(server, {
  transports: ["websocket", "polling"],
  pingInterval: 25000,
  pingTimeout: 20000
});

app.disable("x-powered-by");

app.use(
  express.json({
    limit: "4mb"
  })
);

app.use(
  express.static(ROOT, {
    index: false,
    etag: true,
    maxAge:
      process.env.NODE_ENV === "production"
        ? "5m"
        : 0
  })
);

/* =========================================
   DATA
========================================= */

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

function normalize(data) {
  const base = baseData();

  if (!data || typeof data !== "object") {
    data = {};
  }

  data.version =
    Number(data.version) || 1;

  data.settings = {
    ...base.settings,
    ...(data.settings || {})
  };

  for (const key of [
    "guilds",
    "players",
    "matches",
    "tournaments",
    "news",
    "streamers",
    "streamerStreams"
  ]) {
    if (!Array.isArray(data[key])) {
      data[key] = [];
    }
  }

  data.overlay = {
    ...base.overlay,
    ...(data.overlay || {})
  };

  return data;
}

function cloneData(data) {
  return JSON.parse(
    JSON.stringify(
      normalize(data)
    )
  );
}

function readSeedFile() {
  try {
    return normalize(
      JSON.parse(
        fs.readFileSync(
          DATA_FILE,
          "utf8"
        )
      )
    );
  } catch {
    return baseData();
  }
}

function readData() {
  return cloneData(
    DATA_CACHE ||
    baseData()
  );
}

function atomicWrite(data) {
  const next =
    cloneData(data);

  DATA_CACHE = next;

  WRITE_CHAIN =
    WRITE_CHAIN
      .then(() =>
        DB.query(
          `
          INSERT INTO lavender_state
          (id, data, updated_at)

          VALUES
          (1, $1::jsonb, NOW())

          ON CONFLICT (id)

          DO UPDATE SET
          data = EXCLUDED.data,
          updated_at = NOW()
          `,
          [
            JSON.stringify(next)
          ]
        )
      )
      .catch(error => {
        console.error(
          "PostgreSQL write error:",
          error
        );
      });
}

async function initDatabase() {
  if (!DATABASE_URL) {
    throw new Error(
      "DATABASE_URL отсутствует в Render Environment"
    );
  }

  DB = new Pool({
    connectionString:
      DATABASE_URL,

    ssl:
      DATABASE_URL.includes(
        "localhost"
      )
        ? false
        : {
            rejectUnauthorized: false
          },

    max: 5,

    idleTimeoutMillis:
      30000,

    connectionTimeoutMillis:
      10000
  });

  await DB.query(
    "SELECT 1"
  );

  await DB.query(`
    CREATE TABLE IF NOT EXISTS lavender_state (
      id INTEGER PRIMARY KEY,
      data JSONB NOT NULL,
      updated_at TIMESTAMPTZ
      NOT NULL DEFAULT NOW()
    )
  `);

  const result =
    await DB.query(
      `
      SELECT data
      FROM lavender_state
      WHERE id = 1
      `
    );

  if (result.rows.length) {
    DATA_CACHE =
      normalize(
        result.rows[0].data
      );

    console.log(
      "LAVENDER data loaded from PostgreSQL"
    );
  } else {
    const seed =
      readSeedFile();

    await DB.query(
      `
      INSERT INTO lavender_state
      (id, data, updated_at)

      VALUES
      (1, $1::jsonb, NOW())
      `,
      [
        JSON.stringify(seed)
      ]
    );

    DATA_CACHE = seed;

    console.log(
      "Initial LAVENDER data imported into PostgreSQL"
    );
  }
}

/* =========================================
   HELPERS
========================================= */

function nextId(arr) {
  if (!arr.length) {
    return 1;
  }

  return (
    Math.max(
      ...arr.map(
        x =>
          Number(x.id) || 0
      )
    ) + 1
  );
}

function cleanText(
  value,
  max = 120
) {
  return String(
    value ?? ""
  )
    .trim()
    .slice(0, max);
}

function cleanImage(
  value,
  fallback
) {
  value =
    String(
      value || ""
    ).trim();

  if (!value) {
    return fallback;
  }

  if (
    value.startsWith(
      "data:image/"
    )
  ) {
    return value.length <=
      3000000
      ? value
      : fallback;
  }

  return value.slice(
    0,
    64
  );
}

/* =========================================
   RANK / ELO
========================================= */

function rankForElo(elo) {
  const e =
    Number(elo) || 0;

  if (e >= 2000) return "S";
  if (e >= 1800) return "A";
  if (e >= 1600) return "B";
  if (e >= 1400) return "C";
  if (e >= 1200) return "D";
  if (e >= 1000) return "E";

  return "F";
}

function expectedScore(
  ratingA,
  ratingB
) {
  return (
    1 /
    (
      1 +
      Math.pow(
        10,
        (
          ratingB -
          ratingA
        ) / 400
      )
    )
  );
}

function calculateEloDelta(
  ratingA,
  ratingB,
  resultA,
  k = 32
) {
  return Math.round(
    k *
    (
      resultA -
      expectedScore(
        ratingA,
        ratingB
      )
    )
  );
}

function applyMatchElo(
  data,
  match
) {
  /*
    Один матч не может
    распределить ELO дважды
  */

  if (match.eloApplied) {
    return null;
  }

  const playerA =
    data.players.find(
      p =>
        Number(p.id) ===
        Number(
          match.playerAId
        )
    );

  const playerB =
    data.players.find(
      p =>
        Number(p.id) ===
        Number(
          match.playerBId
        )
    );

  if (
    !playerA ||
    !playerB
  ) {
    return null;
  }

  const scoreA =
    Number(
      match.scoreA
    );

  const scoreB =
    Number(
      match.scoreB
    );

  if (
    scoreA === scoreB
  ) {
    return null;
  }

  const oldA =
    Number(
      playerA.elo
    ) || 1200;

  const oldB =
    Number(
      playerB.elo
    ) || 1200;

  const resultA =
    scoreA > scoreB
      ? 1
      : 0;

  const delta =
    calculateEloDelta(
      oldA,
      oldB,
      resultA,
      32
    );

  playerA.elo =
    Math.max(
      0,
      oldA + delta
    );

  playerB.elo =
    Math.max(
      0,
      oldB - delta
    );

  if (resultA === 1) {
    playerA.wins =
      (
        Number(
          playerA.wins
        ) || 0
      ) + 1;

    playerB.losses =
      (
        Number(
          playerB.losses
        ) || 0
      ) + 1;
  } else {
    playerB.wins =
      (
        Number(
          playerB.wins
        ) || 0
      ) + 1;

    playerA.losses =
      (
        Number(
          playerA.losses
        ) || 0
      ) + 1;
  }

  playerA.updatedAt =
    new Date()
      .toISOString();

  playerB.updatedAt =
    new Date()
      .toISOString();

  match.eloApplied =
    true;

  match.eloChange = {
    playerAId:
      playerA.id,

    playerBId:
      playerB.id,

    beforeA:
      oldA,

    beforeB:
      oldB,

    afterA:
      playerA.elo,

    afterB:
      playerB.elo,

    deltaA:
      delta,

    deltaB:
      -delta,

    appliedAt:
      new Date()
        .toISOString()
  };

  return match.eloChange;
}

/* =========================================
   AUTH
========================================= */

function parseCookies(req) {
  const result = {};

  String(
    req.headers.cookie ||
    ""
  )
    .split(";")
    .forEach(part => {
      const index =
        part.indexOf("=");

      if (index > 0) {
        result[
          part
            .slice(
              0,
              index
            )
            .trim()
        ] =
          decodeURIComponent(
            part
              .slice(
                index + 1
              )
              .trim()
          );
      }
    });

  return result;
}

function sign(value) {
  return crypto
    .createHmac(
      "sha256",
      AUTH_SECRET
    )
    .update(value)
    .digest(
      "base64url"
    );
}

function createToken(
  payload
) {
  const encoded =
    Buffer.from(
      JSON.stringify({
        ...payload,
        t: Date.now()
      })
    ).toString(
      "base64url"
    );

  return (
    encoded +
    "." +
    sign(encoded)
  );
}

function verifyToken(
  token
) {
  try {
    const [
      payload,
      signature
    ] =
      String(
        token || ""
      ).split(".");

    if (
      !payload ||
      !signature
    ) {
      return null;
    }

    const expected =
      sign(payload);

    const a =
      Buffer.from(
        signature
      );

    const b =
      Buffer.from(
        expected
      );

    if (
      a.length !==
        b.length ||
      !crypto.timingSafeEqual(
        a,
        b
      )
    ) {
      return null;
    }

    const decoded =
      JSON.parse(
        Buffer.from(
          payload,
          "base64url"
        ).toString(
          "utf8"
        )
      );

    if (
      !Number.isFinite(
        decoded.t
      )
    ) {
      return null;
    }

    if (
      Date.now() -
        decoded.t >
      TOKEN_TTL
    ) {
      return null;
    }

    return decoded;
  } catch {
    return null;
  }
}

function authInfo(req) {
  return verifyToken(
    parseCookies(req)
      .lavender_session ||
    ""
  );
}

function requireAdmin(
  req,
  res,
  next
) {
  const auth =
    authInfo(req);

  if (
    !auth ||
    auth.role !== "admin"
  ) {
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
  const auth =
    authInfo(req);

  if (
    !auth ||
    auth.role !==
      "streamer"
  ) {
    return res
      .status(401)
      .json({
        error:
          "Нужен вход стримера"
      });
  }

  const data =
    readData();

  const streamer =
    data.streamers.find(
      s =>
        Number(s.id) ===
          Number(
            auth.streamerId
          ) &&
        s.active !== false
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
  const auth =
    authInfo(req);

  if (!auth) {
    return res
      .status(401)
      .json({
        error:
          "Нужен вход администратора или стримера"
      });
  }

  if (
    auth.role ===
    "admin"
  ) {
    req.editor = {
      role: "admin",
      id: null,
      name: ADMIN_USER
    };

    return next();
  }

  if (
    auth.role ===
    "streamer"
  ) {
    const data =
      readData();

    const streamer =
      data.streamers.find(
        s =>
          Number(s.id) ===
            Number(
              auth.streamerId
            ) &&
          s.active !== false
      );

    if (!streamer) {
      return res
        .status(401)
        .json({
          error:
            "Стример отключён"
        });
    }

    req.editor = {
      role:
        "streamer",

      id:
        streamer.id,

      name:
        streamer.displayName ||
        streamer.username
    };

    return next();
  }

  return res
    .status(403)
    .json({
      error:
        "Нет доступа"
    });
}

async function hashPassword(
  password
) {
  const salt =
    crypto
      .randomBytes(16)
      .toString("hex");

  const key =
    await scryptAsync(
      String(password),
      salt,
      64
    );

  return (
    salt +
    ":" +
    key.toString("hex")
  );
}

async function verifyPassword(
  password,
  saved
) {
  try {
    const [
      salt,
      keyHex
    ] =
      String(
        saved || ""
      ).split(":");

    if (
      !salt ||
      !keyHex
    ) {
      return false;
    }

    const key =
      await scryptAsync(
        String(password),
        salt,
        64
      );

    const stored =
      Buffer.from(
        keyHex,
        "hex"
      );

    return (
      stored.length ===
        key.length &&
      crypto.timingSafeEqual(
        stored,
        key
      )
    );
  } catch {
    return false;
  }
}

/* =========================================
   PUBLIC DATA
========================================= */

function publicData(
  data = readData()
) {
  const guildMap =
    Object.fromEntries(
      data.guilds.map(
        guild => [
          Number(
            guild.id
          ),
          guild
        ]
      )
    );

  const players =
    data.players
      .map(player => {
        const wins =
          Number(
            player.wins
          ) || 0;

        const losses =
          Number(
            player.losses
          ) || 0;

        const kills =
          Number(
            player.kills
          ) || 0;

        const deaths =
          Number(
            player.deaths
          ) || 0;

        return {
          ...player,

          guild:
            guildMap[
              Number(
                player.guildId
              )
            ] || null,

          rank:
            rankForElo(
              player.elo
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
          Number(
            b.elo
          ) -
          Number(
            a.elo
          )
      );

  const playerMap =
    Object.fromEntries(
      players.map(
        player => [
          Number(
            player.id
          ),
          player
        ]
      )
    );

  const guilds =
    data.guilds
      .map(guild => {
        const roster =
          players.filter(
            player =>
              Number(
                player.guildId
              ) ===
              Number(
                guild.id
              )
          );

        return {
          ...guild,

          rank:
            rankForElo(
              guild.elo
            ),

          memberCount:
            roster.length,

          roster
        };
      })
      .sort(
        (a, b) =>
          Number(
            b.elo
          ) -
          Number(
            a.elo
          )
      );

  const matches =
    data.matches
      .map(match => ({
        ...match,

        guildA:
          guildMap[
            Number(
              match.guildAId
            )
          ] || null,

        guildB:
          guildMap[
            Number(
              match.guildBId
            )
          ] || null,

        playerA:
          playerMap[
            Number(
              match.playerAId
            )
          ] || null,

        playerB:
          playerMap[
            Number(
              match.playerBId
            )
          ] || null
      }))
      .sort(
        (a, b) =>
          Number(b.id) -
          Number(a.id)
      );

  const streamers =
    data.streamers
      .filter(
        streamer =>
          streamer.active !==
          false
      )
      .map(
        ({
          passwordHash,
          ...streamer
        }) => ({
          ...streamer,

          stream:
            data.streamerStreams.find(
              stream =>
                Number(
                  stream.streamerId
                ) ===
                Number(
                  streamer.id
                )
            ) || null
        })
      );

  return {
    ...data,
    players,
    guilds,
    matches,
    streamers,
    streamerStreams:
      undefined
  };
}

/* =========================================
   OVERLAY
========================================= */

function overlayState() {
  const data =
    publicData();

  const match =
    data.matches.find(
      match =>
        Number(
          match.id
        ) ===
        Number(
          data.overlay
            .activeMatchId
        )
    ) ||
    data.matches[0] ||
    null;

  return {
    overlay:
      data.overlay,

    match,

    settings:
      data.settings,

    version:
      APP_VERSION
  };
}

function broadcast() {
  io.emit(
    "overlay:update",
    overlayState()
  );

  io.emit(
    "site:update",
    {
      at: Date.now()
    }
  );
}

/* =========================================
   HEALTH
========================================= */

app.get(
  "/health",
  (req, res) => {
    res.json({
      ok: true,
      version:
        APP_VERSION,
      uptime:
        Math.round(
          process.uptime()
        )
    });
  }
);

app.get(
  "/db-health",
  async (
    req,
    res
  ) => {
    try {
      const result =
        await DB.query(`
          SELECT updated_at
          FROM lavender_state
          WHERE id = 1
        `);

      res.json({
        ok: true,
        database:
          "postgresql",
        version:
          APP_VERSION,
        updatedAt:
          result.rows[0]
            ?.updated_at ||
          null
      });
    } catch (error) {
      res
        .status(500)
        .json({
          ok: false,
          error:
            error.message
        });
    }
  }
);

/* =========================================
   AUTH API
========================================= */

app.get(
  "/api/auth/status",
  (req, res) => {
    const auth =
      authInfo(req);

    if (!auth) {
      return res.json({
        authenticated:
          false,
        role: null
      });
    }

    if (
      auth.role ===
      "admin"
    ) {
      return res.json({
        authenticated:
          true,
        role: "admin",
        user:
          ADMIN_USER
      });
    }

    const data =
      readData();

    const streamer =
      data.streamers.find(
        s =>
          Number(s.id) ===
          Number(
            auth.streamerId
          )
      );

    if (!streamer) {
      return res.json({
        authenticated:
          false,
        role: null
      });
    }

    res.json({
      authenticated:
        true,

      role:
        "streamer",

      streamer: {
        id:
          streamer.id,
        username:
          streamer.username,
        displayName:
          streamer.displayName,
        avatar:
          streamer.avatar
      }
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
        createToken({
          role: "admin"
        });

      res.setHeader(
        "Set-Cookie",
        `lavender_session=${token}; HttpOnly; SameSite=Lax; Path=/; Max-Age=43200${
          COOKIE_SECURE
            ? "; Secure"
            : ""
        }`
      );

      return res.json({
        ok: true,
        role:
          "admin"
      });
    }

    const data =
      readData();

    const streamer =
      data.streamers.find(
        s =>
          s.active !== false &&
          String(
            s.username
          ).toLowerCase() ===
          username.toLowerCase()
      );

    if (
      !streamer ||
      !await verifyPassword(
        password,
        streamer.passwordHash
      )
    ) {
      return res
        .status(401)
        .json({
          error:
            "Неверный логин или пароль"
        });
    }

    const token =
      createToken({
        role:
          "streamer",
        streamerId:
          streamer.id
      });

    res.setHeader(
      "Set-Cookie",
      `lavender_session=${token}; HttpOnly; SameSite=Lax; Path=/; Max-Age=43200${
        COOKIE_SECURE
          ? "; Secure"
          : ""
      }`
    );

    res.json({
      ok: true,
      role:
        "streamer",
      streamerId:
        streamer.id
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

    res.json({
      ok: true
    });
  }
);

app.get(
  "/api/admin/ping",
  requireAdmin,
  (req, res) => {
    res.json({
      ok: true,
      version:
        APP_VERSION
    });
  }
);

app.get(
  "/api/streamer/ping",
  requireStreamer,
  (req, res) => {
    res.json({
      ok: true,
      streamerId:
        req.streamer.id
    });
  }
);

/* =========================================
   MAIN DATA API
========================================= */

app.get(
  "/api/all",
  (req, res) => {
    res.json(
      publicData()
    );
  }
);

app.get(
  "/api/overlay",
  (req, res) => {
    res.json(
      overlayState()
    );
  }
);

/* =========================================
   PLAYERS
========================================= */

app.post(
  "/api/players",
  requireEditor,
  (req, res) => {
    const data =
      readData();

    const body =
      req.body || {};

    const nickname =
      cleanText(
        body.nickname,
        50
      );

    if (!nickname) {
      return res
        .status(400)
        .json({
          error:
            "Укажи ник игрока"
        });
    }

    const player = {
      id:
        nextId(
          data.players
        ),

      nickname,

      gameId:
        cleanText(
          body.gameId,
          40
        ),

      avatar:
        cleanImage(
          body.avatar,
          "👤"
        ),

      guildId:
        body.guildId
          ? Number(
              body.guildId
            )
          : null,

      elo:
        Number(
          body.elo
        ) || 1200,

      wins:
        Number(
          body.wins
        ) || 0,

      losses:
        Number(
          body.losses
        ) || 0,

      kills:
        Number(
          body.kills
        ) || 0,

      deaths:
        Number(
          body.deaths
        ) || 0,

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

    data.players.push(
      player
    );

    atomicWrite(data);

    broadcast();

    res.json(player);
  }
);

app.patch(
  "/api/players/:id",
  requireEditor,
  (req, res) => {
    const data =
      readData();

    const player =
      data.players.find(
        p =>
          Number(p.id) ===
          Number(
            req.params.id
          )
      );

    if (!player) {
      return res
        .status(404)
        .json({
          error:
            "Игрок не найден"
        });
    }

    const body =
      req.body || {};

    if (
      "nickname" in body
    ) {
      player.nickname =
        cleanText(
          body.nickname,
          50
        );
    }

    if (
      "gameId" in body
    ) {
      player.gameId =
        cleanText(
          body.gameId,
          40
        );
    }

    if (
      "avatar" in body
    ) {
      player.avatar =
        cleanImage(
          body.avatar,
          player.avatar ||
          "👤"
        );
    }

    if (
      "guildId" in body
    ) {
      player.guildId =
        body.guildId
          ? Number(
              body.guildId
            )
          : null;
    }

    for (const key of [
      "elo",
      "wins",
      "losses",
      "kills",
      "deaths"
    ]) {
      if (key in body) {
        player[key] =
          Number(
            body[key]
          ) || 0;
      }
    }

    player.updatedAt =
      new Date()
        .toISOString();

    atomicWrite(data);

    broadcast();

    res.json(player);
  }
);

app.delete(
  "/api/players/:id",
  requireEditor,
  (req, res) => {
    const data =
      readData();

    const id =
      Number(
        req.params.id
      );

    data.players =
      data.players.filter(
        p =>
          Number(p.id) !==
          id
      );

    data.matches.forEach(
      match => {
        if (
          Number(
            match.playerAId
          ) === id
        ) {
          match.playerAId =
            null;
        }

        if (
          Number(
            match.playerBId
          ) === id
        ) {
          match.playerBId =
            null;
        }
      }
    );

    atomicWrite(data);

    broadcast();

    res.json({
      ok: true
    });
  }
);

/* =========================================
   GUILDS
========================================= */

app.post(
  "/api/guilds",
  requireEditor,
  (req, res) => {
    const data =
      readData();

    const body =
      req.body || {};

    const name =
      cleanText(
        body.name,
        50
      );

    const tag =
      cleanText(
        body.tag,
        12
      ).toUpperCase();

    if (
      !name ||
      !tag
    ) {
      return res
        .status(400)
        .json({
          error:
            "Укажи название и тег"
        });
    }

    const guild = {
      id:
        nextId(
          data.guilds
        ),

      name,
      tag,

      logo:
        cleanImage(
          body.logo,
          "🪻"
        ),

      region:
        cleanText(
          body.region ||
          "Кыргызстан",
          40
        ),

      elo:
        Number(
          body.elo
        ) || 1200,

      wins:
        Number(
          body.wins
        ) || 0,

      losses:
        Number(
          body.losses
        ) || 0,

      captain:
        cleanText(
          body.captain,
          50
        ),

      description:
        cleanText(
          body.description,
          400
        ),

      createdByRole:
        req.editor.role,

      createdByName:
        req.editor.name,

      createdAt:
        new Date()
          .toISOString()
    };

    data.guilds.push(
      guild
    );

    atomicWrite(data);

    broadcast();

    res.json(guild);
  }
);

app.patch(
  "/api/guilds/:id",
  requireEditor,
  (req, res) => {
    const data =
      readData();

    const guild =
      data.guilds.find(
        g =>
          Number(g.id) ===
          Number(
            req.params.id
          )
      );

    if (!guild) {
      return res
        .status(404)
        .json({
          error:
            "Гильдия не найдена"
        });
    }

    const body =
      req.body || {};

    if ("name" in body)
      guild.name =
        cleanText(
          body.name,
          50
        );

    if ("tag" in body)
      guild.tag =
        cleanText(
          body.tag,
          12
        ).toUpperCase();

    if ("logo" in body)
      guild.logo =
        cleanImage(
          body.logo,
          guild.logo ||
          "🪻"
        );

    if ("region" in body)
      guild.region =
        cleanText(
          body.region,
          40
        );

    if ("elo" in body)
      guild.elo =
        Number(
          body.elo
        ) || 0;

    if ("wins" in body)
      guild.wins =
        Number(
          body.wins
        ) || 0;

    if ("losses" in body)
      guild.losses =
        Number(
          body.losses
        ) || 0;

    guild.updatedAt =
      new Date()
        .toISOString();

    atomicWrite(data);

    broadcast();

    res.json(guild);
  }
);

app.delete(
  "/api/guilds/:id",
  requireEditor,
  (req, res) => {
    const data =
      readData();

    const id =
      Number(
        req.params.id
      );

    data.guilds =
      data.guilds.filter(
        guild =>
          Number(
            guild.id
          ) !== id
      );

    data.players.forEach(
      player => {
        if (
          Number(
            player.guildId
          ) === id
        ) {
          player.guildId =
            null;
        }
      }
    );

    atomicWrite(data);

    broadcast();

    res.json({
      ok: true
    });
  }
);

/* =========================================
   MATCHES
========================================= */

app.post(
  "/api/matches",
  requireEditor,
  (req, res) => {
    const data =
      readData();

    const body =
      req.body || {};

    const match = {
      id:
        nextId(
          data.matches
        ),

      title:
        cleanText(
          body.title ||
          "LIVE MATCH",
          80
        ),

      tournament:
        cleanText(
          body.tournament ||
          "LAVENDER CUP",
          80
        ),

      guildAId:
        body.guildAId
          ? Number(
              body.guildAId
            )
          : null,

      guildBId:
        body.guildBId
          ? Number(
              body.guildBId
            )
          : null,

      playerAId:
        body.playerAId
          ? Number(
              body.playerAId
            )
          : null,

      playerBId:
        body.playerBId
          ? Number(
              body.playerBId
            )
          : null,

      scoreA:
        Number(
          body.scoreA
        ) || 0,

      scoreB:
        Number(
          body.scoreB
        ) || 0,

      roundText:
        cleanText(
          body.roundText ||
          "ROUND 1",
          30
        ),

      format:
        cleanText(
          body.format ||
          "BO7",
          20
        ),

      status:
        cleanText(
          body.status ||
          "SCHEDULED",
          20
        ).toUpperCase(),

      createdAt:
        new Date()
          .toISOString()
    };

    data.matches.push(
      match
    );

    data.overlay
      .activeMatchId =
      match.id;

    atomicWrite(data);

    broadcast();

    res.json(match);
  }
);

app.patch(
  "/api/matches/:id",
  requireEditor,
  (req, res) => {
    const data =
      readData();

    const match =
      data.matches.find(
        m =>
          Number(m.id) ===
          Number(
            req.params.id
          )
      );

    if (!match) {
      return res
        .status(404)
        .json({
          error:
            "Матч не найден"
        });
    }

    const body =
      req.body || {};

    for (const key of [
      "title",
      "tournament",
      "roundText",
      "format",
      "status"
    ]) {
      if (key in body) {
        match[key] =
          cleanText(
            body[key],
            80
          );
      }
    }

    for (const key of [
      "guildAId",
      "guildBId",
      "playerAId",
      "playerBId"
    ]) {
      if (key in body) {
        match[key] =
          body[key]
            ? Number(
                body[key]
              )
            : null;
      }
    }

    if (
      "scoreA" in body
    ) {
      match.scoreA =
        Math.max(
          0,
          Number(
            body.scoreA
          ) || 0
        );
    }

    if (
      "scoreB" in body
    ) {
      match.scoreB =
        Math.max(
          0,
          Number(
            body.scoreB
          ) || 0
        );
    }

    match.status =
      String(
        match.status ||
        "LIVE"
      ).toUpperCase();

    /*
      Если поставили FINAL,
      ELO распределяется
      автоматически
    */

    if (
      match.status ===
      "FINAL"
    ) {
      applyMatchElo(
        data,
        match
      );
    }

    atomicWrite(data);

    broadcast();

    res.json(match);
  }
);

/* =========================================
   ВЫБРАТЬ ПОБЕДИТЕЛЯ -> ELO
========================================= */

app.post(
  "/api/matches/:id/set-winner",
  requireEditor,
  (req, res) => {
    const data =
      readData();

    const match =
      data.matches.find(
        m =>
          Number(m.id) ===
          Number(
            req.params.id
          )
      );

    if (!match) {
      return res
        .status(404)
        .json({
          error:
            "Матч не найден"
        });
    }

    if (
      match.eloApplied
    ) {
      return res
        .status(400)
        .json({
          error:
            "ELO этого матча уже рассчитано"
        });
    }

    const playerAId =
      Number(
        req.body
          ?.playerAId ||
        match.playerAId
      );

    const playerBId =
      Number(
        req.body
          ?.playerBId ||
        match.playerBId
      );

    const winnerId =
      Number(
        req.body
          ?.winnerId
      );

    if (
      !playerAId ||
      !playerBId
    ) {
      return res
        .status(400)
        .json({
          error:
            "Выбери двух игроков"
        });
    }

    if (
      playerAId ===
      playerBId
    ) {
      return res
        .status(400)
        .json({
          error:
            "Нельзя выбрать одного игрока дважды"
        });
    }

    if (
      winnerId !==
        playerAId &&
      winnerId !==
        playerBId
    ) {
      return res
        .status(400)
        .json({
          error:
            "Выбери победителя"
        });
    }

    match.playerAId =
      playerAId;

    match.playerBId =
      playerBId;

    /*
      Нам достаточно результата 1:0,
      чтобы определить победителя.
    */

    match.scoreA =
      winnerId ===
        playerAId
        ? 1
        : 0;

    match.scoreB =
      winnerId ===
        playerBId
        ? 1
        : 0;

    match.status =
      "FINAL";

    const eloChange =
      applyMatchElo(
        data,
        match
      );

    atomicWrite(data);

    broadcast();

    const playerA =
      data.players.find(
        player =>
          Number(
            player.id
          ) ===
          playerAId
      );

    const playerB =
      data.players.find(
        player =>
          Number(
            player.id
          ) ===
          playerBId
      );

    res.json({
      ok: true,

      match,

      winnerId,

      eloChange,

      players: {
        A: {
          id:
            playerA.id,
          nickname:
            playerA.nickname,
          elo:
            playerA.elo,
          rank:
            rankForElo(
              playerA.elo
            )
        },

        B: {
          id:
            playerB.id,
          nickname:
            playerB.nickname,
          elo:
            playerB.elo,
          rank:
            rankForElo(
              playerB.elo
            )
        }
      }
    });
  }
);

app.delete(
  "/api/matches/:id",
  requireEditor,
  (req, res) => {
    const data =
      readData();

    const id =
      Number(
        req.params.id
      );

    data.matches =
      data.matches.filter(
        match =>
          Number(
            match.id
          ) !== id
      );

    atomicWrite(data);

    broadcast();

    res.json({
      ok: true
    });
  }
);

/* =========================================
   OVERLAY UPDATE
========================================= */

app.patch(
  "/api/overlay",
  requireAdmin,
  (req, res) => {
    const data =
      readData();

    const body =
      req.body || {};

    if (
      "activeMatchId" in
      body
    ) {
      data.overlay
        .activeMatchId =
        body.activeMatchId
          ? Number(
              body.activeMatchId
            )
          : null;
    }

    if (
      "visible" in body
    ) {
      data.overlay.visible =
        !!body.visible;
    }

    if (
      "customText" in body
    ) {
      data.overlay.customText =
        cleanText(
          body.customText,
          80
        );
    }

    if (
      "accent" in body &&
      /^#[0-9a-f]{6}$/i.test(
        body.accent
      )
    ) {
      data.overlay.accent =
        body.accent;
    }

    atomicWrite(data);

    broadcast();

    res.json(
      overlayState()
    );
  }
);

/* =========================================
   BACKUP
========================================= */

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
    const data =
      normalize(
        req.body
      );

    atomicWrite(data);

    broadcast();

    res.json({
      ok: true
    });
  }
);

/* =========================================
   SOCKET.IO
========================================= */

io.on(
  "connection",
  socket => {
    socket.emit(
      "overlay:update",
      overlayState()
    );
  }
);

/* =========================================
   PAGES
========================================= */

app.get(
  "/",
  (req, res) => {
    res.sendFile(
      path.join(
        ROOT,
        "index.html"
      )
    );
  }
);

app.get(
  "/overlay.html",
  (req, res) => {
    res.sendFile(
      path.join(
        ROOT,
        "overlay.html"
      )
    );
  }
);

app.get(
  "*",
  (req, res) => {
    res.sendFile(
      path.join(
        ROOT,
        "index.html"
      )
    );
  }
);

/* =========================================
   START
========================================= */

async function start() {
  try {
    await initDatabase();

    server.listen(
      PORT,
      "0.0.0.0",
      () => {
        console.log(
          `LAVENDER ${APP_VERSION} running on ${PORT}`
        );
      }
    );
  } catch (error) {
    console.error(
      "DATABASE START ERROR:",
      error
    );

    process.exit(1);
  }
}

process.on(
  "SIGTERM",
  async () => {
    try {
      await WRITE_CHAIN;

      if (DB) {
        await DB.end();
      }
    } catch {}

    process.exit(0);
  }
);

start();
