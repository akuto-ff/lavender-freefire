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

/* =========================================================
   CONFIG
========================================================= */

const APP_VERSION = "8.1.0-postgres-stable";

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

/* =========================================================
   APP
========================================================= */

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
    maxAge: 0
  })
);

/* =========================================================
   POSTGRES
========================================================= */

let DB = null;
let DATA_CACHE = null;
let WRITE_CHAIN = Promise.resolve();

function baseData() {
  return {
    version: 1,

    settings: {
      siteName: "LAVENDER",
      tagline: "FREE FIRE COMMUNITY",
      accent: "#b46cff"
    },

    players: [],
    guilds: [],
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
      customText: "LAVENDER • LIVE",
      textColor: "#ffffff",
      panelColor: "#17101e",
      glowColor: "#b46cff",
      backgroundColor: "#08060b",
      leftAvatar: "",
      rightAvatar: "",
      showAvatars: true,
      avatarShape: "rounded",
      theme: "neon"
    }
  };
}

function normalizeData(data) {
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

  const arrays = [
    "players",
    "guilds",
    "matches",
    "tournaments",
    "news",
    "streamers",
    "streamerStreams"
  ];

  for (const name of arrays) {
    if (!Array.isArray(data[name])) {
      data[name] = [];
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
      normalizeData(data)
    )
  );
}

function readSeedFile() {
  try {
    if (!fs.existsSync(DATA_FILE)) {
      return baseData();
    }

    return normalizeData(
      JSON.parse(
        fs.readFileSync(
          DATA_FILE,
          "utf8"
        )
      )
    );
  } catch (error) {
    console.error(
      "Could not read data.json:",
      error.message
    );

    return baseData();
  }
}

function readData() {
  return cloneData(
    DATA_CACHE ||
    baseData()
  );
}

async function atomicWrite(data) {
  const next =
    cloneData(data);

  DATA_CACHE = next;

  WRITE_CHAIN =
    WRITE_CHAIN.then(async () => {
      await DB.query(
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
      );
    });

  try {
    await WRITE_CHAIN;
  } catch (error) {
    console.error(
      "POSTGRES WRITE ERROR:",
      error
    );

    throw error;
  }
}

async function initDatabase() {
  if (!DATABASE_URL) {
    throw new Error(
      "DATABASE_URL отсутствует в Environment"
    );
  }

  DB = new Pool({
    connectionString: DATABASE_URL,

    ssl:
      DATABASE_URL.includes("localhost")
        ? false
        : {
            rejectUnauthorized: false
          },

    max: 5,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 10000
  });

  await DB.query("SELECT 1");

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
      normalizeData(
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

/* =========================================================
   HELPERS
========================================================= */

function nextId(items) {
  if (!Array.isArray(items) || !items.length) {
    return 1;
  }

  return (
    Math.max(
      ...items.map(
        item =>
          Number(item.id) || 0
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
  fallback = "👤"
) {
  const input =
    String(
      value || ""
    ).trim();

  if (!input) {
    return fallback;
  }

  if (
    input.startsWith(
      "data:image/"
    )
  ) {
    return input.length <= 3000000
      ? input
      : fallback;
  }

  return input.slice(
    0,
    3000000
  );
}

function rankForElo(elo) {
  const value =
    Number(elo) || 0;

  if (value >= 2000) return "S";
  if (value >= 1800) return "A";
  if (value >= 1600) return "B";
  if (value >= 1400) return "C";
  if (value >= 1200) return "D";
  if (value >= 1000) return "E";

  return "F";
}

/* =========================================================
   ELO
========================================================= */

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
    Один матч нельзя
    начислить дважды
  */

  if (match.eloApplied) {
    return null;
  }

  const playerA =
    data.players.find(
      player =>
        Number(player.id) ===
        Number(
          match.playerAId
        )
    );

  const playerB =
    data.players.find(
      player =>
        Number(player.id) ===
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

  if (
    Number(playerA.id) ===
    Number(playerB.id)
  ) {
    return null;
  }

  const scoreA =
    Number(
      match.scoreA
    ) || 0;

  const scoreB =
    Number(
      match.scoreB
    ) || 0;

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
        Number(playerA.wins) ||
        0
      ) + 1;

    playerB.losses =
      (
        Number(playerB.losses) ||
        0
      ) + 1;
  } else {
    playerB.wins =
      (
        Number(playerB.wins) ||
        0
      ) + 1;

    playerA.losses =
      (
        Number(playerA.losses) ||
        0
      ) + 1;
  }

  const now =
    new Date().toISOString();

  playerA.updatedAt = now;
  playerB.updatedAt = now;

  match.eloApplied = true;

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
      now
  };

  return match.eloChange;
}


/* =========================================================
   TEAM SERIES 6v6 / 8v8 / 10v10 / 12v12
========================================================= */

function seriesSizeFromValue(value) {
  const m = String(value ?? "").match(/(6|8|10|12)/);
  const n = m ? Number(m[1]) : Number(value);
  return [6, 8, 10, 12].includes(n) ? n : 6;
}

function uniqueNumberIds(list) {
  return [...new Set((Array.isArray(list) ? list : []).map(Number).filter(Boolean))];
}

function validateGuildLineup(data, guildId, ids, size) {
  const lineup = uniqueNumberIds(ids);
  if (lineup.length !== size) {
    return { ok: false, error: `Нужно выбрать ровно ${size} игроков` };
  }

  const players = lineup.map(id => data.players.find(p => Number(p.id) === id));
  if (players.some(p => !p)) {
    return { ok: false, error: "Один из выбранных игроков не найден" };
  }

  if (players.some(p => Number(p.guildId) !== Number(guildId))) {
    return { ok: false, error: "Все выбранные игроки должны состоять в своей гильдии" };
  }

  return { ok: true, lineup, players };
}

function applySeriesGameElo(data, playerAId, playerBId, winnerId) {
  const playerA = data.players.find(p => Number(p.id) === Number(playerAId));
  const playerB = data.players.find(p => Number(p.id) === Number(playerBId));

  if (!playerA || !playerB) {
    throw new Error("Игрок для ELO не найден");
  }

  if (Number(playerA.id) === Number(playerB.id)) {
    throw new Error("Один игрок не может играть сам против себя");
  }

  const winner = Number(winnerId);
  if (![Number(playerA.id), Number(playerB.id)].includes(winner)) {
    throw new Error("Победитель не участвует в текущей игре");
  }

  const oldA = Number(playerA.elo) || 1200;
  const oldB = Number(playerB.elo) || 1200;
  const resultA = winner === Number(playerA.id) ? 1 : 0;
  const deltaA = calculateEloDelta(oldA, oldB, resultA, 32);
  const deltaB = -deltaA;

  playerA.elo = Math.max(0, oldA + deltaA);
  playerB.elo = Math.max(0, oldB + deltaB);

  if (resultA === 1) {
    playerA.wins = (Number(playerA.wins) || 0) + 1;
    playerB.losses = (Number(playerB.losses) || 0) + 1;
  } else {
    playerB.wins = (Number(playerB.wins) || 0) + 1;
    playerA.losses = (Number(playerA.losses) || 0) + 1;
  }

  const now = new Date().toISOString();
  playerA.updatedAt = now;
  playerB.updatedAt = now;

  return {
    playerAId: playerA.id,
    playerBId: playerB.id,
    winnerId: winner,
    beforeA: oldA,
    beforeB: oldB,
    afterA: playerA.elo,
    afterB: playerB.elo,
    deltaA,
    deltaB,
    appliedAt: now
  };
}

function syncSeriesCurrentPlayers(match) {
  const size = seriesSizeFromValue(match.seriesSize || match.format);
  const lineupA = uniqueNumberIds(match.lineupAIds);
  const lineupB = uniqueNumberIds(match.lineupBIds);

  match.seriesSize = size;
  match.targetWins = Number(match.targetWins) || size;
  match.seriesHistory = Array.isArray(match.seriesHistory) ? match.seriesHistory : [];
  match.currentStage = Math.max(0, Number(match.currentStage) || 0);

  if (lineupA.length && lineupB.length) {
    const idx = match.currentStage % Math.min(lineupA.length, lineupB.length);
    match.playerAId = lineupA[idx] || null;
    match.playerBId = lineupB[idx] || null;
    match.roundText = `GAME ${match.seriesHistory.length + 1} • SLOT ${idx + 1}/${size}`;
  }

  return match;
}

/* =========================================================
   PASSWORD
========================================================= */

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
  stored
) {
  try {
    const [
      salt,
      keyHex
    ] =
      String(
        stored || ""
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

    const saved =
      Buffer.from(
        keyHex,
        "hex"
      );

    return (
      saved.length ===
        key.length &&
      crypto.timingSafeEqual(
        saved,
        key
      )
    );
  } catch {
    return false;
  }
}

/* =========================================================
   COOKIE AUTH
========================================================= */

function parseCookies(req) {
  const out = {};

  String(
    req.headers.cookie ||
    ""
  )
    .split(";")
    .forEach(part => {
      const index =
        part.indexOf("=");

      if (index <= 0) {
        return;
      }

      const key =
        part
          .slice(0, index)
          .trim();

      const value =
        part
          .slice(index + 1)
          .trim();

      out[key] =
        decodeURIComponent(
          value
        );
    });

  return out;
}

function signTokenPayload(
  value
) {
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

function makeToken(
  payload
) {
  const data =
    Buffer.from(
      JSON.stringify({
        ...payload,
        time: Date.now()
      }),
      "utf8"
    ).toString(
      "base64url"
    );

  return (
    data +
    "." +
    signTokenPayload(data)
  );
}

function verifyToken(
  token
) {
  try {
    const parts =
      String(
        token || ""
      ).split(".");

    if (
      parts.length !== 2
    ) {
      return null;
    }

    const [
      payload,
      signature
    ] = parts;

    const expected =
      signTokenPayload(
        payload
      );

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
      b.length
    ) {
      return null;
    }

    if (
      !crypto.timingSafeEqual(
        a,
        b
      )
    ) {
      return null;
    }

    const result =
      JSON.parse(
        Buffer.from(
          payload,
          "base64url"
        ).toString("utf8")
      );

    if (
      !Number.isFinite(
        result.time
      )
    ) {
      return null;
    }

    if (
      Date.now() -
        result.time >
      TOKEN_TTL
    ) {
      return null;
    }

    return result;
  } catch {
    return null;
  }
}

function authInfo(req) {
  const cookies =
    parseCookies(req);

  return verifyToken(
    cookies.lavender_session ||
    ""
  );
}

function setLoginCookie(
  res,
  token
) {
  res.setHeader(
    "Set-Cookie",
    `lavender_session=${token}; HttpOnly; SameSite=Lax; Path=/; Max-Age=43200${
      COOKIE_SECURE
        ? "; Secure"
        : ""
    }`
  );
}

function clearLoginCookie(
  res
) {
  res.setHeader(
    "Set-Cookie",
    `lavender_session=; HttpOnly; SameSite=Lax; Path=/; Max-Age=0${
      COOKIE_SECURE
        ? "; Secure"
        : ""
    }`
  );
}

/* =========================================================
   PERMISSIONS
========================================================= */

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

  req.admin =
    auth;

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
      item =>
        Number(item.id) ===
          Number(
            auth.streamerId
          ) &&
        item.active !== false
    );

  if (!streamer) {
    return res
      .status(401)
      .json({
        error:
          "Аккаунт стримера отключён или удалён"
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
      name:
        ADMIN_USER
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
        item =>
          Number(item.id) ===
            Number(
              auth.streamerId
            ) &&
          item.active !== false
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
        "Недостаточно прав"
    });
}

/* =========================================================
   PUBLIC DATA
========================================================= */

function publicData(
  source = readData()
) {
  const data =
    normalizeData(
      cloneData(source)
    );

  const guildMap =
    Object.fromEntries(
      data.guilds.map(
        guild => [
          Number(guild.id),
          guild
        ]
      )
    );

  const players =
    data.players
      .map(player => {
        const wins =
          Number(player.wins) ||
          0;

        const losses =
          Number(player.losses) ||
          0;

        const kills =
          Number(player.kills) ||
          0;

        const deaths =
          Number(player.deaths) ||
          0;

        const headshots =
          Number(player.headshots) ||
          0;

        return {
          ...player,

          headshots,

          headshotRate:
            kills > 0
              ? Math.round(
                  headshots /
                  kills *
                  100
                )
              : 0,

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
            deaths > 0
              ? Number(
                  (
                    kills /
                    deaths
                  ).toFixed(2)
                )
              : kills,

          winrate:
            wins + losses > 0
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
          Number(b.elo || 0) -
          Number(a.elo || 0)
      );

  const playerMap =
    Object.fromEntries(
      players.map(
        player => [
          Number(player.id),
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

        const wins =
          Number(guild.wins) ||
          0;

        const losses =
          Number(guild.losses) ||
          0;

        return {
          ...guild,

          rank:
            rankForElo(
              guild.elo
            ),

          roster,

          memberCount:
            roster.length,

          winrate:
            wins + losses > 0
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
          Number(b.elo || 0) -
          Number(a.elo || 0)
      );

  const matches =
    data.matches
      .map(match => {
        const lineupAIds = uniqueNumberIds(match.lineupAIds);
        const lineupBIds = uniqueNumberIds(match.lineupBIds);
        const seriesSize = [6, 8, 10, 12].includes(Number(match.seriesSize))
          ? Number(match.seriesSize)
          : null;
        const currentStage = Math.max(0, Number(match.currentStage) || 0);
        const currentIndex = seriesSize ? currentStage % seriesSize : 0;

        return {
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
            ] || null,

          lineupAIds,
          lineupBIds,

          lineupA:
            lineupAIds
              .map(id => playerMap[Number(id)])
              .filter(Boolean),

          lineupB:
            lineupBIds
              .map(id => playerMap[Number(id)])
              .filter(Boolean),

          seriesSize,
          targetWins:
            Number(match.targetWins) ||
            seriesSize ||
            null,

          seriesHistory:
            Array.isArray(match.seriesHistory)
              ? match.seriesHistory
              : [],

          currentStage,
          currentIndex
        };
      })
      .sort(
        (a, b) =>
          Number(b.id) -
          Number(a.id)
      );

  const tournaments =
    data.tournaments
      .map(tournament => ({
        ...tournament,

        guildIds:
          Array.isArray(
            tournament.guildIds
          )
            ? tournament.guildIds
            : [],

        participants:
          (
            Array.isArray(
              tournament.guildIds
            )
              ? tournament.guildIds
              : []
          )
            .map(
              id =>
                guildMap[
                  Number(id)
                ]
            )
            .filter(Boolean)
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
          streamer.active !== false
      )
      .map(streamer => {
        const {
          passwordHash,
          ...safe
        } = streamer;

        return {
          ...safe,

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
        };
      });

  return {
    ...data,

    players,
    guilds,
    matches,
    tournaments,
    streamers,

    streamerStreams:
      undefined
  };
}

/* =========================================================
   OVERLAY
========================================================= */

function globalOverlayState() {
  const data =
    publicData();

  const match =
    data.matches.find(
      item =>
        Number(item.id) ===
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

function streamerOverlayState(
  streamerId
) {
  const data =
    publicData();

  const streamer =
    data.streamers.find(
      item =>
        Number(item.id) ===
        Number(streamerId)
    );

  if (!streamer) {
    return null;
  }

  const stream =
    streamer.stream || {};

  const match =
    data.matches.find(
      item =>
        Number(item.id) ===
        Number(
          stream.matchId
        )
    ) ||
    data.matches[0] ||
    null;

  const resultMatch =
    match
      ? { ...match }
      : null;

  if (resultMatch) {
    const playerA =
      data.players.find(
        player =>
          Number(player.id) ===
          Number(
            stream.playerAId
          )
      );

    const playerB =
      data.players.find(
        player =>
          Number(player.id) ===
          Number(
            stream.playerBId
          )
      );

    if (playerA) {
      resultMatch.playerA =
        playerA;

      resultMatch.playerAId =
        playerA.id;
    }

    if (playerB) {
      resultMatch.playerB =
        playerB;

      resultMatch.playerBId =
        playerB.id;
    }
  }

  return {
    streamer,

    overlay: {
      visible:
        stream.status ===
        "LIVE",

      accent:
        stream.accent ||
        data.settings.accent ||
        "#b46cff",

      position:
        stream.position ||
        "bottom",

      showPlayers:
        stream.showPlayers !==
        false,

      showStats:
        stream.showStats !==
        false,

      customText:
        stream.customText ||
        `${
          streamer.displayName ||
          streamer.username
        } • LIVE`,

      textColor:
        stream.textColor ||
        "#ffffff",

      panelColor:
        stream.panelColor ||
        "#17101e",

      glowColor:
        stream.glowColor ||
        stream.accent ||
        "#b46cff",

      backgroundColor:
        stream.backgroundColor ||
        "#08060b",

      leftAvatar:
        stream.leftAvatar ||
        "",

      rightAvatar:
        stream.rightAvatar ||
        "",

      showAvatars:
        stream.showAvatars !==
        false,

      avatarShape:
        ["circle","square","rounded"].includes(stream.avatarShape)
          ? stream.avatarShape
          : "rounded",

      theme:
        ["neon","glass","minimal"].includes(stream.theme)
          ? stream.theme
          : "neon"
    },

    match:
      resultMatch,

    settings:
      data.settings,

    version:
      APP_VERSION
  };
}

/* =========================================================
   SOCKET BROADCAST
========================================================= */

function broadcast() {
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

function broadcastStreamer(
  streamerId
) {
  io
    .to(
      `streamer:${streamerId}`
    )
    .emit(
      "streamer-overlay:update",
      streamerOverlayState(
        streamerId
      )
    );

  io.emit(
    "site:update",
    {
      at: Date.now()
    }
  );
}

/* =========================================================
   HEALTH
========================================================= */

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
        await DB.query(
          `
          SELECT updated_at
          FROM lavender_state
          WHERE id = 1
          `
        );

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

/* =========================================================
   AUTH
========================================================= */

app.get(
  "/api/auth/status",
  (req, res) => {
    const auth =
      authInfo(req);

    if (!auth) {
      return res.json({
        authenticated:
          false,

        role: null,

        version:
          APP_VERSION
      });
    }

    if (
      auth.role ===
      "admin"
    ) {
      return res.json({
        authenticated:
          true,

        role:
          "admin",

        user:
          ADMIN_USER,

        version:
          APP_VERSION
      });
    }

    if (
      auth.role ===
      "streamer"
    ) {
      const data =
        readData();

      const streamer =
        data.streamers.find(
          item =>
            Number(item.id) ===
              Number(
                auth.streamerId
              ) &&
            item.active !== false
        );

      if (!streamer) {
        return res.json({
          authenticated:
            false,

          role: null,

          version:
            APP_VERSION
        });
      }

      return res.json({
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
        },

        version:
          APP_VERSION
      });
    }

    res.json({
      authenticated:
        false,
      role: null,
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
        req.body?.username ||
        ""
      ).trim();

    const password =
      String(
        req.body?.password ||
        ""
      );

    /*
      ADMIN
    */

    if (
      username ===
        ADMIN_USER &&
      password ===
        ADMIN_PASSWORD
    ) {
      const token =
        makeToken({
          role:
            "admin",

          username:
            ADMIN_USER
        });

      setLoginCookie(
        res,
        token
      );

      return res.json({
        ok: true,
        role:
          "admin"
      });
    }

    /*
      STREAMER
    */

    const data =
      readData();

    const streamer =
      data.streamers.find(
        item =>
          item.active !==
            false &&
          String(
            item.username
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
      makeToken({
        role:
          "streamer",

        streamerId:
          streamer.id,

        username:
          streamer.username
      });

    setLoginCookie(
      res,
      token
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
    clearLoginCookie(res);

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
        req.streamer.id,

      version:
        APP_VERSION
    });
  }
);

/* =========================================================
   ADMIN STREAMERS
========================================================= */

/*
  СПИСОК
*/

app.get(
  "/api/admin/streamers",
  requireAdmin,
  (req, res) => {
    const data =
      readData();

    const list =
      data.streamers.map(
        streamer => {
          const {
            passwordHash,
            ...safe
          } = streamer;

          return {
            ...safe,

            stream:
              data.streamerStreams.find(
                item =>
                  Number(
                    item.streamerId
                  ) ===
                  Number(
                    streamer.id
                  )
              ) || null
          };
        }
      );

    res.json(list);
  }
);

/*
  ДОБАВЛЕНИЕ
*/

app.post(
  "/api/admin/streamers",
  requireAdmin,
  async (
    req,
    res
  ) => {
    try {
      const data =
        readData();

      const body =
        req.body || {};

      const username =
        cleanText(
          body.username,
          40
        );

      const displayName =
        cleanText(
          body.displayName ||
          username,
          60
        );

      const password =
        String(
          body.password ||
          ""
        );

      if (!username) {
        return res
          .status(400)
          .json({
            error:
              "Укажи логин стримера"
          });
      }

      if (
        password.length < 4
      ) {
        return res
          .status(400)
          .json({
            error:
              "Пароль минимум 4 символа"
          });
      }

      const duplicate =
        data.streamers.some(
          item =>
            String(
              item.username
            ).toLowerCase() ===
            username.toLowerCase()
        );

      if (duplicate) {
        return res
          .status(400)
          .json({
            error:
              "Такой логин уже существует"
          });
      }

      const streamer = {
        id:
          nextId(
            data.streamers
          ),

        username,

        displayName,

        avatar:
          cleanImage(
            body.avatar,
            "🎥"
          ),

        active:
          true,

        passwordHash:
          await hashPassword(
            password
          ),

        createdAt:
          new Date()
            .toISOString()
      };

      data.streamers.push(
        streamer
      );

      const firstMatch =
        data.matches[0] ||
        null;

      const stream = {
        id:
          nextId(
            data.streamerStreams
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
          firstMatch?.id ||
          null,

        playerAId:
          firstMatch?.playerAId ||
          null,

        playerBId:
          firstMatch?.playerBId ||
          null,

        accent:
          data.settings.accent ||
          "#b46cff",

        position:
          "bottom",

        showPlayers:
          true,

        showStats:
          true,

        customText:
          `${displayName} • LIVE`,

        textColor:
          "#ffffff",

        panelColor:
          "#17101e",

        glowColor:
          data.settings.accent ||
          "#b46cff",

        backgroundColor:
          "#08060b",

        leftAvatar:
          "",

        rightAvatar:
          "",

        showAvatars:
          true,

        avatarShape:
          "rounded",

        theme:
          "neon",

        updatedAt:
          new Date()
            .toISOString()
      };

      data.streamerStreams.push(
        stream
      );

      await atomicWrite(data);

      broadcast();
      broadcastStreamer(
        streamer.id
      );

      res.json({
        id:
          streamer.id,

        username:
          streamer.username,

        displayName:
          streamer.displayName,

        avatar:
          streamer.avatar,

        active:
          streamer.active,

        stream
      });
    } catch (error) {
      console.error(
        "CREATE STREAMER ERROR:",
        error
      );

      res
        .status(500)
        .json({
          error:
            "Ошибка создания стримера"
        });
    }
  }
);

/*
  РЕДАКТИРОВАНИЕ
*/

app.patch(
  "/api/admin/streamers/:id",
  requireAdmin,
  async (
    req,
    res
  ) => {
    try {
      const data =
        readData();

      const id =
        Number(
          req.params.id
        );

      const streamer =
        data.streamers.find(
          item =>
            Number(item.id) ===
            id
        );

      if (!streamer) {
        return res
          .status(404)
          .json({
            error:
              "Стример не найден"
          });
      }

      const body =
        req.body || {};

      if (
        "username" in body
      ) {
        const username =
          cleanText(
            body.username,
            40
          );

        if (!username) {
          return res
            .status(400)
            .json({
              error:
                "Логин не может быть пустым"
            });
        }

        const duplicate =
          data.streamers.some(
            item =>
              Number(item.id) !==
                id &&
              String(
                item.username
              ).toLowerCase() ===
                username.toLowerCase()
          );

        if (duplicate) {
          return res
            .status(400)
            .json({
              error:
                "Такой логин уже существует"
            });
        }

        streamer.username =
          username;
      }

      if (
        "displayName" in body
      ) {
        streamer.displayName =
          cleanText(
            body.displayName,
            60
          );
      }

      if (
        "avatar" in body
      ) {
        streamer.avatar =
          cleanImage(
            body.avatar,
            streamer.avatar ||
            "🎥"
          );
      }

      if (
        "active" in body
      ) {
        streamer.active =
          !!body.active;
      }

      if (
        body.password
      ) {
        const password =
          String(
            body.password
          );

        if (
          password.length < 4
        ) {
          return res
            .status(400)
            .json({
              error:
                "Пароль минимум 4 символа"
            });
        }

        streamer.passwordHash =
          await hashPassword(
            password
          );
      }

      streamer.updatedAt =
        new Date()
          .toISOString();

      await atomicWrite(data);

      broadcast();
      broadcastStreamer(id);

      res.json({
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
    } catch (error) {
      console.error(
        "UPDATE STREAMER ERROR:",
        error
      );

      res
        .status(500)
        .json({
          error:
            "Ошибка обновления стримера"
        });
    }
  }
);

/*
  УДАЛЕНИЕ
*/

app.delete(
  "/api/admin/streamers/:id",
  requireAdmin,
  async (
    req,
    res
  ) => {
    const data =
      readData();

    const id =
      Number(
        req.params.id
      );

    const exists =
      data.streamers.some(
        item =>
          Number(item.id) ===
          id
      );

    if (!exists) {
      return res
        .status(404)
        .json({
          error:
            "Стример не найден"
        });
    }

    data.streamers =
      data.streamers.filter(
        item =>
          Number(item.id) !==
          id
      );

    data.streamerStreams =
      data.streamerStreams.filter(
        item =>
          Number(
            item.streamerId
          ) !== id
      );

    await atomicWrite(data);

    io
      .to(
        `streamer:${id}`
      )
      .emit(
        "streamer-removed"
      );

    broadcast();

    res.json({
      ok: true
    });
  }
);

/* =========================================================
   STREAMER ZONE
========================================================= */

app.get(
  "/api/streamer/me",
  requireStreamer,
  async (
    req,
    res
  ) => {
    const data =
      readData();

    let stream =
      data.streamerStreams.find(
        item =>
          Number(
            item.streamerId
          ) ===
          Number(
            req.streamer.id
          )
      );

    if (!stream) {
      const firstMatch =
        data.matches[0] ||
        null;

      stream = {
        id:
          nextId(
            data.streamerStreams
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
          firstMatch?.id ||
          null,

        playerAId:
          firstMatch?.playerAId ||
          null,

        playerBId:
          firstMatch?.playerBId ||
          null,

        accent:
          data.settings.accent ||
          "#b46cff",

        position:
          "bottom",

        showPlayers:
          true,

        showStats:
          true,

        customText:
          `${
            req.streamer.displayName ||
            req.streamer.username
          } • LIVE`,

        textColor:
          "#ffffff",

        panelColor:
          "#17101e",

        glowColor:
          data.settings.accent ||
          "#b46cff",

        backgroundColor:
          "#08060b",

        leftAvatar:
          "",

        rightAvatar:
          "",

        showAvatars:
          true,

        avatarShape:
          "rounded",

        theme:
          "neon",

        updatedAt:
          new Date()
            .toISOString()
      };

      data.streamerStreams.push(
        stream
      );

      await atomicWrite(data);
    }

    const publicState =
      publicData(data);

    res.json({
      streamer: {
        id:
          req.streamer.id,

        username:
          req.streamer.username,

        displayName:
          req.streamer.displayName,

        avatar:
          req.streamer.avatar
      },

      stream,

      matches:
        publicState.matches ||
        [],

      players:
        publicState.players ||
        [],

      guilds:
        publicState.guilds ||
        []
    });
  }
);

app.patch(
  "/api/streamer/me",
  requireStreamer,
  async (
    req,
    res
  ) => {
    const data =
      readData();

    const body =
      req.body || {};

    let stream =
      data.streamerStreams.find(
        item =>
          Number(
            item.streamerId
          ) ===
          Number(
            req.streamer.id
          )
      );

    if (!stream) {
      stream = {
        id:
          nextId(
            data.streamerStreams
          ),

        streamerId:
          req.streamer.id,

        status:
          "OFFLINE"
      };

      data.streamerStreams.push(
        stream
      );
    }

    if (
      "title" in body
    ) {
      stream.title =
        cleanText(
          body.title,
          100
        );
    }

    if (
      "platform" in body
    ) {
      stream.platform =
        cleanText(
          body.platform,
          30
        );
    }

    if (
      "streamUrl" in body
    ) {
      stream.streamUrl =
        cleanText(
          body.streamUrl,
          300
        );
    }

    if (
      "matchId" in body
    ) {
      stream.matchId =
        body.matchId
          ? Number(
              body.matchId
            )
          : null;
    }

    if (
      "playerAId" in body
    ) {
      stream.playerAId =
        body.playerAId
          ? Number(
              body.playerAId
            )
          : null;
    }

    if (
      "playerBId" in body
    ) {
      stream.playerBId =
        body.playerBId
          ? Number(
              body.playerBId
            )
          : null;
    }

    if (
      "status" in body
    ) {
      const status =
        String(
          body.status
        ).toUpperCase();

      if (
        [
          "LIVE",
          "OFFLINE",
          "PAUSED"
        ].includes(status)
      ) {
        stream.status =
          status;
      }
    }

    if (
      "accent" in body &&
      /^#[0-9a-f]{6}$/i.test(
        body.accent
      )
    ) {
      stream.accent =
        body.accent;
    }

    if (
      "position" in body
    ) {
      stream.position =
        body.position ===
        "top"
          ? "top"
          : "bottom";
    }

    if (
      "showPlayers" in body
    ) {
      stream.showPlayers =
        !!body.showPlayers;
    }

    if (
      "showStats" in body
    ) {
      stream.showStats =
        !!body.showStats;
    }

    if (
      "customText" in body
    ) {
      stream.customText =
        cleanText(
          body.customText,
          80
        );
    }

    for (const key of [
      "textColor",
      "panelColor",
      "glowColor",
      "backgroundColor"
    ]) {
      if (
        key in body &&
        /^#[0-9a-f]{6}$/i.test(
          String(body[key] || "")
        )
      ) {
        stream[key] =
          body[key];
      }
    }

    if (
      "leftAvatar" in body
    ) {
      stream.leftAvatar =
        cleanImage(
          body.leftAvatar,
          ""
        );
    }

    if (
      "rightAvatar" in body
    ) {
      stream.rightAvatar =
        cleanImage(
          body.rightAvatar,
          ""
        );
    }

    if (
      "showAvatars" in body
    ) {
      stream.showAvatars =
        !!body.showAvatars;
    }

    if (
      "avatarShape" in body
    ) {
      const shape =
        String(
          body.avatarShape
        );

      if (
        [
          "circle",
          "square",
          "rounded"
        ].includes(shape)
      ) {
        stream.avatarShape =
          shape;
      }
    }

    if (
      "theme" in body
    ) {
      const theme =
        String(
          body.theme
        );

      if (
        [
          "neon",
          "glass",
          "minimal"
        ].includes(theme)
      ) {
        stream.theme =
          theme;
      }
    }

    stream.updatedAt =
      new Date()
        .toISOString();

    await atomicWrite(data);

    broadcast();
    broadcastStreamer(
      req.streamer.id
    );

    res.json(stream);
  }
);

/* =========================================================
   STREAMER OVERLAY
========================================================= */

app.get(
  "/api/streamer-overlay/:id",
  (req, res) => {
    const state =
      streamerOverlayState(
        req.params.id
      );

    if (!state) {
      return res
        .status(404)
        .json({
          error:
            "Стример не найден"
        });
    }

    res.json(state);
  }
);

/* =========================================================
   ALL DATA
========================================================= */

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
      globalOverlayState()
    );
  }
);

/* =========================================================
   PLAYERS
========================================================= */

app.post(
  "/api/players",
  requireEditor,
  async (
    req,
    res
  ) => {
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
          50
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

      headshots:
        Number(
          body.headshots
        ) || 0,

      role:
        cleanText(
          body.role ||
          "Player",
          30
        ),

      country:
        cleanText(
          body.country ||
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

    data.players.push(
      player
    );

    await atomicWrite(data);

    broadcast();

    res.json(player);
  }
);

app.patch(
  "/api/players/:id",
  requireAdmin,
  async (
    req,
    res
  ) => {
    const data =
      readData();

    const player =
      data.players.find(
        item =>
          Number(item.id) ===
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
          50
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

    for (
      const key of [
        "elo",
        "wins",
        "losses",
        "kills",
        "deaths",
        "headshots"
      ]
    ) {
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

    await atomicWrite(data);

    broadcast();

    res.json(player);
  }
);

app.delete(
  "/api/players/:id",
  requireAdmin,
  async (
    req,
    res
  ) => {
    const data =
      readData();

    const id =
      Number(
        req.params.id
      );

    data.players =
      data.players.filter(
        item =>
          Number(item.id) !==
          id
      );

    for (
      const match of
      data.matches
    ) {
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

    await atomicWrite(data);

    broadcast();

    res.json({
      ok: true
    });
  }
);


/* =========================================================
   STREAMER PLAYER STATS
   Стример может ТОЛЬКО прибавлять kills / deaths / headshots.
   ELO, wins, losses и профиль игрока меняются не здесь.
========================================================= */

app.post(
  "/api/streamer/player-stats/:id",
  requireStreamer,
  async (
    req,
    res
  ) => {
    try {
      const data =
        readData();

      const player =
        data.players.find(
          item =>
            Number(item.id) ===
            Number(req.params.id)
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

      const kills =
        Math.max(
          0,
          Math.floor(
            Number(body.kills) ||
            0
          )
        );

      const deaths =
        Math.max(
          0,
          Math.floor(
            Number(body.deaths) ||
            0
          )
        );

      const headshots =
        Math.max(
          0,
          Math.floor(
            Number(body.headshots) ||
            0
          )
        );

      if (
        kills === 0 &&
        deaths === 0 &&
        headshots === 0
      ) {
        return res
          .status(400)
          .json({
            error:
              "Добавь хотя бы один показатель"
          });
      }

      if (headshots > kills) {
        return res
          .status(400)
          .json({
            error:
              "Хедшотов не может быть больше киллов за эту катку"
          });
      }

      player.kills =
        (
          Number(player.kills) ||
          0
        ) + kills;

      player.deaths =
        (
          Number(player.deaths) ||
          0
        ) + deaths;

      player.headshots =
        (
          Number(player.headshots) ||
          0
        ) + headshots;

      player.updatedAt =
        new Date()
          .toISOString();

      player.lastStatsBy = {
        streamerId:
          req.streamer.id,

        streamerName:
          req.streamer.displayName ||
          req.streamer.username,

        kills,
        deaths,
        headshots,

        at:
          new Date()
            .toISOString()
      };

      await atomicWrite(data);

      broadcast();

      res.json({
        ok: true,

        player: {
          id:
            player.id,

          nickname:
            player.nickname,

          kills:
            player.kills,

          deaths:
            player.deaths,

          headshots:
            player.headshots,

          kd:
            player.deaths > 0
              ? Number(
                  (
                    player.kills /
                    player.deaths
                  ).toFixed(2)
                )
              : player.kills,

          headshotRate:
            player.kills > 0
              ? Math.round(
                  player.headshots /
                  player.kills *
                  100
                )
              : 0,

          elo:
            player.elo,

          wins:
            player.wins,

          losses:
            player.losses
        }
      });
    } catch (error) {
      console.error(
        "STREAMER PLAYER STATS ERROR:",
        error
      );

      res
        .status(500)
        .json({
          error:
            "Не удалось сохранить статистику"
        });
    }
  }
);


/* =========================================================
   GUILDS
========================================================= */

app.post(
  "/api/guilds",
  requireEditor,
  async (
    req,
    res
  ) => {
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

    if (
      data.guilds.some(
        item =>
          String(
            item.tag
          ).toUpperCase() ===
          tag
      )
    ) {
      return res
        .status(400)
        .json({
          error:
            "Такой тег уже существует"
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

      description:
        cleanText(
          body.description,
          400
        ),

      captain:
        cleanText(
          body.captain,
          50
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

    data.guilds.push(
      guild
    );

    await atomicWrite(data);

    broadcast();

    res.json(guild);
  }
);

app.patch(
  "/api/guilds/:id",
  requireAdmin,
  async (
    req,
    res
  ) => {
    const data =
      readData();

    const guild =
      data.guilds.find(
        item =>
          Number(item.id) ===
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

    if (
      "name" in body
    ) {
      guild.name =
        cleanText(
          body.name,
          50
        );
    }

    if (
      "tag" in body
    ) {
      guild.tag =
        cleanText(
          body.tag,
          12
        ).toUpperCase();
    }

    if (
      "logo" in body
    ) {
      guild.logo =
        cleanImage(
          body.logo,
          guild.logo ||
          "🪻"
        );
    }

    if (
      "region" in body
    ) {
      guild.region =
        cleanText(
          body.region,
          40
        );
    }

    if (
      "description" in body
    ) {
      guild.description =
        cleanText(
          body.description,
          400
        );
    }

    if (
      "captain" in body
    ) {
      guild.captain =
        cleanText(
          body.captain,
          50
        );
    }

    for (
      const key of [
        "elo",
        "wins",
        "losses"
      ]
    ) {
      if (key in body) {
        guild[key] =
          Number(
            body[key]
          ) || 0;
      }
    }

    guild.updatedAt =
      new Date()
        .toISOString();

    await atomicWrite(data);

    broadcast();

    res.json(guild);
  }
);

app.delete(
  "/api/guilds/:id",
  requireAdmin,
  async (
    req,
    res
  ) => {
    const data =
      readData();

    const id =
      Number(
        req.params.id
      );

    data.guilds =
      data.guilds.filter(
        item =>
          Number(item.id) !==
          id
      );

    for (
      const player of
      data.players
    ) {
      if (
        Number(
          player.guildId
        ) === id
      ) {
        player.guildId =
          null;
      }
    }

    for (
      const match of
      data.matches
    ) {
      if (
        Number(
          match.guildAId
        ) === id
      ) {
        match.guildAId =
          null;
      }

      if (
        Number(
          match.guildBId
        ) === id
      ) {
        match.guildBId =
          null;
      }
    }

    await atomicWrite(data);

    broadcast();

    res.json({
      ok: true
    });
  }
);

/* =========================================================
   MATCHES
========================================================= */

app.post(
  "/api/matches",
  requireEditor,
  async (
    req,
    res
  ) => {
    try {
      const data = readData();
      const body = req.body || {};

      const guildAId = Number(body.guildAId) || null;
      const guildBId = Number(body.guildBId) || null;
      const seriesSize = seriesSizeFromValue(body.seriesSize || body.format || 6);

      if (!guildAId || !guildBId) {
        return res.status(400).json({
          error: "Выбери две гильдии"
        });
      }

      if (guildAId === guildBId) {
        return res.status(400).json({
          error: "Нужно выбрать две разные гильдии"
        });
      }

      const guildA = data.guilds.find(g => Number(g.id) === guildAId);
      const guildB = data.guilds.find(g => Number(g.id) === guildBId);

      if (!guildA || !guildB) {
        return res.status(400).json({
          error: "Одна из гильдий не найдена"
        });
      }

      const match = {
        id: nextId(data.matches),

        title:
          cleanText(
            body.title ||
            "GUILD BATTLE",
            80
          ),

        tournament:
          cleanText(
            body.tournament ||
            "LAVENDER CUP",
            80
          ),

        subtitle:
          cleanText(
            body.subtitle ||
            "FREE FIRE",
            80
          ),

        guildAId,
        guildBId,

        seriesSize,
        targetWins: seriesSize,

        playerAId: null,
        playerBId: null,

        scoreA: 0,
        scoreB: 0,

        roundText: "GAME 1",

        format: `${seriesSize}v${seriesSize}`,

        status:
          cleanText(
            body.status ||
            "SCHEDULED",
            20
          ).toUpperCase(),

        seriesHistory: [],

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

      data.matches.push(match);
      data.overlay.activeMatchId = match.id;

      await atomicWrite(data);
      broadcast();

      res.json(match);
    } catch (error) {
      console.error("CREATE MATCH ERROR:", error);

      res.status(500).json({
        error: "Не удалось создать матч"
      });
    }
  }
);

app.patch(
  "/api/matches/:id",
  requireEditor,
  async (
    req,
    res
  ) => {
    const data =
      readData();

    const match =
      data.matches.find(
        item =>
          Number(item.id) ===
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

    for (
      const key of [
        "title",
        "tournament",
        "subtitle",
        "roundText",
        "format",
        "status"
      ]
    ) {
      if (key in body) {
        match[key] =
          cleanText(
            body[key],
            80
          );
      }
    }

    for (
      const key of [
        "guildAId",
        "guildBId",
        "playerAId",
        "playerBId"
      ]
    ) {
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

    match.updatedAt =
      new Date()
        .toISOString();

    await atomicWrite(data);

    broadcast();

    res.json(match);
  }
);


/* =========================================================
   SERIES CONFIG + GAME RESULT
========================================================= */

app.patch(
  "/api/matches/:id/series-config",
  requireEditor,
  async (req, res) => {
    try {
      const data = readData();

      const match = data.matches.find(
        item => Number(item.id) === Number(req.params.id)
      );

      if (!match) {
        return res.status(404).json({
          error: "Матч не найден"
        });
      }

      match.seriesHistory = Array.isArray(match.seriesHistory)
        ? match.seriesHistory
        : [];

      if (match.seriesHistory.length) {
        return res.status(400).json({
          error: "После первой игры формат и гильдии менять нельзя"
        });
      }

      const body = req.body || {};

      const guildAId = Number(body.guildAId ?? match.guildAId) || null;
      const guildBId = Number(body.guildBId ?? match.guildBId) || null;
      const size = seriesSizeFromValue(
        body.seriesSize ||
        body.format ||
        match.seriesSize ||
        match.format
      );

      if (!guildAId || !guildBId) {
        return res.status(400).json({
          error: "Выбери две гильдии"
        });
      }

      if (guildAId === guildBId) {
        return res.status(400).json({
          error: "Нужно выбрать две разные гильдии"
        });
      }

      const guildA = data.guilds.find(g => Number(g.id) === guildAId);
      const guildB = data.guilds.find(g => Number(g.id) === guildBId);

      if (!guildA || !guildB) {
        return res.status(400).json({
          error: "Одна из гильдий не найдена"
        });
      }

      match.guildAId = guildAId;
      match.guildBId = guildBId;
      match.seriesSize = size;
      match.targetWins = size;
      match.format = `${size}v${size}`;
      match.scoreA = 0;
      match.scoreB = 0;
      match.playerAId = null;
      match.playerBId = null;
      match.seriesHistory = [];
      match.roundText = "GAME 1";
      match.updatedAt = new Date().toISOString();

      await atomicWrite(data);
      broadcast();

      res.json(match);
    } catch (error) {
      console.error("SERIES CONFIG ERROR:", error);

      res.status(500).json({
        error: "Ошибка настройки серии"
      });
    }
  }
);

app.post(
  "/api/matches/:id/series-game",
  requireEditor,
  async (req, res) => {
    try {
      const data = readData();

      const match = data.matches.find(
        item => Number(item.id) === Number(req.params.id)
      );

      if (!match) {
        return res.status(404).json({
          error: "Матч не найден"
        });
      }

      if (String(match.status).toUpperCase() === "FINAL") {
        return res.status(400).json({
          error: "Серия уже завершена"
        });
      }

      const playerAId = Number(req.body?.playerAId) || null;
      const playerBId = Number(req.body?.playerBId) || null;
      const winnerId = Number(req.body?.winnerId) || null;

      if (!playerAId || !playerBId) {
        return res.status(400).json({
          error: "Перед каждой каткой выбери двух игроков"
        });
      }

      if (playerAId === playerBId) {
        return res.status(400).json({
          error: "Один игрок не может играть против себя"
        });
      }

      const playerA = data.players.find(
        p => Number(p.id) === playerAId
      );

      const playerB = data.players.find(
        p => Number(p.id) === playerBId
      );

      if (!playerA || !playerB) {
        return res.status(400).json({
          error: "Один из игроков не найден"
        });
      }

      if (Number(playerA.guildId) !== Number(match.guildAId)) {
        return res.status(400).json({
          error: `${playerA.nickname} не состоит в гильдии A`
        });
      }

      if (Number(playerB.guildId) !== Number(match.guildBId)) {
        return res.status(400).json({
          error: `${playerB.nickname} не состоит в гильдии B`
        });
      }

      if (![playerAId, playerBId].includes(winnerId)) {
        return res.status(400).json({
          error: "Выбери победителя текущей игры"
        });
      }

      const eloChange = applySeriesGameElo(
        data,
        playerAId,
        playerBId,
        winnerId
      );

      if (winnerId === playerAId) {
        match.scoreA = (Number(match.scoreA) || 0) + 1;
      } else {
        match.scoreB = (Number(match.scoreB) || 0) + 1;
      }

      match.seriesHistory = Array.isArray(match.seriesHistory)
        ? match.seriesHistory
        : [];

      const gameNumber = match.seriesHistory.length + 1;

      match.playerAId = playerAId;
      match.playerBId = playerBId;

      match.seriesHistory.push({
        game: gameNumber,
        playerAId,
        playerBId,
        playerAName: playerA.nickname,
        playerBName: playerB.nickname,
        winnerId,
        eloChange,
        playedAt: new Date().toISOString()
      });

      const size = seriesSizeFromValue(
        match.seriesSize ||
        match.format
      );

      match.seriesSize = size;
      match.targetWins = Number(match.targetWins) || size;
      match.format = `${size}v${size}`;

      const finished =
        Number(match.scoreA) >= match.targetWins ||
        Number(match.scoreB) >= match.targetWins;

      if (finished) {
        match.status = "FINAL";
        match.roundText = `FINAL • ${match.scoreA}:${match.scoreB}`;
      } else {
        match.status = "LIVE";
        match.roundText = `GAME ${gameNumber + 1}`;

        // ВАЖНО: следующая пара НЕ выбирается автоматически.
        // Стример/админ выбирает двух игроков вручную перед следующей каткой.
        match.playerAId = null;
        match.playerBId = null;
      }

      match.updatedAt = new Date().toISOString();

      await atomicWrite(data);
      broadcast();

      res.json({
        ok: true,
        finished,
        match,
        eloChange,

        game: {
          number: gameNumber,

          playerA: {
            id: playerA.id,
            nickname: playerA.nickname,
            elo: playerA.elo,
            rank: rankForElo(playerA.elo)
          },

          playerB: {
            id: playerB.id,
            nickname: playerB.nickname,
            elo: playerB.elo,
            rank: rankForElo(playerB.elo)
          },

          winnerId
        }
      });
    } catch (error) {
      console.error("SERIES GAME ERROR:", error);

      res.status(500).json({
        error:
          error.message ||
          "Ошибка сохранения результата игры"
      });
    }
  }
);

/* =========================================================
   FINISH MATCH + ELO
========================================================= */

app.post(
  "/api/matches/:id/finish",
  requireEditor,
  async (
    req,
    res
  ) => {
    const data =
      readData();

    const match =
      data.matches.find(
        item =>
          Number(item.id) ===
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
      "scoreA" in req.body
    ) {
      match.scoreA =
        Number(
          req.body.scoreA
        ) || 0;
    }

    if (
      "scoreB" in req.body
    ) {
      match.scoreB =
        Number(
          req.body.scoreB
        ) || 0;
    }

    if (
      "playerAId" in
      req.body
    ) {
      match.playerAId =
        Number(
          req.body.playerAId
        ) || null;
    }

    if (
      "playerBId" in
      req.body
    ) {
      match.playerBId =
        Number(
          req.body.playerBId
        ) || null;
    }

    if (
      Number(match.scoreA) ===
      Number(match.scoreB)
    ) {
      return res
        .status(400)
        .json({
          error:
            "Ничья не подходит для ELO"
        });
    }

    if (
      !match.playerAId ||
      !match.playerBId
    ) {
      return res
        .status(400)
        .json({
          error:
            "Выбери двух игроков"
        });
    }

    match.status =
      "FINAL";

    match.updatedAt =
      new Date()
        .toISOString();

    const change =
      applyMatchElo(
        data,
        match
      );

    await atomicWrite(data);

    broadcast();

    const playerA =
      data.players.find(
        player =>
          Number(player.id) ===
          Number(
            match.playerAId
          )
      );

    const playerB =
      data.players.find(
        player =>
          Number(player.id) ===
          Number(
            match.playerBId
          )
      );

    res.json({
      ok: true,

      match,

      eloChange:
        change ||
        match.eloChange ||
        null,

      players: {
        A: playerA
          ? {
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
            }
          : null,

        B: playerB
          ? {
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
          : null
      }
    });
  }
);

/* =========================================================
   SELECT WINNER + AUTO ELO
========================================================= */

app.post(
  "/api/matches/:id/set-winner",
  requireEditor,
  async (
    req,
    res
  ) => {
    const data =
      readData();

    const match =
      data.matches.find(
        item =>
          Number(item.id) ===
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
        req.body?.playerAId ||
        match.playerAId
      );

    const playerBId =
      Number(
        req.body?.playerBId ||
        match.playerBId
      );

    const winnerId =
      Number(
        req.body?.winnerId
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

    const change =
      applyMatchElo(
        data,
        match
      );

    await atomicWrite(data);

    broadcast();

    const playerA =
      data.players.find(
        player =>
          Number(player.id) ===
          playerAId
      );

    const playerB =
      data.players.find(
        player =>
          Number(player.id) ===
          playerBId
      );

    res.json({
      ok: true,

      winnerId,

      match,

      eloChange:
        change,

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
  async (
    req,
    res
  ) => {
    const data =
      readData();

    const id =
      Number(
        req.params.id
      );

    data.matches =
      data.matches.filter(
        item =>
          Number(item.id) !==
          id
      );

    if (
      Number(
        data.overlay
          .activeMatchId
      ) === id
    ) {
      data.overlay
        .activeMatchId =
        data.matches[0]?.id ||
        null;
    }

    for (
      const stream of
      data.streamerStreams
    ) {
      if (
        Number(
          stream.matchId
        ) === id
      ) {
        stream.matchId =
          data.matches[0]?.id ||
          null;
      }
    }

    await atomicWrite(data);

    broadcast();

    res.json({
      ok: true
    });
  }
);

/* =========================================================
   TOURNAMENTS
========================================================= */

app.post(
  "/api/tournaments",
  requireAdmin,
  async (
    req,
    res
  ) => {
    const data =
      readData();

    const body =
      req.body || {};

    const name =
      cleanText(
        body.name,
        80
      );

    if (!name) {
      return res
        .status(400)
        .json({
          error:
            "Укажи название турнира"
        });
    }

    const tournament = {
      id:
        nextId(
          data.tournaments
        ),

      name,

      status:
        cleanText(
          body.status ||
          "UPCOMING",
          20
        ).toUpperCase(),

      date:
        cleanText(
          body.date,
          30
        ),

      format:
        cleanText(
          body.format ||
          "BO7",
          20
        ),

      prize:
        cleanText(
          body.prize,
          80
        ),

      description:
        cleanText(
          body.description,
          500
        ),

      guildIds:
        Array.isArray(
          body.guildIds
        )
          ? body.guildIds.map(
              Number
            )
          : [],

      createdAt:
        new Date()
          .toISOString()
    };

    data.tournaments.push(
      tournament
    );

    await atomicWrite(data);

    broadcast();

    res.json(tournament);
  }
);

app.patch(
  "/api/tournaments/:id",
  requireAdmin,
  async (
    req,
    res
  ) => {
    const data =
      readData();

    const tournament =
      data.tournaments.find(
        item =>
          Number(item.id) ===
          Number(
            req.params.id
          )
      );

    if (!tournament) {
      return res
        .status(404)
        .json({
          error:
            "Турнир не найден"
        });
    }

    const body =
      req.body || {};

    for (
      const key of [
        "name",
        "status",
        "date",
        "format",
        "prize",
        "description"
      ]
    ) {
      if (
        key in body
      ) {
        tournament[key] =
          cleanText(
            body[key],
            key ===
              "description"
              ? 500
              : 100
          );
      }
    }

    if (
      "guildIds" in body
    ) {
      tournament.guildIds =
        Array.isArray(
          body.guildIds
        )
          ? body.guildIds.map(
              Number
            )
          : [];
    }

    tournament.status =
      String(
        tournament.status ||
        "UPCOMING"
      ).toUpperCase();

    await atomicWrite(data);

    broadcast();

    res.json(tournament);
  }
);

app.delete(
  "/api/tournaments/:id",
  requireAdmin,
  async (
    req,
    res
  ) => {
    const data =
      readData();

    const id =
      Number(
        req.params.id
      );

    data.tournaments =
      data.tournaments.filter(
        item =>
          Number(item.id) !==
          id
      );

    await atomicWrite(data);

    broadcast();

    res.json({
      ok: true
    });
  }
);

/* =========================================================
   NEWS
========================================================= */

app.post(
  "/api/news",
  requireAdmin,
  async (
    req,
    res
  ) => {
    const data =
      readData();

    const body =
      req.body || {};

    const title =
      cleanText(
        body.title,
        120
      );

    if (!title) {
      return res
        .status(400)
        .json({
          error:
            "Укажи заголовок"
        });
    }

    const news = {
      id:
        nextId(
          data.news
        ),

      title,

      body:
        cleanText(
          body.body,
          1500
        ),

      pinned:
        !!body.pinned,

      createdAt:
        new Date()
          .toISOString()
    };

    data.news.unshift(
      news
    );

    await atomicWrite(data);

    broadcast();

    res.json(news);
  }
);

app.patch(
  "/api/news/:id",
  requireAdmin,
  async (
    req,
    res
  ) => {
    const data =
      readData();

    const news =
      data.news.find(
        item =>
          Number(item.id) ===
          Number(
            req.params.id
          )
      );

    if (!news) {
      return res
        .status(404)
        .json({
          error:
            "Новость не найдена"
        });
    }

    if (
      "title" in
      req.body
    ) {
      news.title =
        cleanText(
          req.body.title,
          120
        );
    }

    if (
      "body" in
      req.body
    ) {
      news.body =
        cleanText(
          req.body.body,
          1500
        );
    }

    if (
      "pinned" in
      req.body
    ) {
      news.pinned =
        !!req.body.pinned;
    }

    await atomicWrite(data);

    broadcast();

    res.json(news);
  }
);

app.delete(
  "/api/news/:id",
  requireAdmin,
  async (
    req,
    res
  ) => {
    const data =
      readData();

    const id =
      Number(
        req.params.id
      );

    data.news =
      data.news.filter(
        item =>
          Number(item.id) !==
          id
      );

    await atomicWrite(data);

    broadcast();

    res.json({
      ok: true
    });
  }
);

/* =========================================================
   SETTINGS
========================================================= */

app.patch(
  "/api/settings",
  requireAdmin,
  async (
    req,
    res
  ) => {
    const data =
      readData();

    if (
      "siteName" in
      req.body
    ) {
      data.settings.siteName =
        cleanText(
          req.body.siteName,
          40
        );
    }

    if (
      "tagline" in
      req.body
    ) {
      data.settings.tagline =
        cleanText(
          req.body.tagline,
          100
        );
    }

    if (
      /^#[0-9a-f]{6}$/i.test(
        req.body?.accent ||
        ""
      )
    ) {
      data.settings.accent =
        req.body.accent;
    }

    await atomicWrite(data);

    broadcast();

    res.json(
      data.settings
    );
  }
);

/* =========================================================
   GLOBAL OVERLAY
========================================================= */

app.patch(
  "/api/overlay",
  requireAdmin,
  async (
    req,
    res
  ) => {
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
      "accent" in body &&
      /^#[0-9a-f]{6}$/i.test(
        body.accent
      )
    ) {
      data.overlay.accent =
        body.accent;
    }

    if (
      "position" in body
    ) {
      data.overlay.position =
        body.position ===
        "top"
          ? "top"
          : "bottom";
    }

    if (
      "showPlayers" in
      body
    ) {
      data.overlay.showPlayers =
        !!body.showPlayers;
    }

    if (
      "showStats" in body
    ) {
      data.overlay.showStats =
        !!body.showStats;
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

    for (const key of [
      "textColor",
      "panelColor",
      "glowColor",
      "backgroundColor"
    ]) {
      if (
        key in body &&
        /^#[0-9a-f]{6}$/i.test(
          String(body[key] || "")
        )
      ) {
        data.overlay[key] =
          body[key];
      }
    }

    if (
      "leftAvatar" in body
    ) {
      data.overlay.leftAvatar =
        cleanImage(
          body.leftAvatar,
          ""
        );
    }

    if (
      "rightAvatar" in body
    ) {
      data.overlay.rightAvatar =
        cleanImage(
          body.rightAvatar,
          ""
        );
    }

    if (
      "showAvatars" in body
    ) {
      data.overlay.showAvatars =
        !!body.showAvatars;
    }

    if (
      "avatarShape" in body
    ) {
      const shape =
        String(
          body.avatarShape
        );

      if (
        [
          "circle",
          "square",
          "rounded"
        ].includes(shape)
      ) {
        data.overlay.avatarShape =
          shape;
      }
    }

    if (
      "theme" in body
    ) {
      const theme =
        String(
          body.theme
        );

      if (
        [
          "neon",
          "glass",
          "minimal"
        ].includes(theme)
      ) {
        data.overlay.theme =
          theme;
      }
    }

    await atomicWrite(data);

    broadcast();

    res.json(
      globalOverlayState()
    );
  }
);

/* =========================================================
   BACKUP
========================================================= */

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
  async (
    req,
    res
  ) => {
    const data =
      normalizeData(
        req.body
      );

    await atomicWrite(data);

    broadcast();

    res.json({
      ok: true
    });
  }
);

/* =========================================================
   SOCKET.IO
========================================================= */

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
        const streamerId =
          Number(id);

        if (!streamerId) {
          return;
        }

        socket.join(
          `streamer:${streamerId}`
        );

        socket.emit(
          "streamer-overlay:update",
          streamerOverlayState(
            streamerId
          )
        );
      }
    );
  }
);

/* =========================================================
   PAGES
========================================================= */

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

/*
  Любая неизвестная страница ->
  index.html

  ВАЖНО:
  API маршруты находятся выше,
  поэтому они сюда не попадут.
*/

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

/* =========================================================
   START
========================================================= */

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
      "SERVER START ERROR:",
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
    } catch (error) {
      console.error(error);
    }

    process.exit(0);
  }
);

start();
