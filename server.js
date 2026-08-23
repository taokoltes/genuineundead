// Relais entre OpenSea et la page genuineundeadcometolife.html
// A deployer sur un hebergeur qui fait tourner du Node en continu
// (Render, Fly.io, Railway, un petit VPS...) puisque taokoltes.com
// est un hebergement statique. Ne jamais copier ce dossier sur
// taokoltes.com: OPENSEA_API_KEY doit rester cote serveur.

import 'dotenv/config';
import express from 'express';
import { WebSocketServer } from 'ws';
import { WebSocket } from 'ws';
import { LocalStorage } from 'node-localstorage';
import { OpenSeaStreamClient, EventType } from '@opensea/stream-js';

const PORT = process.env.PORT || 8080;
const OPENSEA_API_KEY = process.env.OPENSEA_API_KEY;
const COLLECTION_SLUG = process.env.COLLECTION_SLUG || 'genuine-undead';
const CONTRACT_ADDRESS = process.env.CONTRACT_ADDRESS || '0xcd44ef6b018fe6061cbb50c44af94ce5f1cfc0b1';

if (!OPENSEA_API_KEY) {
  console.error('OPENSEA_API_KEY manquante. Copier .env.example vers .env et renseigner la cle.');
  process.exit(1);
}

// -----------------------------------------------------------------
// SERVEUR WEBSOCKET COTE FRONTEND
// -----------------------------------------------------------------
const app = express();
app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*'); // restreindre a taokoltes.com en prod
  next();
});

const server = app.listen(PORT, () => {
  console.log(`Relais actif sur le port ${PORT}`);
});

const wss = new WebSocketServer({ server });

function broadcast(message) {
  const payload = JSON.stringify(message);
  wss.clients.forEach((client) => {
    if (client.readyState === client.OPEN) client.send(payload);
  });
}

// -----------------------------------------------------------------
// LIVE EVENT RESILIENCE
// The OpenSea Stream API is explicitly documented as "best effort":
// any event that arrives while this process is disconnected (a
// dropped socket, a Render free-tier cold start after inactivity, a
// crash/restart) is simply never re-sent by OpenSea. Relying on the
// stream alone means a real sale can happen and just never reach the
// site if nobody was connected/running at that exact moment - which
// is exactly the "3 ventes il y a une heure" symptom.
//
// Two standard mitigations for this kind of feed:
//  1. RING BUFFER + BACKLOG ON CONNECT: every event this relay
//     broadcasts is also kept here. Any browser that (re)connects -
//     including one that missed events while the tab was closed or
//     the relay was asleep - gets the buffer replayed once as its
//     first message, so the cube and "Recent events" catch up
//     instead of silently staying stale forever.
//  2. REST POLLING BACKSTOP (added further down): independently of
//     the stream, this relay also polls OpenSea's REST events
//     endpoint on an interval and backfills anything the stream
//     missed, deduplicated against what's already gone out.
// -----------------------------------------------------------------
const RING_BUFFER_SIZE = 60;
const recentBroadcasts = []; // oldest first: { tokenId, type, timestamp }

function recordBroadcast(tokenId, type) {
  recentBroadcasts.push({ tokenId, type, timestamp: Date.now() });
  if (recentBroadcasts.length > RING_BUFFER_SIZE) recentBroadcasts.shift();
}

// Every live event (stream or REST backstop) goes through this
// single function, so the ring buffer always reflects exactly what
// was actually broadcast.
function broadcastEvent(tokenId, type) {
  recordBroadcast(tokenId, type);
  broadcast({ tokenIndex: tokenIdToIndex(tokenId), type });
}

wss.on('connection', (ws) => {
  if (recentBroadcasts.length === 0) return;
  ws.send(JSON.stringify({
    backlog: recentBroadcasts.map((e) => ({
      tokenIndex: tokenIdToIndex(e.tokenId),
      type: e.type,
      timestamp: e.timestamp
    }))
  }));
});

// -----------------------------------------------------------------
// TOKEN ID -> INDEX DANS LE MONOLITHE
// Le monolithe attribue l'indice 0 au premier voxel (coin haut-gauche
// de la face avant). Si Genuine Undead numerote ses tokens de 1 a 9900,
// tokenIndex = tokenId - 1. A verifier/ajuster selon la numerotation
// reelle de la collection (certaines collections commencent a 0,
// d'autres ont des trous).
function tokenIdToIndex(tokenId) {
  return Number(tokenId) - 1;
}

function extractTokenId(nftId) {
  // format attendu: "ethereum/0xcontrat/1234"
  const parts = String(nftId).split('/');
  return parts[parts.length - 1];
}

// -----------------------------------------------------------------
// CONNEXION A OPENSEA STREAM API
// -----------------------------------------------------------------
const client = new OpenSeaStreamClient({
  token: OPENSEA_API_KEY,
  connectOptions: {
    transport: WebSocket,
    sessionStorage: LocalStorage
  }
});

// Best-effort dedup key shared with the REST poll below, so an event
// delivered by both the stream and a poll cycle only pulses once.
// Not watertight (the two APIs don't guarantee identical timestamps
// down to the second) but cheap and good enough - a rare duplicate
// pulse is harmless, a missed sale is not.
function markStreamEvent(openseaType, tokenId, ts) {
  if (ts) seenEventKeys.add(`${openseaType}:${tokenId}:${ts}`);
}

client.onItemSold(COLLECTION_SLUG, (event) => {
  const tokenId = extractTokenId(event.payload.item.nft_id);
  markStreamEvent('sale', tokenId, event.payload.event_timestamp || event.payload.closing_date);
  broadcastEvent(tokenId, 'sale');
});

client.onItemListed(COLLECTION_SLUG, (event) => {
  const tokenId = extractTokenId(event.payload.item.nft_id);
  markStreamEvent('listing', tokenId, event.payload.event_timestamp || event.payload.listing_date);
  broadcastEvent(tokenId, 'offer');
});

client.onItemReceivedOffer(COLLECTION_SLUG, (event) => {
  const tokenId = extractTokenId(event.payload.item.nft_id);
  markStreamEvent('offer', tokenId, event.payload.event_timestamp);
  broadcastEvent(tokenId, 'offer');
});

client.onItemReceivedBid(COLLECTION_SLUG, (event) => {
  // "auction accepted" only ever comes from the live stream: OpenSea's
  // REST events endpoint has no event_type that distinguishes an
  // accepted bid from a regular sale, so the polling backstop below
  // can never reconstruct this one if it's missed. Documented gap.
  const tokenId = extractTokenId(event.payload.item.nft_id);
  broadcastEvent(tokenId, 'auction');
});

client.onItemTransferred(COLLECTION_SLUG, (event) => {
  const tokenId = extractTokenId(event.payload.item.nft_id);
  markStreamEvent('transfer', tokenId, event.payload.event_timestamp || event.payload.transaction?.timestamp);
  broadcastEvent(tokenId, 'transfer');
});

console.log(`Ecoute des evenements OpenSea pour ${COLLECTION_SLUG}...`);

// -----------------------------------------------------------------
// REST POLLING BACKSTOP
// Independent of the stream above: periodically re-checks OpenSea's
// own collection-wide history endpoint and broadcasts anything not
// already seen. This is what actually fixes "3 sales happened an
// hour ago and the cube never showed them" - if the stream was
// disconnected (or this whole process was asleep on a free Render
// dyno) at that moment, the stream can never recover those events,
// but this poll picks them up on its next run regardless.
// -----------------------------------------------------------------
const SEEN_KEYS_CAP = 500;
const seenEventKeys = new Set();

function markSeen(key) {
  seenEventKeys.add(key);
  if (seenEventKeys.size > SEEN_KEYS_CAP) {
    seenEventKeys.delete(seenEventKeys.values().next().value);
  }
}

// Only these OpenSea event_type values map onto the site's legend
// (sale / auction / new offer-listing / mint-transfer, see
// config.js EVENT_COLORS). "cancel", "redemption" and raw "order"
// events have no bucket in the UI and are ignored here.
function mapEventType(openseaEventType) {
  switch (openseaEventType) {
    case 'sale': return 'sale';
    case 'listing': return 'offer';
    case 'offer': return 'offer';
    case 'transfer': return 'transfer';
    default: return null; // cancel, redemption, order: no UI bucket
  }
}

let pollAfter = Math.floor(Date.now() / 1000) - 3600; // first run: catch up on the last hour

async function pollCollectionEvents() {
  const since = pollAfter;
  pollAfter = Math.floor(Date.now() / 1000);
  try {
    const res = await fetch(
      `https://api.opensea.io/api/v2/events/collection/${COLLECTION_SLUG}?after=${since}&limit=50`,
      { headers: { 'x-api-key': OPENSEA_API_KEY } }
    );
    if (!res.ok) throw new Error(`events poll HTTP ${res.status}`);
    const data = await res.json();
    for (const ev of (data.asset_events || [])) {
      const type = mapEventType(ev.event_type);
      const tokenId = ev.nft?.identifier;
      if (!type || !tokenId) continue;
      const key = `${ev.event_type}:${tokenId}:${ev.event_timestamp}`;
      if (seenEventKeys.has(key)) continue;
      markSeen(key);
      broadcastEvent(tokenId, type);
    }
  } catch (err) {
    console.error('[relay] events poll failed:', err.message || err);
  }
}

pollCollectionEvents(); // catch up immediately on boot/wake
setInterval(pollCollectionEvents, 45000); // then every 45s as a backstop

// -----------------------------------------------------------------
// HISTORIQUE D'UN TOKEN (appele au clic sur la page)
// -----------------------------------------------------------------
app.get('/token/:index', async (req, res) => {
  const tokenIndex = Number(req.params.index);
  const tokenId = tokenIndex + 1; // inverse de tokenIdToIndex, meme hypothese

  try {
    const nftRes = await fetch(
      `https://api.opensea.io/api/v2/chain/ethereum/contract/${CONTRACT_ADDRESS}/nfts/${tokenId}`,
      { headers: { 'x-api-key': OPENSEA_API_KEY } }
    );
    const nftData = await nftRes.json();

    const eventsRes = await fetch(
      `https://api.opensea.io/api/v2/events/chain/ethereum/contract/${CONTRACT_ADDRESS}?token_ids=${tokenId}&limit=10`,
      { headers: { 'x-api-key': OPENSEA_API_KEY } }
    );
    const eventsData = await eventsRes.json();

    // Forward the raw material instead of pre-digesting it into
    // "rows": the client (events.js normalizeHistory/normalizeTraits)
    // already builds the on-chain history summary AND the traits grid
    // from exactly this shape (nft.traits + asset_events) - it was
    // just never being sent. This also fixes "no trait data yet":
    // nftData.nft.traits was fetched from OpenSea all along, it was
    // simply dropped before reaching the browser.
    res.json({
      imageUrl: nftData?.nft?.image_url || null,
      traits: nftData?.nft?.traits || [],
      events: eventsData.asset_events || []
    });
  } catch (err) {
    res.status(502).json({
      imageUrl: null,
      traits: [],
      rows: [{ label: 'Erreur', value: 'API OpenSea indisponible' }]
    });
  }
});
