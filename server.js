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

function recordBroadcast(tokenId, type, timestamp) {
  recentBroadcasts.push({ tokenId, type, timestamp: timestamp || Date.now() });
  if (recentBroadcasts.length > RING_BUFFER_SIZE) recentBroadcasts.shift();
}

// -----------------------------------------------------------------
// LAST-24H EVENT LOG
// Separate from the ring buffer above on purpose: the ring buffer is
// capped by COUNT (60 entries) purely to give a freshly-(re)connected
// WebSocket something to catch up on; it can be shorter than 24h on a
// busy day, or hold stale entries during a quiet one. This log is
// capped by TIME instead, so GET /recent-events (below) can always
// answer "every sale / new offer in the last 24h",
// which is what the frontend needs to make every concerned Genuine
// Undead blink on page load - not just the last N events.
// -----------------------------------------------------------------
const DAY_MS = 24 * 60 * 60 * 1000;
const dailyEvents = []; // oldest first: { tokenId, type, timestamp }

function pruneDailyEvents() {
  const cutoff = Date.now() - DAY_MS;
  while (dailyEvents.length && dailyEvents[0].timestamp < cutoff) dailyEvents.shift();
}

function recordDailyEvent(tokenId, type, timestamp) {
  dailyEvents.push({ tokenId, type, timestamp: timestamp || Date.now() });
  pruneDailyEvents();
}

// -----------------------------------------------------------------
// "SALES 48H" LOG — sales only, kept for twice as long as dailyEvents
// above. Powers GET /recent-sales-48h further down, which only
// returns the OLDER half of this window (24h-48h ago): the newer half
// is exactly what /recent-events (and the pink cube treatment) above
// already cover, so this endpoint only ever adds the "extra" day of
// history the blue "sales 48h" button asks for - fetched on demand
// only when that button is clicked (see events.js), never pushed
// automatically the way the 24h catch-up is.
// -----------------------------------------------------------------
const TWO_DAY_MS = 2 * DAY_MS;
const salesLog48h = []; // oldest first: { tokenId, timestamp } — sales only

function pruneSalesLog48h() {
  const cutoff = Date.now() - TWO_DAY_MS;
  while (salesLog48h.length && salesLog48h[0].timestamp < cutoff) salesLog48h.shift();
}

function recordSalesLog48h(tokenId, type, timestamp) {
  if (type !== 'sale') return; // "sales 48h" is sales only, never offers
  salesLog48h.push({ tokenId, timestamp: timestamp || Date.now() });
  pruneSalesLog48h();
}

// Every live event (stream or REST backstop) goes through this
// single function, so the ring buffer, the 24h log and the 48h sales
// log always reflect exactly what was actually broadcast. timestampMs
// is optional - live stream events happening right now omit it and
// default to "now"; the REST backstop below passes the event's real
// on-chain/OpenSea timestamp so /recent-events reflects when things
// actually happened, not when this process happened to poll for them.
function broadcastEvent(tokenId, type, timestampMs) {
  const ts = timestampMs || Date.now();
  recordBroadcast(tokenId, type, ts);
  recordDailyEvent(tokenId, type, ts);
  recordSalesLog48h(tokenId, type, ts);
  broadcast({ tokenIndex: tokenIdToIndex(tokenId), type, timestamp: ts });
}

// GET /recent-events: every sale / new offer
// broadcast in the last 24h, regardless of whether a browser was
// connected when it happened. This is what events.js fetches once on
// every page load so all concerned Genuine Undead blink on arrival,
// not just events that land while the tab is already open.
app.get('/recent-events', (req, res) => {
  pruneDailyEvents();
  res.json({
    events: dailyEvents.map((e) => ({
      tokenIndex: tokenIdToIndex(e.tokenId),
      type: e.type,
      timestamp: e.timestamp
    }))
  });
});

// GET /recent-sales-48h: sales broadcast between 24h and 48h ago only
// - the older half of the window, additive to whatever /recent-events
// (last 24h) already covers. Fetched on demand only, when the blue
// "sales 48h" button is clicked client-side (see events.js) - never
// on page load, unlike /recent-events above.
app.get('/recent-sales-48h', (req, res) => {
  pruneSalesLog48h();
  const cutoff24h = Date.now() - DAY_MS;
  res.json({
    events: salesLog48h
      .filter((e) => e.timestamp < cutoff24h)
      .map((e) => ({
        tokenIndex: tokenIdToIndex(e.tokenId),
        type: 'sale',
        timestamp: e.timestamp
      }))
  });
});

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

client.onItemReceivedOffer(COLLECTION_SLUG, (event) => {
  const tokenId = extractTokenId(event.payload.item.nft_id);
  markStreamEvent('offer', tokenId, event.payload.event_timestamp);
  broadcastEvent(tokenId, 'offer');
});

// REMOVED (2026-08-24): onItemReceivedBid ("accepted offer") is gone
// for good. OpenSea's stream API has no way to tell apart a real ETH
// sale at the listed price from a WETH offer the owner accepted -
// both fire as different events but there's no reliable signal to
// split them into their own trustworthy category, client- or
// server-side. Rather than keep a bucket that would be silently
// wrong for some events, "accepted offer" was dropped entirely: no
// subscription, no color, no legend entry (see config.js).
//
// NOTE (2026-08-24): onItemListed (listings) and onItemTransferred
// (mint/transfer) subscriptions were removed on purpose. Monitored
// activity is now exactly 2 categories - sale, new offer - see
// EVENT_COLORS/EVENT_LABELS in config.js. A listing or a transfer no
// longer pulses the cube, no longer appears in the legend, and is no
// longer polled below.

console.log(`Ecoute des evenements OpenSea pour ${COLLECTION_SLUG}...`);

// -----------------------------------------------------------------
// REST POLLING BACKSTOP
// Independent of the stream above: periodically re-checks OpenSea's
// own collection-wide history endpoint and broadcasts anything not
// already seen. This is what actually fixes "a sale happened and the
// cube never showed it" - if the stream was disconnected (or this
// whole process was asleep on a free Render dyno) at that moment, the
// stream can never recover that event, but this poll picks it up on
// its next run regardless.
//
// SPLIT CADENCE (fixes GU#1897 sold same-day, never showing up):
// sales are checked on their own, dedicated 15-minute cycle; every
// other monitored activity (new listing, new offer, mint/transfer)
// is checked on a separate 30-minute cycle. Two consequences of that
// split, both deliberate:
//   - a sale query with event_type=sale is a smaller, cheaper request
//     that isn't competing/paginated against listing/offer/transfer
//     noise, so it's less likely to silently truncate at limit=50
//     and drop a real sale off the page during a busy window.
//   - each cycle's lookback window overlaps its own previous run by
//     a fixed buffer (OVERLAP_SECONDS) instead of starting exactly
//     where the last run ended - a process that stalls, GCs, or is
//     mid-restart for even a few seconds right at the boundary can
//     otherwise leave a 1-timestamp gap that a sale silently falls
//     into. seenEventKeys dedupes the resulting overlap so nothing
//     is broadcast twice.
// -----------------------------------------------------------------
const SEEN_KEYS_CAP = 500;
const seenEventKeys = new Set();

function markSeen(key) {
  seenEventKeys.add(key);
  if (seenEventKeys.size > SEEN_KEYS_CAP) {
    seenEventKeys.delete(seenEventKeys.values().next().value);
  }
}

const SALE_POLL_INTERVAL_MS = 15 * 60 * 1000;   // ventes: verifiees toutes les 15 min
const OFFER_POLL_INTERVAL_MS = 30 * 60 * 1000;  // nouvelles offres: verifiees toutes les 30 min
const OVERLAP_SECONDS = 5 * 60; // marge de recouvrement anti-trou en bordure d'intervalle

// Builds a collection-events URL for one or several OpenSea
// event_type values (repeated query param, as the API expects).
function buildEventsUrl(since, eventTypes) {
  const params = new URLSearchParams({ after: String(since), limit: '50' });
  eventTypes.forEach((t) => params.append('event_type', t));
  return `https://api.opensea.io/api/v2/events/collection/${COLLECTION_SLUG}?${params.toString()}`;
}

// Only these OpenSea event_type values map onto the site's legend
// (sale / new offer - see config.js EVENT_COLORS).
// "listing", "transfer", "cancel", "redemption" and any unrecognized
// raw "order" have no bucket in the UI anymore and are ignored here -
// removed on purpose so a listing or a mint/transfer can never make a
// voxel blink again.
function mapEventType(openseaEventType) {
  switch (openseaEventType) {
    case 'sale': return 'sale';
    case 'offer': return 'offer';
    default: return null; // listing, transfer, cancel, redemption, order: no UI bucket
  }
}

async function pollEvents(eventTypes, sinceState, label) {
  const since = sinceState.after - OVERLAP_SECONDS;
  sinceState.after = Math.floor(Date.now() / 1000);
  try {
    const res = await fetch(buildEventsUrl(since, eventTypes), {
      headers: { 'x-api-key': OPENSEA_API_KEY }
    });
    if (!res.ok) throw new Error(`${label} poll HTTP ${res.status}`);
    const data = await res.json();
    for (const ev of (data.asset_events || [])) {
      const type = mapEventType(ev.event_type);
      const tokenId = ev.nft?.identifier;
      if (!type || !tokenId) continue;
      const key = `${ev.event_type}:${tokenId}:${ev.event_timestamp}`;
      if (seenEventKeys.has(key)) continue;
      markSeen(key);
      // Use the event's own OpenSea timestamp (when the sale/offer
      // actually happened), not "now" (when this poll noticed it) -
      // so /recent-events and the ring buffer both reflect real
      // event time, which matters for a backfilled event that could
      // be hours old.
      const tsMs = ev.event_timestamp ? Date.parse(ev.event_timestamp) : Date.now();
      broadcastEvent(tokenId, type, Number.isFinite(tsMs) ? tsMs : Date.now());
    }
  } catch (err) {
    console.error(`[relay] ${label} poll failed:`, err.message || err);
  }
}

// Initial lookback widened from 1h to 24h (2026-08-24): on every
// boot/wake (including after a free-tier Render cold start), this
// makes the very first poll backfill a full day of sales/offers
// instead of just the last hour - which is what actually powers "see
// every sale / new offer from the last 24h stay lit",
// since the live stream alone can only ever show events for the
// exact time window this process happens to be awake and connected.
const salesPollState = { after: Math.floor(Date.now() / 1000) - DAY_MS / 1000 };
const offersPollState = { after: Math.floor(Date.now() / 1000) - DAY_MS / 1000 };

function pollSales() {
  return pollEvents(['sale'], salesPollState, 'sales');
}

function pollOffers() {
  return pollEvents(['offer'], offersPollState, 'offers');
}

pollSales();   // catch up on ventes immediatement au demarrage/reveil
pollOffers();  // idem pour les nouvelles offres

setInterval(pollSales, SALE_POLL_INTERVAL_MS);
setInterval(pollOffers, OFFER_POLL_INTERVAL_MS);

// -----------------------------------------------------------------
// TRAIT RARITY (collection-wide)
// The per-NFT endpoint used below (GET /nfts/{identifier}) returns
// each trait's type/value but never a count or a rarity % against
// the collection - that data only exists on OpenSea's separate
// collection-traits endpoint. Without this, the panel had traits but
// no rarity %, unlike the OpenSea page for the same token.
//
// GET /api/v2/traits/{slug} returns { categories, counts } where
// counts is { trait_type: { value: numberOfNftsWithThatValue } } -
// exactly the shape normalizeTraits() in events.js already expects
// as "collectionTraits" (it divides count by TOTAL_TOKENS to get %).
//
// Cached with a 1h TTL: these counts only change on a reveal, a burn,
// or new mints - never on every panel open - so refetching per
// request would just be wasted calls against the rate limit. On a
// failed refresh, the previous (even if stale) cached counts keep
// being served rather than wiping out rarity data for every open
// panel over one transient OpenSea hiccup.
// -----------------------------------------------------------------
const TRAIT_STATS_TTL_MS = 60 * 60 * 1000;
let traitStatsCache = { counts: null, fetchedAt: 0 };

async function getCollectionTraitCounts() {
  const isFresh = traitStatsCache.counts && (Date.now() - traitStatsCache.fetchedAt) < TRAIT_STATS_TTL_MS;
  if (isFresh) return traitStatsCache.counts;
  try {
    const res = await fetch(`https://api.opensea.io/api/v2/traits/${COLLECTION_SLUG}`, {
      headers: { 'x-api-key': OPENSEA_API_KEY }
    });
    if (!res.ok) {
      // Surface the response body too, not just the status: a 401/403
      // here usually means the API key's tier doesn't include this
      // endpoint, which a bare status code doesn't make obvious.
      const body = await res.text().catch(() => '');
      throw new Error(`traits HTTP ${res.status}${body ? ` - ${body.slice(0, 300)}` : ''}`);
    }
    const data = await res.json();
    if (data && data.counts) {
      traitStatsCache = { counts: data.counts, fetchedAt: Date.now() };
      console.log(`[relay] collection traits refreshed: ${Object.keys(data.counts).length} trait categories cached.`);
    } else {
      console.warn('[relay] collection traits response had no "counts" field:', JSON.stringify(data).slice(0, 300));
    }
  } catch (err) {
    console.error('[relay] collection traits fetch failed:', err.message || err);
  }
  return traitStatsCache.counts;
}
getCollectionTraitCounts(); // warm the cache at boot instead of on the first click

// -----------------------------------------------------------------
// HISTORIQUE D'UN TOKEN (appele au clic sur la page)
// -----------------------------------------------------------------
app.get('/token/:index', async (req, res) => {
  const tokenIndex = Number(req.params.index);
  const tokenId = tokenIndex + 1; // inverse de tokenIdToIndex, meme hypothese

  let nftData = null;
  let eventsData = { asset_events: [] };
  // MAJOR BUG FIXED HERE: a failed events request (bad HTTP status,
  // network error, OpenSea rate-limit, timeout...) used to fall back
  // to `{ asset_events: [] }` with only a server-side console.error -
  // completely indistinguishable, from the client's point of view,
  // from a token that genuinely has zero history. That is exactly
  // why the panel kept showing "no activity" even for GUs with real
  // sales/offers: any hiccup on this one request silently erased the
  // history for every single token until the next successful call.
  // eventsError now travels all the way to the browser (events.js
  // reads it) so the panel can tell "really nothing yet" apart from
  // "couldn't fetch it right now" instead of conflating the two.
  let eventsError = null;

  try {
    const nftRes = await fetch(
      `https://api.opensea.io/api/v2/chain/ethereum/contract/${CONTRACT_ADDRESS}/nfts/${tokenId}`,
      { headers: { 'x-api-key': OPENSEA_API_KEY } }
    );
    if (!nftRes.ok) {
      console.error(`[relay] /token/${tokenIndex}: NFT metadata fetch HTTP ${nftRes.status}`);
    } else {
      nftData = await nftRes.json();
    }
  } catch (err) {
    console.error(`[relay] /token/${tokenIndex}: NFT metadata fetch failed:`, err.message || err);
  }

  // Correct per-NFT endpoint: /events/chain/{chain}/contract/{address}/nfts/{identifier}
  // (the contract-wide events endpoint has no per-token filter, so it
  // must never be used here - see git history for that earlier bug).
  try {
    const eventsRes = await fetch(
      `https://api.opensea.io/api/v2/events/chain/ethereum/contract/${CONTRACT_ADDRESS}/nfts/${tokenId}?limit=10`,
      { headers: { 'x-api-key': OPENSEA_API_KEY } }
    );
    if (!eventsRes.ok) {
      eventsError = `HTTP ${eventsRes.status}`;
      console.error(`[relay] /token/${tokenIndex}: events fetch HTTP ${eventsRes.status}`);
    } else {
      eventsData = await eventsRes.json();
    }
  } catch (err) {
    eventsError = err.message || String(err);
    console.error(`[relay] /token/${tokenIndex}: events fetch failed:`, err.message || err);
  }

  const collectionTraits = await getCollectionTraitCounts();

  // Forward the raw material instead of pre-digesting it into "rows":
  // the client (events.js normalizeHistory/normalizeTraits) already
  // builds the history summary AND the traits grid from exactly this
  // shape (nft.traits + asset_events), and now also gets eventsError
  // to tell a real failure apart from genuinely-empty history, plus
  // collectionTraits so normalizeTraits() can attach a rarity % to
  // each trait card instead of leaving it blank.
  res.json({
    imageUrl: nftData?.nft?.image_url || null,
    traits: nftData?.nft?.traits || [],
    events: eventsData.asset_events || [],
    eventsError,
    collectionTraits
  });
});
