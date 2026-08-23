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

client.onItemSold(COLLECTION_SLUG, (event) => {
  const tokenId = extractTokenId(event.payload.item.nft_id);
  broadcast({ tokenIndex: tokenIdToIndex(tokenId), type: 'sale' });
});

client.onItemListed(COLLECTION_SLUG, (event) => {
  const tokenId = extractTokenId(event.payload.item.nft_id);
  broadcast({ tokenIndex: tokenIdToIndex(tokenId), type: 'offer' });
});

client.onItemReceivedOffer(COLLECTION_SLUG, (event) => {
  const tokenId = extractTokenId(event.payload.item.nft_id);
  broadcast({ tokenIndex: tokenIdToIndex(tokenId), type: 'offer' });
});

client.onItemReceivedBid(COLLECTION_SLUG, (event) => {
  const tokenId = extractTokenId(event.payload.item.nft_id);
  broadcast({ tokenIndex: tokenIdToIndex(tokenId), type: 'auction' });
});

client.onItemTransferred(COLLECTION_SLUG, (event) => {
  const tokenId = extractTokenId(event.payload.item.nft_id);
  broadcast({ tokenIndex: tokenIdToIndex(tokenId), type: 'transfer' });
});

console.log(`Ecoute des evenements OpenSea pour ${COLLECTION_SLUG}...`);

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

    const rows = (eventsData.asset_events || []).slice(0, 6).map((e) => ({
      label: e.event_type,
      value: e.event_timestamp ? new Date(e.event_timestamp * 1000).toLocaleDateString('fr-FR') : '-'
    }));

    res.json({
      imageUrl: nftData?.nft?.image_url || null,
      rows: rows.length ? rows : [{ label: 'Historique', value: 'aucun evenement recent' }]
    });
  } catch (err) {
    res.status(502).json({ imageUrl: null, rows: [{ label: 'Erreur', value: 'API OpenSea indisponible' }] });
  }
});
