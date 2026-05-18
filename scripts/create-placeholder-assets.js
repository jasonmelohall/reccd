#!/usr/bin/env node
/**
 * Creates valid placeholder PNG images for Expo assets
 * Uses a base64-encoded minimal valid PNG
 */

const fs = require('fs');
const path = require('path');

// Base64-encoded minimal valid 1x1 transparent PNG
const minimalPNGBase64 = 'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==';
const minimalPNG = Buffer.from(minimalPNGBase64, 'base64');

const assetsDir = path.join(__dirname, '..', 'assets');
const assets = ['icon.png', 'splash.png', 'adaptive-icon.png', 'favicon.png'];

// Ensure assets directory exists
if (!fs.existsSync(assetsDir)) {
  fs.mkdirSync(assetsDir, { recursive: true });
}

// Create or replace placeholder images
assets.forEach(asset => {
  const assetPath = path.join(assetsDir, asset);
  fs.writeFileSync(assetPath, minimalPNG);
  console.log(`Created/updated placeholder: ${asset}`);
});

console.log('Placeholder assets ready');

