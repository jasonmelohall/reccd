# Reccd Mobile App

Cross-platform app (iOS, Android, Web) for personalized Amazon product recommendations.

## Quick Start

```bash
cd reccd-app
npm install
npx expo install react-native-web react-dom @expo/webpack-config

# Start web version
npm run web
```

Opens at http://localhost:19006

## Run on Phone

```bash
npm start  # Shows QR code
```

1. Install **Expo Go** app on your phone
2. Scan QR code with Camera (iOS) or Expo Go (Android)
3. App opens on your phone!

## Configuration

Edit `src/config.js` to point to your backend:
```javascript
export const API_BASE_URL = 'http://localhost:8000'  // Local dev
```

## How It Works

1. User enters search term
2. Backend checks database:
   - **Has results**: Show immediately + refresh in background
   - **No results**: Show "Analyzing... 2-3 minutes" message
3. Backend runs full pipeline (all 5 Python scripts)
4. Results auto-refresh every 10 seconds until ready
5. User sees personalized rankings with:
   - Product images
   - Price, rating, reviews
   - Release date, frequency, search rank
   - Reccd score
6. Tap product → Opens Amazon app

## Features

- Amazon-style product grid (2 columns)
- Pull-to-refresh
- Auto-polling when processing
- Deep linking to Amazon app
- Works on iOS, Android, Web

## Troubleshooting

**"Failed to search" error**:
- Ensure backend is running: http://localhost:8000/docs
- Check `src/config.js` has correct API URL

**No results after waiting**:
- Pull down to refresh
- Check backend terminal for errors
- Pipeline takes 2-3 minutes

**Images not showing**:
- Images load from Amazon CDN
- Check internet connection
- Verify image_url in API response

## Next Steps

- Test on physical device with Expo Go
- Add more search terms
- Deploy to production (see GETTING_STARTED.md)



