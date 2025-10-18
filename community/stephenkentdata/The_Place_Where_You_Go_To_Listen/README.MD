<!--fused:readme-->
# The Place Where You Go to Listen

An interactive sonification of real-time geophysical data, inspired by John Luther Adams' installation at the University of Alaska Museum of the North.

## About

This web application transforms live Earth data into continuous sound and color:

- **Seismic activity** from earthquakes detected by USGS
- **Aurora intensity** from NOAA's geomagnetic monitoring
- **Solar position** based on time of day and location
- **Seasonal cycles** throughout the year

## How It Works

**Sound:**
- Deep bass (15-40 Hz): Local earthquakes within 500km
- Low rumble (60-120 Hz): Regional earthquakes within 2000km  
- Mid-range tone (200-500 Hz): Largest earthquake globally
- Harmonics (120-280 Hz): Aurora borealis/australis activity
- Filtered noise: Solar brightness and atmospheric conditions

**Visual:**
- Single color field that shifts based on all data combined
- Warm tones during day, cooler at night
- Blue shifts during aurora activity
- Red intensity during seismic events

## Usage

1. Select a location from the dropdown menu
2. Click "Listen" to start the experience
3. Data updates automatically every 60 seconds
4. Click "Stop" to pause

## Credits

Inspired by [John Luther Adams](https://www.johnlutheradams.net/)' "[The Place Where You Go to Listen](https://www.uaf.edu/centennial/uaf100/ideas/the-place.php)"

Data sources: USGS Earthquake API, NOAA Space Weather

Built with Fused.io
