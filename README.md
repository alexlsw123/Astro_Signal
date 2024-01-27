# Astro_Signal
## Using Spark to mine Astro signals

## Data

The data contains a series of signals stored as 

| ascension (degrees) | declination (degrees) | time (seconds) | frequency (MHz) |
| --- | --- | --- | --- |
| Stretched | Gaussian | .843 | sadfs |

These are radiofrequency signals captured by an array of instruments scanning a solid angle of the sky.

The data is, like almost all real data, a little noisy and has sources of errors. In our case the angular
coordinates have 0.1 degrees error, the signal frequency has 0.1 MHZ error and the timestamp error is < 0.01s (that is one STD or standard deviation).

## Task

### Finding the repeating cosmological signal (pulsar) in the captured data
My job is to find the source with the most signals (that is "blips", or entries in the datalog)
