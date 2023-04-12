# Associate oil slick detections to AIS trajectories
Refer to this [document](https://docs.google.com/document/d/1jDgz9x-elC_jxk7SiicrU8iGPtPQHJ9GOdxA9JTEdzw) for a description of the algorithm.

## Example notebook
The `slick_to_ais.ipynb` notebook should be used, which illustrates how to run a single sample through the association pipeline.

## Leaflet app
The `slick_explorer.ipynb` is a notebook for viewing samples from the initial provided dataset in a leaflet map application. The class definition is implemented in `slickmap.py`.

## Utilities
A brief description of the utilities is below:

- `associate.py` - functions to associate oil slicks to trajectories
- `constants.py` - constants used throughout the algorithm
- `gee.py` - google earth engine helpers for the leaflet app
- `misc.py` - miscellaneous functions that didn't belong anywhere else
- `scoring.py` - functions for calculating various metrics used for scoring
- `trajectory.py` - functions for working with AIS trajectories
