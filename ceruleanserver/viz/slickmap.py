"""
Map built on Leaflet in order to visualize slick datasets
"""

from datetime import datetime, timedelta
from glob import glob
import json
import os
import random

import geopandas as gpd
import movingpandas as mpd
import numpy as np
import ipyleaflet as ipyl
import ipywidgets as ipyw
import matplotlib.colors
import matplotlib.pyplot as plt
import pandas as pd

from utils import (ais_points_to_lines,
                   ais_points_to_trajectories,
                   get_s1_tile_layer)

# define dataset directories
DATA_DIR = '/home/k3blu3/datasets/cerulean'
AIS_DIR = os.path.join(DATA_DIR, '19_ais')
SLICK_DIR = os.path.join(DATA_DIR, '19_vectors')
TRUTH_FILE = os.path.join(DATA_DIR, 'slick_truth_year1.csv')

class SlickMap:
    def __init__(self):
        self._create_map()

        self._create_logo()
        
        self._create_vector_layers()

        self._load_dataset()

        self._build_data_controls()

    def _create_map(self):
        map_kwargs = dict()
        
        map_kwargs["zoom"] = 10
        map_kwargs["zoom_control"] = False
        map_kwargs["attribution_control"] = False
        map_kwargs["scroll_wheel_zoom"] = True
        map_kwargs["no_wrap"] = True
        map_kwargs["prefer_canvas"] = True

        basemap = ipyl.basemaps.Esri.WorldImagery
        map_kwargs["basemap"] = basemap

        layout = ipyw.Layout(height='800px')

        self.map = ipyl.Map(layout=layout, **map_kwargs)

    def _create_logo(self):
        logo = ipyw.Image(
            value=open("skytruth_logo.png", "rb").read(),
            format='png',
            width=80,
        )

        logo_wc = ipyl.WidgetControl(widget=logo, position='bottomleft')
        self.map.add_control(logo_wc)

    def _create_vector_layers(self):
        def random_color(feature, **kwargs):
            # pick a random color from a categorical colormap
            cmap = plt.cm.get_cmap('tab20')
            color = matplotlib.colors.rgb2hex(cmap(random.uniform(0, 1)))
            return {
                'color': color,
            }

        self.ais_layer = ipyl.GeoJSON(
            name="AIS",
            style={
                "opacity": 0.8,
                "dashArray": "9",
                "fillOpacity": 0.1,
                "weight": 1.5,
            },
            point_style={
                "radius": 4,
                "weight": 1,
                "opacity": 1,
            },
            style_callback=random_color
        )

        self.truth_layer = ipyl.GeoJSON(
            name="Truth",
            style={
                "color": "white",
                "opacity": 1,
                "dashArray": "0",
                "fillOpacity": 1,
                "weight": 2
            },
        )
        
        self.slick_layer = ipyl.GeoJSON(
            name="Slick",
            style={
                "color": "red",
                "fillColor": "black",
                "opacity": 0.8,
                "fillOpacity": 0.2,
                "weight": 1,
            },
            hover_style={
                "fillColor": "red",
                "fillOpacity": 0.8,
                "opacity": 1,
                "weight": 2
            }
        )

        self.footprint_layer = ipyl.GeoJSON(
            name="Footprint",
            style={
                "color": "yellow",
                "opacity": 0.8,
                "fillOpacity": 0,
                "weight": 1
            }
        )

        self.s1_layer = ipyl.TileLayer(nowrap=True)

        self.map.add_layer(self.s1_layer)
        self.map.add_layer(self.footprint_layer)
        self.map.add_layer(self.ais_layer)
        self.map.add_layer(self.truth_layer)
        self.map.add_layer(self.slick_layer)

    def _load_dataset(self):
        ais_files = glob(os.path.join(AIS_DIR, '*.geojson'))
        slick_files = glob(os.path.join(SLICK_DIR, '*.geojson'))

        ais = pd.DataFrame(ais_files).rename(columns={0: 'fname'})
        slick = pd.DataFrame(slick_files).rename(columns={0: 'fname'})

        ais['basename'] = ais['fname'].apply(lambda x: os.path.splitext(os.path.basename(x))[0])
        slick['basename'] = slick['fname'].apply(lambda x: os.path.splitext(os.path.basename(x))[0])

        df = ais.merge(slick, on='basename', how='inner', suffixes=('_ais', '_slick'))

        # load in truth
        truth = pd.read_csv(os.path.join(DATA_DIR, TRUTH_FILE))
        truth = truth.rename(columns={'PID': 'basename', 'HITL MMSI': 'ssvid_truth'})
        truth = truth.fillna('DARK') # any NaN values are assumed to be DARK

        # merge truth into dataset
        df = df.merge(truth, on='basename', how='inner')
        df = df[['basename', 'fname_ais', 'fname_slick', 'ssvid_truth']]

        self.df = df
        self.ctr = 0 # start at the beginning of the dataset

    def _load_sample(self, ctr: int):
        self.basename = self.df['basename'].iloc[ctr]
        self.ais = gpd.read_file(self.df['fname_ais'].iloc[ctr])
        self.slick = gpd.read_file(self.df['fname_slick'].iloc[ctr])
        self.ssvid_truths = self.df['ssvid_truth'].iloc[ctr].split(',')
        self.ssvid_truths = [s.strip() for s in self.ssvid_truths]

        # extract time of collection
        start_time = self.basename.split('_')[4]
        start_time = datetime.strptime(start_time, '%Y%m%dT%H%M%S')
        self.collect_time = start_time

        # create a time vector that we will interpolate the AIS tracks on
        self.time_vec = pd.date_range(
            self.collect_time - timedelta(hours=4),
            self.collect_time,
            periods=20
        )

        # get the best UTM zone for this sample, and reproject
        self.utm_zone = self.ais.estimate_utm_crs()
        self.ais = self.ais.to_crs(self.utm_zone)
        self.slick = self.slick.to_crs(self.utm_zone)

        # get trajectories from AIS data
        self.ais_trajectories = ais_points_to_trajectories(self.ais, self.time_vec)

        # get interpolated trajectories as gdf
        # this is what we will use to plot the AIS tracks
        # also get the truth trajectories if they exist
        self.ais_gdf = self.ais_trajectories.to_traj_gdf()
        self.truth_gdf = self.ais_gdf[self.ais_gdf['traj_id'].isin(self.ssvid_truths)]
    
        # pull S1 tile layer around this collection
        s1_url, s1_footprint = get_s1_tile_layer(self.collect_time, self.basename)
        self.s1_layer.url = s1_url
        self.s1_layer.redraw()

        # update vector layers
        self.footprint_layer.data = s1_footprint
        self.ais_layer.data = json.loads(self.ais_gdf.to_crs('EPSG:4326').geometry.to_json())
        self.truth_layer.data = json.loads(self.truth_gdf.to_crs('EPSG:4326').geometry.to_json())
        self.slick_layer.data = json.loads(self.slick.to_crs('EPSG:4326').geometry.to_json())

        # update date display
        self.date_display.value = self.collect_time.strftime('%Y-%m-%d')

        # center map on the slick
        self.map.center = self.slick.dissolve().centroid.to_crs('EPSG:4326').iloc[0].coords[0][::-1]

    def _build_data_controls(self):
        def next_sample(b):
            if self.ctr < len(self.df):
                self.ctr += 1
                self.data_progress.value = self.ctr + 1
                self.data_progress.description = f"{self.ctr+1}/{len(self.df)}"
                self._load_sample(self.ctr)

        def prev_sample(b):
            if self.ctr > 0:
                self.ctr -= 1
                self.data_progress.value = self.ctr + 1
                self.data_progress.description = f"{self.ctr+1}/{len(self.df)}"
                self._load_sample(self.ctr)

        def random_sample(b):
            self.ctr = np.random.randint(0, len(self.df) - 1)
            self.data_progress.value = self.ctr + 1
            self.data_progress.description = f"{self.ctr+1}/{len(self.df)}"
            self._load_sample(self.ctr)

        self.next_button = ipyw.Button(
            description='',
            disabled=False,
            button_style='info',
            tooltip='Go to the next sample',
            icon='arrow-right',
            layout=ipyw.Layout(width='50px')
        )
        self.next_button.on_click(next_sample)

        self.prev_button = ipyw.Button(
            description='',
            disabled=False,
            button_style='info',
            tooltip='Go to the previous sample',
            icon='arrow-left',
            layout=ipyw.Layout(width='50px')
        )
        self.prev_button.on_click(prev_sample)

        self.random_button = ipyw.Button(
            description='',
            disabled=False,
            button_style='info',
            tooltip='Go to a random sample',
            icon='random',
            layout=ipyw.Layout(width='50px')
        )
        self.random_button.on_click(random_sample)

        self.data_progress = ipyw.IntProgress(
            value=self.ctr + 1,
            min=1,
            max=len(self.df),
            description=f"{self.ctr+1}/{len(self.df)}",
            bar_style='info',
            orientation='horizontal',
            layout=ipyw.Layout(width='200px')
        )

        self.date_display = ipyw.HTML()

        self.data_pane = ipyw.VBox(
            [
                ipyw.HBox(
                    [
                        self.random_button,
                        self.prev_button,
                        self.next_button
                    ],
                ),
                self.data_progress,
                self.date_display
            ]
        )
        self.data_pane.layout = ipyw.Layout(align_items='flex-end')

        self.data_control = ipyl.WidgetControl(
            widget=self.data_pane,
            position='bottomright'
        )

        self.map.add_control(self.data_control)
        self._load_sample(self.ctr)








        


