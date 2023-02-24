"""
Map built on Leaflet in order to visualize slick datasets
"""

from datetime import datetime, timedelta
from glob import glob
import json
import os
import random

import geopandas as gpd
import ipyleaflet as ipyl
import ipywidgets as ipyw
import matplotlib.colors
import matplotlib.pyplot as plt
import pandas as pd

from utils import ais_points_to_lines, get_s1_tile_layer

# define dataset directories
DATA_DIR = '/home/k3blu3/datasets/cerulean'
AIS_DIR = os.path.join(DATA_DIR, '19_ais')
SLICK_DIR = os.path.join(DATA_DIR, '19_vectors')

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
            hover_style={
                "color": "white",
                "dashArray": "0",
                "fillOpacity": 0.5,
                "weight": 2
            },
            style_callback=random_color
        )
        
        self.slick_layer = ipyl.GeoJSON(
            name="Slick",
            style={
                "color": "white",
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

        self.map.add_layer(self.footprint_layer)
        self.map.add_layer(self.ais_layer)
        self.map.add_layer(self.slick_layer)
        self.map.add_layer(self.s1_layer)

    def _load_dataset(self):
        ais_files = glob(os.path.join(AIS_DIR, '*.geojson'))
        slick_files = glob(os.path.join(SLICK_DIR, '*.geojson'))

        ais = pd.DataFrame(ais_files).rename(columns={0: 'fname'})
        slick = pd.DataFrame(slick_files).rename(columns={0: 'fname'})

        ais['basename'] = ais['fname'].apply(lambda x: os.path.splitext(os.path.basename(x))[0])
        slick['basename'] = slick['fname'].apply(lambda x: os.path.splitext(os.path.basename(x))[0])

        df = ais.merge(slick, on='basename', how='inner', suffixes=('_ais', '_slick'))
        df = df[['basename', 'fname_ais', 'fname_slick']]

        self.df = df
        self.ctr = 0 # start at the beginning of the dataset

    def _load_sample(self, ctr: int):
        self.basename = self.df['basename'].iloc[ctr]
        self.ais = gpd.read_file(self.df['fname_ais'].iloc[ctr])
        self.slick = gpd.read_file(self.df['fname_slick'].iloc[ctr])

        # extract time of collection
        start_time = self.basename.split('_')[4]
        start_time = datetime.strptime(start_time, '%Y%m%dT%H%M%S')
        self.collect_time = start_time

        # convert ais points to lines
        self.ais_lines = ais_points_to_lines(self.ais)

        # pull S1 tile layer around this collection
        s1_url, s1_footprint = get_s1_tile_layer(self.collect_time, self.basename)
        self.s1_layer.url = s1_url
        self.s1_layer.redraw()

        # update vector layers
        self.ais_layer.data = json.loads(self.ais_lines.to_json())
        self.slick_layer.data = json.loads(self.slick.to_json())
        self.footprint_layer.data = s1_footprint

        # update date display
        self.date_display.value = self.collect_time.strftime('%Y-%m-%d')

        # center map on the slick
        self.map.center = self.slick.dissolve().centroid[0].coords[0][::-1]

    def _build_data_controls(self):
        def next_sample(b):
            if self.ctr <= len(self.df):
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

        self.next_button = ipyw.Button(
            description='',
            disabled=False,
            button_style='info',
            tooltip='Go to the next sample',
            icon='arrow-right',
            layout=ipyw.Layout(width='80px')
        )
        self.next_button.on_click(next_sample)

        self.prev_button = ipyw.Button(
            description='',
            disabled=False,
            button_style='info',
            tooltip='Go to the previous sample',
            icon='arrow-left',
            layout=ipyw.Layout(width='80px')
        )
        self.prev_button.on_click(prev_sample)

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








        


