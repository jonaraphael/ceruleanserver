#%%
# Inspiration from: https://colab.research.google.com/github/google/earthengine-api/blob/master/python/examples/ipynb/ee-api-colab-setup.ipynb#scrollTo=NMp9Ei9b0XXL
# !mamba install -c conda-forge earthengine-api
import IPython
import ee
import geemap.foliumap as geemap
ee.Initialize() # Initialize the library.

infraStyles = {
    "oil": {"pointShape": "circle", "color":"F008", "pointSize":2, "width":.1 },
    "wind": {"pointShape": "circle", "color":"0F08", "pointSize":2, "width":.1 },
    "other": {"pointShape": "circle", "color":"FF08", "pointSize":2, "width":.1 },
    }

def set_style(feature): return feature.set('style_params', ee.Dictionary(infraStyles).get(feature.get('label')))
    
def context_map(pids):
    Map = geemap.Map() # Create a map https://github.com/giswqs/geemap
    for pid in pids:
        try:
            ee_img = ee.Image(f'COPERNICUS/S1_GRD/{pid}').select('VV')
            Map.addLayer(name=pid, ee_object=ee_img, vis_params = {'min': -30,'max': 1,})
            Map.centerObject(ee_img, 7)
            Map.addLayer(name=f'Inf-{pid}', shown=True, ee_object=geemap.geojson_to_ee(f"/Users/jonathanraphael/git/ceruleanserver/local/temp/outputs/vectors/{pid}.geojson"), vis_params={'color': 'red', 'opacity': ee.Number(.2)})
        except:
            print(f"{pid} missing")
    Map.add_wms_layer(name='Vessel Density', shown=False, url="https://gmtds.maplarge.com/ogc/ais:density/wms?", layers = ['ais:density']) # https://maplarge-public.s3.us-east-1.amazonaws.com/UserGuides/GMTDS_Technical_Integration_Guide.pdf        
    Map.addLayer(name='Infrastructure', shown=False, ee_object=ee.FeatureCollection("projects/cerulean-338116/assets/GFW_Infra").map(set_style).style(styleProperty='style_params')) # Syling parameters: https://developers.google.com/earth-engine/apidocs/ee-featurecollection-style
    Map.addLayer(name='FPSOs', shown=True, ee_object=geemap.geojson_to_ee("/Users/jonathanraphael/git/ceruleanserver/local/aux_files/Most recent FPSOs.geojson").style(pointShape="s", color="0FFB", pointSize=5, width=.1))
    Map.addLayer(name='Leaky Infrastructure', shown=True, ee_object=geemap.geojson_to_ee("/Users/jonathanraphael/git/ceruleanserver/local/aux_files/Global Coincident Infrastructure.geojson").style(pointShape="cross", color="F00F", pointSize=5, width=.01))
    IPython.display.clear_output()
    display(Map)
# %%
# pid_groups = [
#     ["S1A_IW_GRDH_1SDV_20200804T045214_20200804T045239_033752_03E97F_88D3"]
# ]

# for pids in pid_groups:
#     context_map(pids)


    
#%%
        # Experimental for inference:
        # geemap.ee_to_numpy(ee_img) # maximum chip size is 512x512...
        # geemap.ee_export_image(ee_img, '/Users/jonathanraphael/git/ceruleanserver/local/temp/outputs/test.tif', scale=180)


# https://gis.stackexchange.com/questions/199382/adding-wms-layer-in-qgis-using-python
# https://spatialthoughts.com/2020/04/04/ndvi-time-series-gee-qgis/
# https://docs.qgis.org/3.22/en/docs/pyqgis_developer_cookbook/loadlayer.html

# from qgis.core import QgsApplication, QgsProject, QgsPathResolver
# from qgis.gui import QgsLayerTreeMapCanvasBridge
# from ee_plugin import Map

# # Supply path to qgis install location
# QgsApplication.setPrefixPath('/Applications/QGIS3.8.app/Contents/MacOS', True)

# # Create a reference to the QgsApplication.  Setting the
# # second argument to False disables the GUI.
# qgs = QgsApplication([], True)

# # Load providers
# qgs.initQgis()

# # Write your code here to load some layers, use processing
# # algorithms, etc.
# project = QgsProject.instance()
# project.read('/Users/jonathanraphael/Desktop/Contracting Files/SkyTruth/infrastructure_context.qgz')
# print(project.fileName())


# # Finally, exitQgis() is called to remove the
# # provider and layer registries from memory
# qgs.exitQgis()

# #%%
# import sys
# sys.path.insert(0, '/Users/jonathanraphael/Library/Application Support/QGIS/QGIS3/profiles/default/python/plugins/ee_plugin/')
# from ee_plugin import *


# # import qgis
# # import importlib.util
# # import sys
# # spec = importlib.util.spec_from_file_location("ee_plugin", "/Users/jonathanraphael/Library/Application Support/QGIS/QGIS3/profiles/default/python/plugins/ee_plugin/ee_plugin.py")
# # ee_plugin = importlib.util.module_from_spec(spec)
# # sys.modules["ee_plugin"] = ee_plugin
# # spec.loader.exec_module(ee_plugin)
# # ee_plugin.Map()


# # %%
