# SkyTruth Automatic AIS Analysis

This repo is a place to develop code that will run on an EC2 instance in AWS cloud.

The code pre-processes a POSTed image, operates a trained ML model on it, then some post-processing and finally outputs it to a postgresql db.



Always prefer "conda install" to "pip install"
conda update -n base -c defaults conda
conda create --clone python3 --name python3plus
source activate python3plus
conda install -y --file requirements.txt
???install fastai

start the app like this:
python code/anyserver.py 
