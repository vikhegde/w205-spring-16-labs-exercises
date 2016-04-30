#!/usr/bin/env python
# coding: utf-8

# In[811]:

from ml_pipeline import *


# # Code for processing Million Song dataset follows

# In[ ]:

nsamples_list = [5, 10, 20, 50, 100, 250, 500, 1000, 5000, 10000, 20000, 30000, 40000,50000, 75000, 100000]
#nsamples_list = [5, 10, 20, 50, 100, 250, 500]
MLRoot.init_storage(password="xypostgres", host="localhost", port="5432")

# # Gaussian Naive Bayes on Million Song Dataset

# In[ ]:

import time

start_time = time.time()

feature_list = [
			'danceability',
			'loudness',
			'energy',
			'tempo',
			'end_fade_in',
			'start_fade_out'
		]
label_list = ['year']
myML = MLRoot()
myML.mount(mount_spec = "./mnttab_millsong_gauss_NB.json")
myML.print_tree()
input_data = {}
input_data["remote_loc"] = ""
myML.compile(json.dumps(input_data))
input_data["remote_loc"] = "./million_song_dir"
# Set maximum size of data we want to process: 100 GB
myML.setprop("/root/fetch", {"max_size" : 100}) 
# Maximum number of files to process: 300,000
myML.setprop("/root/fetch/raw/collect_h5", {"max_process" : 300000})
#myML.setprop("/root/fetch/raw/collect_h5", {"max_process" : 2000})
# Maximum number of examples (training+test) to collect: 150,000
myML.setprop("/root/fetch/raw/collect_h5", {"nsongs" : 150000})
#myML.setprop("/root/fetch/raw/collect_h5", {"nsongs" : 1000})
myML.setprop("/root/derive/millsong_extract", {"train_frac" : 0.7})
myML.setprop("/root/derive/millsong_extract", {"X_map" : feature_list})
myML.setprop("/root/derive/millsong_extract", {"y_map" : label_list})
myML.setprop("/root/derive/millsong_extract", {"data_name" : "millgaussNB"})
myML.setprop("/root/model/gauss_nb", {"nsamples_list" : nsamples_list})
myML.run(save=False, input_data=input_data)
myML.umount()
dprint(DPRINT_INFO, "Total Time taken: " + str(time.time() - start_time))


# # Logistic Regression on Million Song Dataset

# In[ ]:

import time

start_time = time.time()

feature_list = [
			'danceability',
			'loudness',
			'energy',
			'tempo',
			'end_fade_in',
			'start_fade_out'
		]
label_list = ['year']
myML = MLRoot()
myML.mount(mount_spec = "./mnttab_millsong_logistic.json")
myML.print_tree()
input_data = {}
input_data["remote_loc"] = ""
myML.compile(json.dumps(input_data))
input_data["remote_loc"] = "./million_song_dir"
# Set maximum size of data we want to process: 100 GB
myML.setprop("/root/fetch", {"max_size" : 100}) 
# Maximum number of files to process: 300,000
myML.setprop("/root/fetch/raw/collect_h5", {"max_process" : 300000})
#myML.setprop("/root/fetch/raw/collect_h5", {"max_process" : 2000})
# Maximum number of examples (training+test) to collect: 150,000
myML.setprop("/root/fetch/raw/collect_h5", {"nsongs" : 150000})
#myML.setprop("/root/fetch/raw/collect_h5", {"nsongs" : 1000})
myML.setprop("/root/derive/millsong_extract", {"train_frac" : 0.7})
myML.setprop("/root/derive/millsong_extract", {"X_map" : feature_list})
myML.setprop("/root/derive/millsong_extract", {"y_map" : label_list})
myML.setprop("/root/derive/millsong_extract", {"data_name" : "millLogistic"})
myML.setprop("/root/model/logistic", {"nsamples_list" : nsamples_list})
myML.run(save=False, input_data=input_data)
myML.umount()
dprint(DPRINT_INFO, "Total Time taken: " + str(time.time() - start_time))

MLRoot.destroy_storage()
