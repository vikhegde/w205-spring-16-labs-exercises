#!/usr/bin/env python
# coding: utf-8

# In[7]:

import time
import pandas as pd
import imp


from ml_pipeline import *


# In[8]:

MLRoot.init_storage(password="xypostgres", host="localhost", port="5432")

myname = "run_sample_pipeline"
state_file = myname + ".state." + str(time.time())

myML = MLRoot()
myML.mount(mount_spec="./mnttab_sample_pipeline.json")
myML.print_tree()
input_data = {}
input_data["remote_loc"] = ""
myML.compile(json.dumps(input_data))
input_data["remote_loc"] = "./sample_data.zip"
# Max data size we want is 1 GB
myML.setprop("/root/fetch", {"max_size" : 1})
myML.setprop("/root/derive/extract_features", {"data_name" : myname})
myML.run(save=True, input_data=input_data, state=state_file)
myML.umount()


# In[ ]:

myML2 = MLRoot()
myML2.run(save=False, resume="/root/model", state=state_file)

