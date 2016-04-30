#!/usr/bin/env python
# coding: utf-8

# In[811]:

import time

from ml_pipeline import *


# # Code to process MNIST data follows this cell

# In[ ]:

# # Bernoulli Naive Bayes on the MNIST dataset

# In[91]:

nsamples_list = [5, 10, 20, 50, 100, 250, 500, 1000, 5000, 10000, 20000, 30000, 40000]
MLRoot.init_storage(password="xypostgres", host="localhost", port="5432")

myML = MLRoot()
myML.mount(mount_spec = "./mnttab_mnist_bernoulli_NB.json")
myML.print_tree()
input_data = {}
input_data["remote_loc"] = ""
myML.compile(json.dumps(input_data))
input_data["remote_loc"] = "./mnist_original.zip"
myML.setprop("/root/fetch", {"max_size" : 1000})
myML.setprop("/root/derive/mnist_extract", {"data_name" : "bernNB"})
myML.setprop("/root/model/mnist_bernNB", {"nsamples_list" : nsamples_list})
myML.run(save=False, input_data=input_data)
myML.umount()


# # Logistic Regression on the MNIST dataset

# In[ ]:

myML = MLRoot()
myML.mount(mount_spec = "./mnttab_mnist_logistic.json")
myML.print_tree()
input_data = {}
input_data["remote_loc"] = ""
myML.compile(json.dumps(input_data))
input_data["remote_loc"] = "./mnist_original.zip"
myML.setprop("/root/fetch", {"max_size" : 1000})
myML.setprop("/root/derive/mnist_extract", {"data_name" : "Logistic"})
myML.setprop("/root/model/mnist_logistic", {"nsamples_list" : nsamples_list})
myML.run(save=False,input_data=input_data)
myML.umount()

MLRoot.destroy_storage()

