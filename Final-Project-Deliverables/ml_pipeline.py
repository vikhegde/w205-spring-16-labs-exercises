
# coding: utf-8

# In[22]:

import sys
import os
import shutil
import requests
import random
import zipfile
import bz2
import gzip
import tarfile
import pandas as pd
import cStringIO
from matplotlib import pyplot
import numpy as np
import math
import json
import cPickle as pickle
import copy
import time


from abc import ABCMeta, abstractmethod

import pgdb_funcs as db

# NOTE: libmagic doesn't work on 64 bit Windows, so this program cannot be run on a Windows host
try:
    import magic
except ImportError:
    raise ImportError("Python file magic module not installed or you are running Windows.")
   


# In[23]:

# Globals
http_chunksize__ = 8192  # 8K chunks when downloading files via HTTP
random_seed__ = 0
throw_exceptions = True
gunzip_blocksize_ = 1 << 16
randint_limit = 1000000

DPRINT_SILENT = -1
DPRINT_ERROR = 1
DPRINT_WARN = 2
DPRINT_INFO = 3
DPRINT_DEBUG = 4
DPRINT_VERBOSE = 5

# Must match above values
dprint_levels = [-1,1,2,3,4,5]

dprint_strs = {-1 : "SILENT", 1 : "ERROR", 2 : "WARN", 3 : "INFO", 4 : "DEBUG", 5 : "VERBOSE"}


# valid values are "dev", "prod"
version="prod"

if version == "dev":
    dprint_mode = DPRINT_INFO
else:
    dprint_mode = DPRINT_INFO


# In[24]:

random.seed(random_seed__)


# In[25]:

# Utility functions
class UtilException(Exception):
    pass

def safe_rmtree(path):
    if not os.path.exists(path):
        return
    if os.path.isdir(path):
        shutil.rmtree(path)
    elif os.path.isfile(path):
        os.remove(path)
        
def get_file_type(path, mime=True):
    return magic.from_file(path,mime)
        
def debug_raise():
    if throw_exceptions:
        raise
   
# Log messages, available in "prod" and "dev" versions
def dprint(mode, msg):
    if mode is None:
        debug_raise()
    if mode <= dprint_mode:
        sys.stdout.write("(" + dprint_strs[mode] + ")" + msg + "\n")
        sys.stdout.flush()

# Trace messages for testing and development available in "dev" version only
def tprint(msg):
    if version != "dev":
        return
    sys.stdout.write(msg + "\n")
    sys.stdout.flush()


# In[26]:

class MLException(Exception):
    pass


# In[27]:

# Utility functions
def walk_exception(oserror):
    dprint(DPRINT_ERROR, "Error encountered while walking with filesystem object: " + oserror.filename)
    raise oserror


# In[28]:

class MLRoot:
    __metaclass__ = ABCMeta
    db_password = None
    db_host = "localhost"
    db_port = "5432"
    db_tmo = 40000 * 3600  # 40 hours
    
    def __init__(self):
        self.class_name = "MLRoot"
        self.name = None   # set based on mountpoint path
        self.parent = None
        self.young_sib = None
        self.old_sib = None
        self.child = []
        
        # We need the remote location
        input_data = {"remote_loc" : ""}
        input_json = json.dumps(input_data)
        # We output a status
        output_data = {"status" : ""}
        output_json = json.dumps(output_data)
        
        self.istr_jsons = input_json
        self.ostr_jsons = output_json
        
    def do_mount(self, subtree, resource):
        
            # subtree name must start with "/"
            if not subtree.startswith("/"):
                raise MLException()
            
            comps = subtree.split("/")
            # subtree is of the form /name/child/grandchild
            # split results in
            #    "", "name", "child", "grandchild"
            # Even for leaf we will have at a minimum 2 components
            #    "", "name"
            if len(comps) < 3:
                raise MLException()
                
            # First component is always the empty string
            if comps[0] != "":
                raise MLException()
            # Second component is always self
            self.name = comps[1]
                
            cname = comps[2]
            if len(comps) == 3:  # My child in the subtree is a leaf, instantiate it
                dprint(DPRINT_DEBUG, "creating node: resource: " + resource + ", mount_subtree: " + subtree)
                child = eval(resource + "()")
                child.class_name = resource
                child.name = cname
                child.parent = self
                old_sib = None
                for c in self.child:
                    old_sib = c
                if not old_sib is None:
                    child.old_sib = old_sib
                    old_sib.young_sib = child
                self.child.append(child)
                return child
            
            # Our chilc is an internal. We assume that internal nodes are instantiated earlier in the
            # mount table (as leaves) that their leaves. So the child must be on our list of instatiated
            # children. It is a mount error otherwise
            child = None
            for c in self.child:
                if c.name == cname:
                    child = c
                    break
            if child is None:
                raise MLException()
                
            subtree = "/" + "/".join(comps[2:])
            return child.do_mount(subtree, resource)
 
        
    def mount(self, mount_spec):
        # Make sure system is not already mounted
        if len(self.child) != 0:
            raise MLException()
        # Don't allow mount() to be invoked on anybody other than root
        if self.class_name != "MLRoot":
            raise MLException()
            
        # One restriction in the mount table is that the
        # parent must appear as a leaf in the mount_table before
        # a child appears as a leaf.
        with open(mount_spec, "rb") as mfd:
            json_str = mfd.read()
            mount_tab = json.loads(json_str)
            mtab_list = []
            for line_dict in mount_tab:
                comment = True
                entry = {}
                for k, v in line_dict.iteritems():
                    if k == "resource" or k == "mount_point" or k == "options":
                        comment = False
                        entry[k] = v
                if comment:
                    continue
                keys = entry.keys()
                if not "resource" in keys or "mount_point" not in keys:
                    raise MLException()
                mtab_list.append(entry)
        
        for entry in mtab_list:
            # No need to mount root, it is always present
            if entry["resource"] == "MLRoot":
                root_mount = entry["mount_point"]
                continue
            
            # We need an absolute path rooted at /root
            if not entry["mount_point"].startswith("/"):
                raise MLException()
            if not entry["mount_point"].startswith(root_mount):
                raise MLException()
            self.do_mount(entry["mount_point"], entry["resource"])
    
    def do_umount(self):
        if len(self.child) != 0:
            child = self.child[0].do_umount()
            del child
        self.child = []
        if not self.young_sib is None:
            young_sib = self.young_sib.do_umount()
            del young_sib
            self.young_sib = None
        
        dprint(DPRINT_DEBUG, "Freeing node: name: " + self.name +",class: " + self.class_name)
        self.class_name = None
        self.name = None
        if not self.old_sib is None:
            self.old_sib.young_sib = None
        
        return self
    
    
    def umount(self):
        # Don't allow mount() to be invoked on anybody other than root
        if self.class_name != "MLRoot":
            raise MLException()
            
        self.do_umount()
        
            
    def do_print(self, parent_path):
        if parent_path is None:
            raise MLException()
            
        my_path = parent_path + "/" + self.name
        dprint(DPRINT_DEBUG, self.name + ": path: " + my_path)
        
        for c in self.child:
            c.do_print(parent_path=my_path)
            
    def print_tree(self):
        my_path = "/" + self.name
        for c in self.child:
            c.do_print(parent_path=my_path)
   
    def do_compile(self, istr_jsons):
        
        dprint(DPRINT_DEBUG,  "do_compile: " + self.name)
        
        if self.ostr_jsons is None:
            raise MLEXception()
            
        input_data = json.loads(istr_jsons)
        output_data = json.loads(self.ostr_jsons)
        for k in output_data.keys():
            input_data[k] = ""
            
        return json.dumps(input_data)
    
    def compile(self, istr_jsons):
        dprint(DPRINT_DEBUG,  "compile: " + self.name)
        if self.istr_jsons is None:
            raise MLException()
            
        # * means I will take anything
        dprint(DPRINT_VERBOSE, self.name + " input_data jsons: " + str(istr_jsons))
        dprint(DPRINT_VERBOSE, self.name + " istr_jsons: " + str(self.istr_jsons))
        
        actual_input = json.loads(istr_jsons)
        actual_keys = actual_input.keys()
        
        # Input should have everything we need (specified by input schema)
        # We don't care if there is other data present in input
        need_input = json.loads(self.istr_jsons)
        for need_key in need_input.keys():
               if not need_key in actual_keys:
                    dprint(DPRINT_DEBUG, self.name + " input_data jsons: " + str(istr_jsons))
                    dprint(DPRINT_DEBUG, self.name + " istr_jsons: " + str(self.istr_jsons))
                    raise MLException()
        
        ostr_jsons = self.do_compile(istr_jsons)
        
        actual_output = json.loads(ostr_jsons)
        actual_keys = actual_output.keys()
       
        # Output should have everything we want to output (specified by output schema)
        # We don't care if there is other data present in output
        need_output = json.loads(self.ostr_jsons)
        for need_key in need_output.keys():
            if not need_key in actual_keys:
                dprint(DPRINT_DEBUG, self.name + " output_data jsons: " + str(ostr_jsons))
                dprint(DPRINT_DEBUG, self.name + " ostr_jsons: " + str(self.ostr_jsons))
                raise MLException()
        
        if len(self.child) != 0:
            ostr_jsons = self.child[0].compile(ostr_jsons)
        if not self.young_sib is None:
            ostr_jsons = self.young_sib.compile(ostr_jsons)
        
        dprint(DPRINT_VERBOSE, self.name + " ostr_jsons: " + str(ostr_jsons))
        return ostr_jsons
    
    def do_run(self, input_data, traversal):
        
        if traversal == "POST":
            input_data["status"] = "SUCCESS"
            return input_data
        
        dprint(DPRINT_DEBUG, self.name + ": RUN: NULL_ACTION: traversal: " + traversal)
        
        return input_data
    
    def run(self, save, resume=None, comps=[""], resume_comps=None, state=None, run_state=None, input_data=None, traversal="PRE"):
        
        if resume == ".":
            raise MLException('"." is currently not supported for resume')
            
        if self.class_name == "MLRoot" and not resume is None:
            if state is None:
                raise MLException()
            if not isinstance(state, basestring):
                raise MLException()
            with open(state, "r") as ifd:
                msg = "Loading state pickle file: " + state
                dprint(DPRINT_INFO, msg)
                tprint(msg)
                state = pickle.load(ifd)
                
            resume_comps = resume.split("/")
            if resume_comps[0] != "":
                raise MLEXception()
            del resume_comps[0]
            dup = state[0][1]
            self.class_name = dup.class_name
            self.name = dup.name
            self.parent = dup.parent
            self.young_sib = dup.young_sib
            self.old_sib = dup.old_sib
            self.child = dup.child
            self.istr_jsons = dup.istr_jsons
            self.ostr_jsons = dup.ostr_jsons
            run_state = []
        elif self.class_name == "MLRoot":
            run_state = []
            resume_comps = []
        
        
        #if not resume is None:
        if len(resume_comps) > 0:
            msg = "Restoring input_data for: " + self.name
            dprint(DPRINT_VERBOSE, msg)
            tprint(msg)
            input_data = state[0][2]
            del state[0]
            
        
        # Do runtime checks on inputs - input should conform to schema
        need_json = json.loads(self.istr_jsons)
        input_keys = input_data.keys()
        
        for need_key in need_json.keys():
            if need_key not in input_keys:
                dprint(DPRINT_ERROR, self.name + ": Runtime Exception: Input data does not conform to input schema.")
                dprint(DPRINT_ERROR, self.name + ": " + json.dumps(input_data.keys()) + "," + self.istr_jsons)
                raise MLException()
    
        #if not resume is None:
        if len(resume_comps) > 0:
            name = resume_comps[0]
            if name == self.name:
                del resume_comps[0]
        
        idx = len(comps)
        comps.append(self.name)
        path = "/".join(comps)
        dprint(DPRINT_DEBUG, "Saving input state for: " + self.name + ", path: " + path)
        run_state.append((path, copy.deepcopy(self), copy.deepcopy(input_data)))
        
        #if resume is None or len(resume_comps) == 0:
        if len(resume_comps) == 0:
            msg = "Running do_run for: " + self.name
            dprint(DPRINT_DEBUG, msg)
            tprint(msg)
            input_data = self.do_run(input_data, "PRE")
        #elif not resume is None:
        else:
            if len(state) > 2 and self.name in state[2][0]:
                self.child = []
                child = state[2][1]
                self.child.append(child)
                child.parent = self
                msg = "Created child: " + child.name + " for: " + self.name
                dprint(DPRINT_DEBUG, msg)
                tprint(msg)
            msg = "Restoring output_data for: " + self.name
            dprint(DPRINT_DEBUG, msg)
            tprint(msg)
            input_data = state[0][2]
            del state[0]
            
        dprint(DPRINT_DEBUG, "Saving output state for: " + self.name + ", path: " + path)
        run_state.append((path, copy.deepcopy(self), copy.deepcopy(input_data)))
        if len(self.child) != 0:
            input_data = self.child[0].run(save, resume, comps, resume_comps, state, run_state, input_data, "PRE")
        
        comps.pop(idx)
                
        #if resume is None or len(resume_comps) == 0:
        if len(resume_comps) == 0:
            msg = "Running do_run (POST) for: " + self.name
            dprint(DPRINT_DEBUG, msg)
            tprint(msg)
            input_data = self.do_run(input_data, "POST") 
        #elif not resume is None:
        else:
            msg = "Restoring POST output_data for: " + self.name
            dprint(DPRINT_DEBUG, msg)
            tprint(msg)
            input_data = state[0][2]
            del state[0]
        
        dprint(DPRINT_DEBUG, "Saving POST state for: " + self.name + ", path: " + path)
        run_state.append((path, copy.deepcopy(self), copy.deepcopy(input_data)))
            
        # At this point we have exhausted our children
        #if not resume is None:
        if len(resume_comps) > 0:
            if self.name in resume_comps:
                raise MLEXception()
            elif len(state) > 0 and self.name in state[0][0]:
                msg = "Still have children in state: name=" + self.name + ", state-path=" + state[0][0]
                dprint(DPRINT_ERROR, msg)
                tprint(msg)
                raise MLException()
        
        # Do POST once our subtree is complete
        # Do runtime checks on outputs - output should conform to schema
        
        ostr_json = json.loads(self.ostr_jsons)
        output_keys = input_data.keys()
        
        for ostr_key in ostr_json.keys():
            if ostr_key not in output_keys:
                dprint(DPRINT_ERROR, self.name + ": Runtime Exception: Output data does not conform to output schema.")
                dprint(DPRINT_ERROR, self.name + ": " + json.dumps(input_data.keys()) + "," + self.ostr_jsons)
                raise MLException()
        
        #if not resume is None and len(state) > 0:
        if len(resume_comps) > 0:
            if not len(state) > 0:
                raise MLException()
            path = state[0][0]
            state_comps = path.split("/")
            if not self.class_name is MLRoot and self.parent.name == state_comps[-2]:
                self.young_sib = state[0][1]
                self.young_sib.old_sib = self
                self.young_sib.parent = self.parent
                self.parent.child.append(self.young_sib)
                msg = "Created younger_sibling: " + self.young_sib.name + " for: " + self.name
                dprint(DPRINT_DEBUG, msg)
                tprint(msg)
                
        if not self.young_sib is None:
            input_data = self.young_sib.run(save, resume, comps, resume_comps, state, run_state, input_data, "PRE")
        
        if self.class_name == "MLRoot" and save:
            if state is None:
                raise MLException()
            if not isinstance(state, basestring):
                raise MLException()
            with open(state, "wb") as ofd:
                pickle.dump(run_state, ofd, -1)
            msg = "Saved state pickle file: " + state
            dprint(DPRINT_INFO, msg)
            tprint(msg)
        return input_data
    
    # dict_json must be a json string for a dict. Only top level keys become property names
    # all other embedded objects become a part of the property value
    # Example:  {"mygoofyprop" : {"myembedded_key" : 123}}
    #           becomes:   self.mygoofyprop = {"myembedded_key" : 123}
    def do_setprop(self, comps, dict_json):
        # Unconditional set (a parent set this property so we need to set it too)
       
        if comps is None:
            for k, v in dict_json.iteritems():
                exec("self." + k + " = " + str(v))
            if len(self.child) != 0:
                self.child[0].do_setprop(None, dict_json)
            if not self.young_sib is None:
                self.young_sib.do_setprop(None, dict_json)
            return
        
        if len(comps) == 1 and comps[0] == self.name:
            for k, v in dict_json.iteritems():
                if isinstance(v, basestring):
                    v = '"""' + v + '"""'
                exec("self." + k + " = " + str(v))
                
            # Ask our children to unconditionally set the proprty
            if len(self.child) != 0:
                return self.child[0].do_setprop(None, dict_json)
            
            return
        elif comps[0] == self.name:
            # There is more than 1 component left
            if len(self.child) != 0:
                return self.child[0].do_setprop(comps[1:], dict_json)
        elif not self.young_sib is None:
            # It not us or our child subtree, pass it on to sibling
            return self.young_sib.do_setprop(comps, dict_json)
        else:
            raise MLException()
        
    
    # Setting a property on a parent node sets it on all child nodes
    def setprop(self, path, dict_json):
        
        comps = path.split("/")
        # comps[0] = "". The real name starts with component 1
        del comps[0]
        if comps[0] != self.name:
            raise MLException()
            
        return self.do_setprop(comps, dict_json)
    
    def do_walk(self, filepath, arg):
        dprint(DPRINT_DEBUG, self.name + ": visiting file: " + filepath)
        return {"stop" : False}
    
    
    def walk_files(self, pathroot, arg, topdown=True):
         
        isfile = os.path.isfile(pathroot)
        isdir = os.path.isdir(pathroot)
        
        if not isfile and not isdir:
            dprint(DPRINT_ERROR, "Unknown FS entity: " + pathroot)
            raise MLException()
                                         
        try:
            if isfile:
                result = self.do_walk(pathroot, arg)
            elif isdir:
                stop = False
                for dirpath, dirnames, filenames in os.walk(pathroot, topdown=topdown, onerror=walk_exception, followlinks=False):
                    if stop:
                        break
                    for filename in filenames:
                        filepath = os.path.join(dirpath, filename)
                        result = self.do_walk(filepath, arg)
                        if result["stop"]:
                            stop = True
                            break
        except:
            # Use debug_raise() to reraise the original exception in debug mode
            debug_raise()
            raise MLException()
            
    # data_name is a string and data is a 1-D or 2-D numpy array
    def do_save(self, data_name, data):

        if not isinstance(data_name, basestring):
            raise MLException()
        
        if not isinstance(data, np.ndarray):
            raise MLException()
            
        if data.ndim != 1 and data.ndim != 2:
            raise MLException()
            
        DBobj = db.PGDBLib(MLRoot.db_password, MLRoot.db_host, MLRoot.db_port)
        DBobj.open_db(MLRoot.db_tmo)
    
        rows = data.shape[0]
            
        fields = []
        fields.append(("example_id", 'varchar', None))
        fields.append(("features", "varchar", None))
        constraint = 'CONSTRAINT pkey_' + data_name + " PRIMARY KEY (example_id)"
        DBobj.create_table(data_name, fields, constraint)
        
        for i in range(rows):
            matcher = []
            matcher.append(("example_id", "varchar", str(i)))
            fields = []
            fields.append(("example_id", 'varchar', str(i)))
            if data.ndim == 1:
                value = [data[i]]
            else:
                value = data[i].tolist()

            value = json.dumps(value)

            fields.append(("features", "varchar", value))
            DBobj.upsert(data_name, fields, matcher)
        DBobj.close_db()
        
    def do_read(self, data_name):
        if not isinstance(data_name, basestring):
            raise MLException()
        
        DBobj = db.PGDBLib(MLRoot.db_password, MLRoot.db_host, MLRoot.db_port)
        DBobj.open_db(MLRoot.db_tmo)   
        
        fields = []
        fields.append(("example_id", "varchar", None))
        fields.append(("features", "varchar", None))
        row_list = DBobj.get_rows(data_name, fields, None) 
        nrows = len(row_list)
        if nrows < 1:
            raise MLException()
        some_row = row_list[0]
                          
        # Two extra fields are added by PGDBLib
        nfields = len(some_row) - 2 
        if nfields != 2:
            raise MLException()
            
        ncols = len(json.loads(some_row[3]))
        if ncols == 1:
            data = np.empty(shape=(nrows))
        else:
            data = np.empty(shape=(nrows, ncols))
    
        for i in range(nrows):
            row = int(row_list[i][2])
            value = json.loads(row_list[i][3])
            if ncols == 1:
                data[row] = value[0]
            else:
                data[row,:] = value
        
        DBobj.close_db()     
        return data
    
    def do_delete(self, data_name):
        return
        if not isinstance(data_name, basestring):
            raise MLException()
        
        DBobj = db.PGDBLib(MLRoot.db_password, MLRoot.db_host, MLRoot.db_port)
        DBobj.open_db(MLRoot.db_tmo) 
        
        DBobj.delete_rows(data_name, [])
        
        DBobj.drop_table(data_name)
        
        DBobj.close_db()
    
    @staticmethod
    def init_storage(password, host="localhost", port="5432"):
        db_password = password
        db_host = host
        db_port = port
        db_tmo = 40000 * 3600 # 40 hours
        DBobj = db.PGDBLib(password, host, port)
        DBobj.create_db()
        DBobj.open_db(db_tmo)
        DBobj.close_db()
    
    @staticmethod
    def destroy_storage():
        db_password = None
        db_host = None
        db_port = None
        


# In[29]:

class MLFetch(MLRoot):
    def __init__(self):
        super(MLFetch, self).__init__()
        self.class_name = "MLFetch"
        self.max_size = None
             
        input_data = {"remote_loc" : ""}
        input_json = json.dumps(input_data)
        output_data = {"final_loc" : "" }
        output_json = json.dumps(output_data)
        
        self.istr_jsons = input_json
        self.ostr_jsons = output_json
        
    def do_run(self, input_data, traversal):
        if traversal == "POST":
            if not "final_loc" in input_data.keys():
                input_data["final_loc"] = ""
            return input_data
        dprint(DPRINT_DEBUG, self.name + ": Fetching data from: " + input_data["remote_loc"])
        return input_data


# In[30]:

class MLDerive(MLRoot):
    def __init__(self):
        super(MLDerive, self).__init__()
        self.class_name = "MLDerive"
        self.X_map = None
        self.y_map = None
        
        input_data = {"final_loc" : ""}
        input_json = json.dumps(input_data)
        output_data = {"features" : ""}
        output_json = json.dumps(output_data)
        
        self.istr_jsons = input_json
        self.ostr_jsons = output_json
        
    def do_walk(self, filepath, arg):
          self.one_X_y(filepath, arg["extract_func"], arg["arg"], arg["X"], arg["y"])
        
    def one_X_y(self, filepath, extract_func, arg, X, y):
        data = extract_func(filepath)
        
        features = np.zeros(shape=(len(self.X_map)))
        labels = np.zeros(shape=(len(self.y_map)))
        
        for j, k in enumerate(self.y_map):
            labels[j] = data[k]
        for j, k in enumerate(self.X_map):
            features[j] = data[k]
        
        X.append(features)
        y.append(labels)
            
    def extract_X_y(self, extract_func, arg, file_spec):
        # Label cannot also be in feature list
        for label in self.y_map:
            if label in self.X_map:
                raise MLException()
        X = []
        y = []
        
        if isinstance(file_spec, basestring):
            arg["extract_func"] = extract_func
            arg["arg"] = arg
            arg["X"] = X
            arg["y"] = y
            self.do_walk(file_spec, arg)
        elif isinstance(file_spec, list):
            for fpath in file_spec:
                self.one_X_y(fpath, extract_func, arg, X, y)
        else:
            raise MLException()
            
        if len(X) != len(y):
            raise MLException()
            
        Xarray = np.empty(shape=(len(X), len(self.X_map)))
        yarray = np.empty(shape=(len(y), len(self.y_map)))
        
        for i in range(len(X)):
            Xarray[i,:] = X[i]
        for i in range(len(y)):
            yarray[i,:] = y[i]
            
        return Xarray, yarray
    
    def do_run(self, input_data, traversal):
        if traversal == "POST":
            if not "features" in input_data.keys():
                input_data["features"] = ""
            return input_data
        
        dprint(DPRINT_DEBUG, self.name + ": Deriving Features from: " + input_data['final_loc'])
        
        self.final_loc = input_data['final_loc']
        
        return input_data


# In[31]:

class MLModel(MLRoot):
    def __init__(self):
        super(MLModel, self).__init__()
        self.class_name = "MLModel"
        
        input_data = {"features" : ""}
        input_json = json.dumps(input_data)
        output_data = {"model" : ""}
        output_json = json.dumps(output_data)
        
        self.istr_jsons = input_json
        self.ostr_jsons = output_json
        
    def do_run(self, input_data, traversal):
        if traversal == "POST":
            if not "model" in input_data.keys():
                input_data["model"] = ""
            return input_data
        dprint(DPRINT_INFO, self.name + ": Modelling... ")
        return input_data


# In[32]:

class MLIngest(MLFetch):
    def __init__(self):
        super(MLIngest, self).__init__()
        self.class_name = "MLIngest"
        
        input_data = {"remote_loc" : ""}
        input_json = json.dumps(input_data)
        output_data = {"fetch_protocol" : "", "download_loc" : ""}
        output_json = json.dumps(output_data)
        
        self.istr_jsons = input_json
        self.ostr_jsons = output_json
        
    def do_run(self, input_data, traversal):
        
        if traversal == "POST":
            if not "download_loc" in input_data.keys():
                input_data["download_loc"] = ""
            return input_data
        
        inloc = input_data["remote_loc"]
        
        if inloc.startswith("http://") or inloc.startswith("https://"):
            fetch_protocol = "HTTP"
        elif os.path.exists(inloc) and (os.path.isfile(inloc) or os.path.isdir(inloc)):
            fetch_protocol = "LOCAL_FS"
        else:
            # We can't handle this 
            raise MLException()
            
        input_data["fetch_protocol"] = fetch_protocol
        
        # We don't prune nodes in the walk as this makes static checking (compile) difficult
        return input_data
        
     
    


# In[33]:

class MLHttpDownload(MLIngest):
    def __init__(self):
        super(MLHttpDownload, self).__init__()
        self.class_name = "MLHttpDownload"
        self.download_loc = None
        
        input_data = {"remote_loc" : "", "fetch_protocol" : ""}
        input_json = json.dumps(input_data)
        output_data = {"download_loc" : ""}
        output_json = json.dumps(output_data)
        
        self.istr_jsons = input_json
        self.ostr_jsons = output_json
        
    def do_run(self, input_data, traversal):
        
        if traversal == "POST":
            if not "download_loc" in input_data.keys():
                input_data["download_loc"] = ""
            return input_data
        
        if input_data["fetch_protocol"] != "HTTP":
            # We can't handle this. Pass it on, someone else may be able to
            input_data["download_loc"] = ""
            return input_data
        
        self.download_loc = str(np.random.randint(randint_limit)) + ".download"
        input_data["download_loc"] = self.download_loc
        dprint(DPRINT_INFO, 
               self.name + ": Downloading HTTP data from: " + input_data["remote_loc"] + " to " + input_data["download_loc"])
           
        url = input_data["remote_loc"]
        local_dest = input_data["download_loc"]
        # Check that the local destination's directory exists and is writable
        try:
            dirpath = os.path.dirname(local_dest)
            with open(local_dest, "wb") as ofd:
                pass
            safe_rmtree(local_dest)
        except:
            debug_raise()
            raise MLException()
            
        try:
            r = requests.head(url)
            if r.status_code != requests.codes.ok:
                raise MLException()
        except:
            debug_raise()
            raise MLException()
            
        try:
            src_size = r.headers['Content-Length']
        except:
            debug_raise()
            raise MLException()
            
        src_size_GB = src_size / (1024.0 * 1024.0 * 1024.0)
        
        if src_size_GB > self.max_size:
            raise MLException()
        
        try:
            r = requests.get(url, stream=True)
            if r.status_code != requests.codes.ok:
                raise MLException()
        except:
            debug_raise()
            raise MLException()
        
        try:
            with open(local_dest, 'wb') as fd:
                # Use 8K chunk size
                for chunk in r.iter_content(fetch_data_chunksize__):
                    fd.write(chunk)
        except:
            debug_raise()
            raise MLException()
        
        self.raw_size = src_size_GB
        
        return input_data
    


# In[34]:

class MLFSDownload(MLIngest):
    def __init__(self):
        super(MLFSDownload, self).__init__()
        self.class_name = "MLFSDownload"
        self.download_loc = None
        
        input_data = {"remote_loc" : "", "fetch_protocol" : ""}
        input_json = json.dumps(input_data)
        output_data = {"download_loc" : ""}
        output_json = json.dumps(output_data)
        
        self.istr_jsons = input_json
        self.ostr_jsons = output_json
     
        
    def do_run(self, input_data, traversal):
        
        if traversal == "POST":
            if not "download_loc" in input_data.keys():
                input_data["download_loc"] = ""
            return input_data

        # First check if we handle this data
        if input_data["fetch_protocol"] != "LOCAL_FS":
            return input_data
        
        # First check if the local_src exists
        local_src = input_data["remote_loc"]
        if os.path.exists(local_src):
            dprint(DPRINT_INFO, self.name + ": The local source (" + local_src + ") exists.")
        else:
            raise MLException()
            
        try:
            src_size = os.path.getsize(local_src)
            src_size_GB = src_size / (1024.0 * 1024.0 * 1024.0)
        except:
            debug_raise()
            raise MLEXception()
        
        if src_size_GB > self.max_size:
            raise MLException()
            
        # Dont't move the data. Use local source as is
        input_data["download_loc"] = local_src
        
        return input_data
        


# In[35]:

class MLDecompress(MLFetch):
    def __init__(self):
        super(MLDecompress, self).__init__()
        self.class_name = "MLDecompress"
         
        input_data = {"download_loc" : ""}
        input_json = json.dumps(input_data)
        output_data = {"decompress_loc" : ""}
        output_json = json.dumps(output_data)
        
        self.istr_jsons = input_json
        self.ostr_jsons = output_json
        
    def do_run(self, input_data, traversal):
        
        if traversal == "POST":
            if not os.path.exists(self.decompress_loc) or not os.path.isdir(self.decompress_loc):
                # No decompression happened
                safe_rmtree(self.decompress_loc)
                input_data["decompress_loc"] = input_data["download_loc"]
            return input_data
        
        dprint(DPRINT_INFO, self.name + ": Decompressing: " + input_data["download_loc"])
        self.decompress_loc = input_data["download_loc"] + "." + str(np.random.randint(randint_limit)) + ".decompressed"
        input_data["decompress_loc"] = self.decompress_loc
        return input_data


# In[36]:

class MLZipDecompress(MLDecompress):
    def __init__(self):
        super(MLZipDecompress, self).__init__()
        self.class_name = "MLZipDecompress"
        self.mime_type = 'application/zip'
        
        # Inherits input and output schema from parent
        
    def do_walk(self, filepath, input_data):
        mime_type = magic.from_file(filepath, mime=True)
        if mime_type != self.mime_type:
            return {"stop" : False}
        
        dprint(DPRINT_INFO, self.name + ": ZipDecompress: " + filepath)
        if not filepath.startswith(self.download_loc):
            raise MLException()
    
        dest_suffix = filepath[len(self.download_loc):]
    
        if dest_suffix == "":
            fname = os.path.basename(self.download_loc) + "." + str(np.random.randint(randint_limit)) + ".unzip"
            dest = os.path.join(self.decompress_loc, fname)
        else:
            subdir = os.path.dirname(dest_suffix)
            fname = os.path.basename(dest_suffix) + "." + str(np.random.randint(randint_limit)) + ".unzip"
            if subdir == "":
                dest = os.path.join(self.decompress_loc, fname)
            else:
                subdir = subdir.split("/")
                subdir = "/".join(subdir[1:])
                dest = os.path.join(self.decompress_loc, subdir, fname) 
        
        try:
            os.makedirs(dest)
        except:
            debug_raise()
            raise MLException()
        
        try:
            z = zipfile.ZipFile(filepath)
            z.extractall(dest)
            z.close()
        except:
            debug_raise()
            safe_rmtree(self.decompress_loc, ignore_errors=True)
            raise MLException()
            
        return {"stop" : False}
        
    def do_run(self, input_data, traversal):
        
        if traversal == "POST":
            if not "decompress_loc" in input_data.keys():
                input_data["decompress_loc"] = ""
            return input_data
        
        self.download_loc = input_data["download_loc"]
        self.decompress_loc = input_data["decompress_loc"]
        self.walk_files(self.download_loc, input_data)
        
        return input_data


# In[37]:

class MLBzip2Decompress(MLDecompress):
    def __init__(self):
        super(MLBzip2Decompress, self).__init__()
        self.class_name = "MLBzip2Decompress"
        self.mime_type = 'application/x-bzip2'
        
        # Inherits input and output schema from parent
      
    def do_walk(self, filepath, input_data):
        
        mime_type = magic.from_file(filepath, mime=True)
        
        if mime_type != self.mime_type:
            return {"stop" : False}
        
        dprint(DPRINT_INFO, self.name + ": Bzip2Decompress: " + filepath)
        if not filepath.startswith(self.download_loc):
            raise MLException()
            
        dest_suffix = filepath[len(self.download_loc):]
        
        if dest_suffix == "":
            fname = self.download_loc + "." + str(np.random.randint(randint_limit)) + ".bunzip2"
            fname = fname.split("/")
            fname = "/".join(fname[1:])
            dest = os.path.join(self.decompress_loc, fname)
        else:
            subdir = os.path.dirname(dest_suffix)
            fname = os.path.basename(dest_suffix) + "." + str(np.random.randint(randint_limit)) + ".bunzip2"
            if subdir == "":
                dest = os.path.join(self.decompress_loc, fname)
            else:
                subdir = subdir.split("/")
                subdir = "/".join(subdir[1:])
                dest = os.path.join(self.decompress_loc, subdir, fname) 
        
        try:
            os.makedirs(dest)
        except:
            debug_raise()
            raise MLException()
            
        destfile = os.path.join(dest, ".file")
            
        try:
            data = bz2.decompress(open(filepath, "rb").read())
        except:
            debug_raise()
            raise MLException()
        
        try:
            with open(destfile, "wb") as ofd:
                ofd.write(data)
        except:
            debug_raise()
            raise MLException()
            
        return {"stop" : False}
        
    def do_run(self, input_data, traversal):
        if traversal == "POST":
            if not "decompress_loc" in input_data.keys():
                input_data["decompress_loc"] = ""
            return input_data
 
        self.download_loc = input_data["download_loc"]
        self.decompress_loc = input_data["decompress_loc"]
        
        self.walk_files(self.download_loc, input_data)
    
        return input_data


# In[38]:

class MLGzipDecompress(MLDecompress):
    def __init__(self):
        super(MLGzipDecompress, self).__init__()
        self.class_name = "MLGzipDecompress"
        self.mime_type = 'application/x-gzip'
        
        # Inherits input and output schema from parent
      
    def do_walk(self, filepath, input_data):
        
        mime_type = magic.from_file(filepath, mime=True)
        if mime_type != self.mime_type:
            return {"stop" : False}
        
        dprint(DPRINT_INFO, self.name + ": GzipDecompress: " + filepath)
        if not filepath.startswith(self.download_loc):
            raise MLException()
        
        dest_suffix = filepath[len(self.download_loc):]
        
        if dest_suffix == "":
            fname = self.download_loc + "." + str(np.random.randint(randint_limit)) + ".gunzip"
            fname = fname.split("/")
            fname = "/".join(fname[1:])
            dest = os.path.join(self.decompress_loc, fname)
        else:
            subdir = os.path.dirname(dest_suffix)
            fname = os.path.basename(dest_suffix) + "." + str(np.random.randint(randint_limit)) + ".gunzip"
            if subdir == "":
                dest = os.path.join(self.decompress_loc, fname)
            else:
                subdir = subdir.split("/")
                subdir = "/".join(subdir[1:])
                dest = os.path.join(self.decompress_loc, subdir, fname) 
 
        try:
            os.makedirs(dest)
        except:
            debug_raise()
            raise MLException()
            
        destfile = os.path.join(dest, ".file")
        
        try:
            with open(destfile, "wb") as ofd:
                with gzip.open(filepath, "rb") as cfd:
                    while True:
                        data = cfd.read(gunzip_blocksize_)
                        if data == '':
                            return {"stop" : False}
                        ofd.write(data)
        except:
            debug_raise()
            raise MLException()
            
        raise MLException()
        
    def do_run(self, input_data, traversal):
        
        if traversal == "POST":
            if not "decompress_loc" in input_data.keys():
                input_data["decompress_loc"] = ""
            return input_data
        
        self.download_loc = input_data["download_loc"]
        self.decompress_loc = input_data["decompress_loc"]
        
        self.walk_files(self.download_loc, input_data)
    
        return input_data
       
        


# In[39]:

class MLUnarchive(MLFetch):
    def __init__(self):
        super(MLUnarchive, self).__init__()
        self.class_name = "MLUnarchive"
        self.mime_type = None
        self.unarchive_loc = None
        
        input_data = {"decompress_loc" : ""}
        input_json = json.dumps(input_data)
        output_data = {"unarchive_loc" : ""}
        output_json = json.dumps(output_data)
        
        self.istr_jsons = input_json
        self.ostr_jsons = output_json
        
    def do_run(self, input_data, traversal):
        
        if traversal == "POST":
            if not os.path.exists(self.unarchive_loc) or not os.path.isdir(self.unarchive_loc):
                # No unarchiving happened
                safe_rmtree(self.unarchive_loc)
                input_data["unarchive_loc"] = input_data["decompress_loc"]
            return input_data
        
        dprint(DPRINT_INFO, self.name + ": Unarchiving: " + input_data["decompress_loc"])
        self.unarchive_loc = input_data["decompress_loc"] + "." + str(np.random.randint(randint_limit)) + ".unarchived"
        input_data["unarchive_loc"] = self.unarchive_loc
        return input_data


# In[40]:

class MLTarUnarchive(MLUnarchive):
    def __init__(self):
        super(MLTarUnarchive, self).__init__()
        self.class_name = "MLTarUnarchive"
        self.mime_type = "application/x-tar"
        
        # Inherits input and output schema from parent
    
    
    def do_walk(self, filepath, input_data):
        
        mime_type = magic.from_file(filepath, mime=True)
        if mime_type != self.mime_type:
            return {"stop" : False}
        
        dprint(DPRINT_INFO, self.name + ": Untar: " + filepath)
        
        if not filepath.startswith(self.decompress_loc):
            raise MLException()
            
        dest_suffix = filepath[len(self.decompress_loc):]
        
        if dest_suffix == "":
            fname = self.download_loc + "." + str(np.random.randint(randint_limit)) + ".untar"
            fname = fname.split("/")
            fname = "/".join(fname[1:])
            dest = os.path.join(self.unarchive_loc, fname)
        else:
            subdir = os.path.dirname(dest_suffix)
            fname =  os.path.basename(dest_suffix) + "." + str(np.random.randint(randint_limit)) + ".untar"
            if subdir == "":
                dest = os.path.join(self.unarchive_loc, fname)
            else:
                subdir = subdir.split("/")
                subdir = "/".join(subdir[1:])
                dest = os.path.join(self.unarchive_loc, subdir, fname)
        
        try:
            if not tarfile.is_tarfile(filepath):
                raise MLException()
        except:
            debug_raise()
            raise MLException()
          
        try:
            tfile = tarfile.TarFile(filepath, "r")
        except:
            debug_raise()
            raise MLException()
        
        try:
            os.makedirs(dest)
        except:
            debug_raise()
            raise MLException()
            
        try:
            tfile.extractall(dest)
        except:
            debug_raise()
            raise MLException()
            
        return {"stop" : False}

        
    def do_run(self, input_data, traversal):
        
        if traversal == "POST":
            if not "unarchive_loc" in input_data.keys():
                input_data["unarchive_loc"] = ""
            return input_data
           
        self.download_loc = input_data["download_loc"] 
        self.decompress_loc = input_data["decompress_loc"]
        self.unarchive_loc = input_data["unarchive_loc"]
        
        self.walk_files(self.decompress_loc, input_data)
        
        return input_data


# In[41]:

class MLRaw(MLFetch):
    def __init__(self):
        super(MLRaw, self).__init__()
        self.class_name = "MLRaw"
        self.final_loc = None
        
        input_data = {"unarchive_loc" : ""}
        input_json = json.dumps(input_data)
        output_data = {"final_loc" : ""}
        output_json = json.dumps(output_data)
        
        self.istr_jsons = input_json
        self.ostr_jsons = output_json
        
    def do_run(self, input_data, traversal):
        
        if traversal == "POST":
            if not "final_loc" in input_data.keys():
                input_data["final_loc"] = ""
            return input_data
        
        dprint(DPRINT_INFO, self.name + ": Raw data processing from: " + input_data["unarchive_loc"])
    
        self.final_loc = input_data["final_loc"] = input_data["unarchive_loc"]
        
        return input_data


# In[42]:

class SampleExtractFeatures(MLDerive):
    def __init__(self):
        super(SampleExtractFeatures, self).__init__()
        self.class_name = "SampleExtractFeatures"
        self.mime_type = 'text/plain'  
        self.final_loc = None
        self.data_name = None
        
        input_data = {"final_loc" : ""}
        input_json = json.dumps(input_data)
        output_data = {"Xtrain" : "", "ytrain" : "", "Xtest" : "", "ytest" : ""}
        output_json = json.dumps(output_data)    
        
        self.istr_jsons = input_json
        self.ostr_jsons = output_json
        
    def sample_extract(self, filepath):
            
        df = pd.read_csv(filepath, header=0)
            
        y = df.iloc[:,0].copy()
        X = df.iloc[:,1:].copy()
        
        X = X.values
        y = y.values

        dprint(DPRINT_INFO, filepath + ": X.shape=" + str(X.shape) + ", y.shape=" + str(y.shape))
        return X, y

    
    def do_walk(self, filepath, input_data):
        if get_file_type(filepath, mime=True) != self.mime_type:
            raise MLException()
            
        if os.path.basename(filepath) == "sample_train_data.csv":
            input_data["Xtrain"], input_data["ytrain"] = self.sample_extract(filepath)
            self.do_save(self.data_name + "_Xtrain", input_data["Xtrain"])
            self.do_save(self.data_name + "_ytrain", input_data["ytrain"])
            input_data["Xtrain"] = self.data_name + "_Xtrain"
            input_data["ytrain"] = self.data_name + "_ytrain"
        elif os.path.basename(filepath) == "sample_test_data.csv":
            input_data["Xtest"], input_data["ytest"] = self.sample_extract(filepath)
            self.do_save(self.data_name + "_Xtest", input_data["Xtest"])
            self.do_save(self.data_name + "_ytest", input_data["ytest"])
            input_data["Xtest"] = self.data_name + "_Xtest"
            input_data["ytest"] = self.data_name + "_ytest"
        else:
            raise MLException()
            
        return {"stop" : False}
            
    def do_run(self, input_data, traversal):
        if traversal == "POST":
            if not "Xtrain" in input_data.keys():
                input_data["Xtrain"] = ""
            if not "ytrain" in input_data.keys():
                input_data["ytrain"] = ""
            if not "Xtest" in input_data.keys():
                input_data["Xtest"] = ""
            if not "ytest" in input_data.keys():
                input_data["ytest"] = ""
            return input_data
         
        if self.data_name is None or not isinstance(self.data_name, basestring):
            raise MLException()
            
        # Data names cannot have "." in them, a limitation of the storage subsystem.
        if "." in self.data_name:
            raise MLException()
            
        self.final_loc = input_data['final_loc']
        
        self.walk_files(self.final_loc, input_data)
        
        return input_data



# coding: utf-8

# In[4]:

from sklearn.naive_bayes import GaussianNB
from sklearn.naive_bayes import BernoulliNB
from sklearn.linear_model import LogisticRegression

# # Code common to both datasets (MNIST and Million Song)

# In[5]:

class BernNB(MLModel):
    def __init__(self):
        super(BernNB, self).__init__()
        self.class_name = "BernNB"
        self.nsamples_list = None
        
        # We don't need to check for features because
        # a) Our parent checks for it
        # b) We don't need it and we shouldn't have that knowledge encoded here, let our parent handle
        #    it in true OOP fashion
        input_data = {"Xtrain" : "", "ytrain" : "", "Xtest" : "", "ytest" : ""}
        input_json = json.dumps(input_data)
        output_data = {"model" : ""}
        output_json = json.dumps(output_data)
        
        self.istr_jsons = input_json
        self.ostr_jsons = output_json
        
    def do_run(self, input_data, traversal):
        if traversal == "POST":
            if not "model" in input_data.keys():
                input_data["model"] = ""
            return input_data
            
        Xtrain_data_name = input_data["Xtrain"]
        ytrain_data_name = input_data["ytrain"]
        Xtest_data_name = input_data["Xtest"]
        ytest_data_name = input_data["ytest"]
        input_data["Xtrain"] = self.do_read(Xtrain_data_name)
        self.do_delete(Xtrain_data_name)
        input_data["ytrain"] = self.do_read(ytrain_data_name)
        self.do_delete(ytrain_data_name)
        input_data["Xtest"] = self.do_read(Xtest_data_name)
        self.do_delete(Xtest_data_name)
        input_data["ytest"] = self.do_read(ytest_data_name)
        self.do_delete(ytest_data_name)
        
        dprint(DPRINT_INFO, "Bernoulli Naive Bayes: ")   
        if self.nsamples_list is None:
            self.nsamples_list = [input_data["Xtrain"].shape[0]]
        for nsamples in self.nsamples_list:
            BernNBclf = BernoulliNB(binarize=0.5)
            BernNBclf.fit(input_data["Xtrain"][0:nsamples], input_data["ytrain"][0:nsamples])
            accuracy = BernNBclf.score(input_data["Xtest"], input_data["ytest"])
            dprint(DPRINT_INFO, "\t\tAccuracy for: " + str(nsamples) + " samples is: " + str(accuracy))
            
        input_data["model"] = BernNBclf
        
        return input_data
        
class Logistic(MLModel):
    def __init__(self):
        super(Logistic, self).__init__()
        self.class_name = "Logistic"
        self.nsamples_list = None
           
        input_data = {"Xtrain" : "", "ytrain" : "", "Xtest" : "", "ytest" : ""}
        input_json = json.dumps(input_data)
        output_data = {"model" : ""}
        output_json = json.dumps(output_data)
        
        self.istr_jsons = input_json
        self.ostr_jsons = output_json
        
    def do_run(self, input_data, traversal):
        if traversal == "POST":
            if not "model" in input_data.keys():
                input_data["model"] = ""
            return input_data
        
        Xtrain_data_name = input_data["Xtrain"]
        ytrain_data_name = input_data["ytrain"]
        Xtest_data_name = input_data["Xtest"]
        ytest_data_name = input_data["ytest"]
        input_data["Xtrain"] = self.do_read(Xtrain_data_name)
        self.do_delete(Xtrain_data_name)
        input_data["ytrain"] = self.do_read(ytrain_data_name)
        self.do_delete(ytrain_data_name)
        input_data["Xtest"] = self.do_read(Xtest_data_name)
        self.do_delete(Xtest_data_name)
        input_data["ytest"] = self.do_read(ytest_data_name)
        self.do_delete(ytest_data_name)
       
        dprint(DPRINT_INFO, "Logistic Regression Classifier: ")
        if self.nsamples_list is None:
            self.nsamples_list = [input_data["Xtrain"].shape[0]]
        for nsamples in self.nsamples_list:
            logitclf = LogisticRegression()
            logitclf.fit(input_data["Xtrain"][0:nsamples], input_data["ytrain"][0:nsamples])
            accuracy = logitclf.score(input_data["Xtest"], input_data["ytest"])
            dprint(DPRINT_INFO, "Accuracy for: " + str(nsamples) + " samples is: " + str(accuracy))
            
        input_data["model"] = logitclf
        
        return input_data

class GaussNB(MLModel):
    def __init__(self):
        super(GaussNB, self).__init__()
        self.class_name = "GaussNB"
        self.nsamples_list = None
        
        input_data = {"Xtrain" : "", "ytrain" : "", "Xtest" : "", "ytest" : ""}
        input_json = json.dumps(input_data)
        output_data = {"model" : ""}
        output_json = json.dumps(output_data)
        
        self.istr_jsons = input_json
        self.ostr_jsons = output_json
        
    def do_run(self, input_data, traversal):
        if traversal == "POST":
            if not "model" in input_data.keys():
                input_data["model"] = ""
            return input_data
        
        Xtrain_data_name = input_data["Xtrain"]
        ytrain_data_name = input_data["ytrain"]
        Xtest_data_name = input_data["Xtest"]
        ytest_data_name = input_data["ytest"]
        input_data["Xtrain"] = self.do_read(Xtrain_data_name)
        self.do_delete(Xtrain_data_name)
        input_data["ytrain"] = self.do_read(ytrain_data_name)
        self.do_delete(ytrain_data_name)
        input_data["Xtest"] = self.do_read(Xtest_data_name)
        self.do_delete(Xtest_data_name)
        input_data["ytest"] = self.do_read(ytest_data_name)
        self.do_delete(ytest_data_name)
            
        dprint(DPRINT_INFO, "Gaussian Naive Bayes: ")   
        if self.nsamples_list is None:
            self.nsamples_list = [input_data["Xtrain"].shape[0]]
            
        for nsamples in self.nsamples_list:
            GaussNBclf = GaussianNB()
            GaussNBclf.fit(input_data["Xtrain"][0:nsamples], input_data["ytrain"][0:nsamples])
            accuracy = GaussNBclf.score(input_data["Xtest"], input_data["ytest"])
            dprint(DPRINT_INFO, "\t\tAccuracy for: " + str(nsamples) + " samples is: " + str(accuracy))
            
        input_data["model"] = GaussNBclf
        
        return input_data
            


# coding: utf-8

# In[811]:

import time
import hdf5_getters

# # Code for processing Million Song dataset follows

# In[ ]:

class CollectH5Files(MLRaw):
    def __init__(self):
        super(CollectH5Files, self).__init__()
        self.class_name = "CollectH5Files"
        self.mime_type = 'application/x-hdf'
        self.nsongs = None
        self.max_process = None
        self.h5_files = []
        
        input_data = {"final_loc" : ""}
        input_json = json.dumps(input_data)
        output_data = {"h5_files" : ""}
        output_json = json.dumps(output_data)
        
        self.istr_jsons = input_json
        self.ostr_jsons = output_json
        
    def do_walk(self, filepath, input_data):
        
        input_data["count"] += 1
        if get_file_type(filepath, mime=True) != self.mime_type:
            return {"stop" : False}
        
        # Uncomment this if you want a subset
        if len(self.h5_files) >= self.nsongs:
            return {"stop" : True}
        
        if input_data["count"] > self.max_process:
            return {"stop" : True}
        
        h5 = hdf5_getters.open_h5_file_read(filepath)
        song_year = int(hdf5_getters.get_year(h5).item())
        h5.close()
        
        if song_year is None or song_year == '' or song_year == 0 or song_year < 1800 or song_year > 2100:
            return {"stop" : False}
        
        self.h5_files.append(filepath)
        
        return {"stop" : False}
            
    def do_run(self, input_data, traversal):
        if traversal == "POST":
            if not "h5_files" in input_data.keys():
                input_data["h5_files"] = ""
            return input_data
            
        self.final_loc = input_data['final_loc']
        
        input_data["count"] = 0
        self.walk_files(self.final_loc, input_data)
        del input_data["count"]
        
        input_data["final_loc"] = self.final_loc
        input_data['h5_files'] = self.h5_files
        
        return input_data
        
class ExtractTrackData(MLDerive):
    def __init__(self):
        super(ExtractTrackData, self).__init__()
        self.class_name = "ExtractTrackData"
        self.mime_type = "application/x-hdf"
        self.h5_files = None
        self.nsongs = None
        self.train_frac = None
        
        input_data = {"final_loc" : "", "h5_files" : ""}
        input_json = json.dumps(input_data)
        output_data = {"Xtrain" : "", "ytrain" : "", "Xtest" : "", "ytest" : ""}
        output_json = json.dumps(output_data)
        
        self.istr_jsons = input_json
        self.ostr_jsons = output_json
        
    def get_track_data(self, filepath):
        h5 = hdf5_getters.open_h5_file_read(filepath)
        keys = filter(lambda x: x[:3] == 'get',hdf5_getters.__dict__.keys())
        track_data = {}
        track_data['year'] = hdf5_getters.get_year(h5).item()
        track_data['danceability'] = hdf5_getters.get_danceability(h5).item()
        track_data['loudness'] = hdf5_getters.get_loudness(h5).item()
        track_data['energy'] = hdf5_getters.get_energy(h5).item()
        track_data['tempo'] = hdf5_getters.get_tempo(h5).item()
        track_data['end_fade_in'] = hdf5_getters.get_end_of_fade_in(h5).item()
        track_data['start_fade_out'] = hdf5_getters.get_start_of_fade_out(h5).item()
        for k, v in track_data.iteritems():
            if not isinstance(v, int) and not isinstance(v, float):
                dprint(DPRINT_WARN,
                    "Found invalid value in track data: " + k +"=" + str(v))
                track_data[k] = 0.0
        
        h5.close()
        
        return track_data
            
    def do_run(self, input_data, traversal):
        if traversal == "POST":
            if not "Xtrain" in input_data.keys():
                input_data["Xtrain"] = ""
            if not "ytrain" in input_data.keys():
                input_data["ytrain"] = ""
            if not "Xtest" in input_data.keys():
                input_data["Xtest"] = ""
            if not "ytest" in input_data.keys():
                input_data["ytest"] = ""
            return input_data
            
        self.h5_files = input_data['h5_files']
        
        self.nsongs = input_data['nsongs'] = len(self.h5_files)
    
        X, y = self.extract_X_y(self.get_track_data, input_data, self.h5_files)
        
        # y is returned as a 2D array to support multilabel prediction. In our case we only have 1 label per example
        # so reshape into 1D array
        y = y.reshape((y.shape[0]))
        
        # We are interested in classifyings into pre-2000 and post-2000 songs
        dprint(DPRINT_DEBUG, "Before transform: y.shape=" + str(y.shape))

        for i in range(y.shape[0]):
            if y[i] >= 2000:
                y[i] = 1.0
            else:
                y[i] = 0.0
        dprint(DPRINT_DEBUG, "After transform: y.shape=" + str(y.shape))
        train_limit = int(self.train_frac * self.nsongs)
        
        # Let the parent add the useless "features" key
        input_data["Xtrain"], input_data["ytrain"] = X[0:train_limit], y[0:train_limit]
        dprint(DPRINT_INFO, 
            "Xtrain.shape=" + str(input_data["Xtrain"].shape) + ", ytrain.shape=" + str(input_data["ytrain"].shape))

        self.do_save(self.data_name + "_Xtrain", input_data["Xtrain"])
        self.do_save(self.data_name + "_ytrain", input_data["ytrain"])
        input_data["Xtrain"] = self.data_name + "_Xtrain"
        input_data["ytrain"] = self.data_name + "_ytrain"

        input_data["Xtest"], input_data["ytest"] = X[train_limit:], y[train_limit:]
        dprint(DPRINT_INFO, 
            "Xtest.shape=" + str(input_data["Xtest"].shape) + ", ytest.shape=" + str(input_data["ytest"].shape))

        self.do_save(self.data_name + "_Xtest", input_data["Xtest"])
        self.do_save(self.data_name + "_ytest", input_data["ytest"])
        input_data["Xtest"] = self.data_name + "_Xtest"
        input_data["ytest"] = self.data_name + "_ytest"

        return input_data


# coding: utf-8

# In[811]:

import pandas as pd

# # Code to process MNIST data follows this cell

# In[ ]:

class ExtractMNIST(MLDerive):
    def __init__(self):
        super(ExtractMNIST, self).__init__()
        self.class_name = "ExtractMNIST"
        self.mime_type = 'text/plain'  
        self.final_loc = None
        self.data_name = None
        
        input_data = {"final_loc" : ""}
        input_json = json.dumps(input_data)
        output_data = {"Xtrain" : "", "ytrain" : "", "Xtest" : "", "ytest" : ""}
        output_json = json.dumps(output_data)    
        
        self.istr_jsons = input_json
        self.ostr_jsons = output_json
        
    def extract_mnist(self, filepath):
            
        df = pd.read_csv(filepath, header=None)
            
        y = df.iloc[:,0].copy()
        X = df.iloc[:,1:].copy()
        
        X = X.values
        y = y.values
        
        # Normalize data to range [0,1]
        X = X/255.0
        dprint(DPRINT_INFO, filepath + ": X.shape=" + str(X.shape) + ", y.shape=" + str(y.shape))
        return X, y

    
    def do_walk(self, filepath, input_data):
        if get_file_type(filepath, mime=True) != self.mime_type:
            raise MLException()
            
        if os.path.basename(filepath) == "mnist_train.csv":
            input_data["Xtrain"], input_data["ytrain"] = self.extract_mnist(filepath)
            self.do_save(self.data_name + "_Xtrain", input_data["Xtrain"])
            self.do_save(self.data_name + "_ytrain", input_data["ytrain"])
            input_data["Xtrain"] = self.data_name + "_Xtrain"
            input_data["ytrain"] = self.data_name + "_ytrain"
        elif os.path.basename(filepath) == "mnist_test.csv":
            input_data["Xtest"], input_data["ytest"] = self.extract_mnist(filepath)
            self.do_save(self.data_name + "_Xtest", input_data["Xtest"])
            self.do_save(self.data_name + "_ytest", input_data["ytest"])
            input_data["Xtest"] = self.data_name + "_Xtest"
            input_data["ytest"] = self.data_name + "_ytest"
        else:
            raise MLException()
            
        return {"stop" : False}
            
    def do_run(self, input_data, traversal):
        if traversal == "POST":
            if not "Xtrain" in input_data.keys():
                input_data["Xtrain"] = ""
            if not "ytrain" in input_data.keys():
                input_data["ytrain"] = ""
            if not "Xtest" in input_data.keys():
                input_data["Xtest"] = ""
            if not "ytest" in input_data.keys():
                input_data["ytest"] = ""
            return input_data
         
        if self.data_name is None or not isinstance(self.data_name, basestring):
            raise MLException()
            
        # Postgres does not like "." in table names
        if "." in self.data_name:
            raise MLException()
            
        self.final_loc = input_data['final_loc']
        
        self.walk_files(self.final_loc, input_data)
        
        return input_data
