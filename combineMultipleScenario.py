'''
Simple wrapper for icclim.indice.

Run as follows:
``python -m dispel4py.new.processor simple ./usecase.py -f usecase_input.json``
'''

from dispel4py.workflow_graph import WorkflowGraph
from dispel4py.base import IterativePE, ProducerPE, ConsumerPE
from dispel4py.core import GenericPE
import pdb
from icclim import icclim
#import xarray
import numpy as np
from netCDF4 import Dataset
import netcdftime
import matplotlib.pyplot as plt
import json

save_path = '/tmp/'


class NetCDFProcessing(GenericPE):
    def __init__(self):
        GenericPE.__init__(self)
        self._add_input('input')
        self._add_output('output')

    def _process(self, parameters):

        import os
        local_path = "usecase_file.nc"
        if not os.path.isfile(local_path):
            os.system('wget https://b2drop.eudat.eu/s/gmKyrmdprXPppRj/download -O '+local_path)

        icclim.indice(**parameters['input'][self.name])

        self.write('output', (parameters['input'], 
        parameters['input'][self.name]['out_file'], 
        parameters['input'][self.name]['indice_name']))


class StreamProducer(GenericPE):
    def __init__(self):
        GenericPE.__init__(self)
        self._add_output('output')

    def _process(self, inputs):
        list_calcul = inputs.keys()
        len_lc = len(list_calcul)

        for name_calc in list_calcul:
            if inputs[name_calc]['out_file'] is None:
                inputs[name_calc]['out_file'] = save_path+name_calc+'.nc'

        self.write('output', inputs)


class NetCDF2xarray(GenericPE):
    def __init__(self):
        GenericPE.__init__(self)
        self._add_input('input')
        self._add_output('output')

    def _process(self, parameters):
        ds = xarray.open_dataset(parameters['input'][-2])
        self.write('output', (ds, [parameters['input'][-1]]))


class ReadNetCDF(GenericPE):
    def __init__(self):
        GenericPE.__init__(self)
        self._add_input('input')
        self._add_output('output')

    def _process(self, parameters):
        #Load the netcdf file
        nc = Dataset(parameters['input'][-2])

        #Extracting the time and change the time format from num to date time
        time = nc.variables['time']
        nc_time = netcdftime.utime(time.units, time.calendar)
        date_time = nc_time.num2date(time[:])

        var = nc.variables[parameters['input'][-1]][:]

        self.write('output', (date_time, var))


class StandardDeviation(GenericPE):
    def __init__(self):
        GenericPE.__init__(self)
        self._add_input('input')
        self._add_output('output')

    def _process(self, parameters):
        time = parameters['input'][0]
        var = parameters['input'][1]
        var = np.reshape(var, (var.shape[0], -1))
        result = np.std(var, axis=1)

        self.write('output',  (time, result, self.name))


class AverageData(GenericPE):
    def __init__(self):
        GenericPE.__init__(self)
        self._add_input('input')
        self._add_output('output')

    def _process(self, parameters):
        time = parameters['input'][0]
        var = parameters['input'][1]
        var = np.reshape(var, (var.shape[0], -1))
        result = np.mean(var, axis=1)

        self.write('output', (time, result, self.name))


class CombineData(GenericPE):
    def __init__(self):
        GenericPE.__init__(self)
        self._add_input('input')
        self._add_output('output')
    
    def _process(self, parameters):
        var = parameters['input'][1]
        var = np.reshape(var, (var.shape[0], -1))
        result = np.mean(var, axis=1)

        self.write('output', (parameters['input'], result))


class CombineAndPlot(GenericPE):
    def __init__(self):
        GenericPE.__init__(self)
        self._add_input('var1',grouping=[1])
        self._add_input('var2',grouping=[1])
        self._add_input('var3',grouping=[1])
        self._add_output('output')
        self.var1=0
        self.name1=''
        self.var2=0
        self.name2=''
        self.var3=0
        self.name3=''
        self.count=0

    def _process(self, parameters):
        #var = parameters['input'][1]
        pdb.set_trace()
        name_var = [name for name in parameters.keys()][0]
        #pdb.set_trace()
        if name_var == 'var1':
            self.var1 = parameters[name_var][1]
            self.name1 = parameters[name_var][2]
            self.count+=1
        elif name_var == 'var2':
            self.var2 = parameters[name_var][1]
            self.name2 = parameters[name_var][2]
            self.count+=1
        elif name_var == 'var3':
            self.var3 = parameters[name_var][1]
            self.name3 = parameters[name_var][2]
            self.count+=1
        print("self.count: "+str(self.count))

        if self.count==3:

            #Get year list
            time = parameters[name_var][0]
            year_list = np.array([t.year for t in time])

            plt.figure()
            lines = plt.plot(year_list, self.var1, year_list, self.var2, year_list, self.var3)
            l1, l2, l3 = lines
            plt.setp(lines, linestyle='-')      
            plt.setp(l1, linewidth=1, color='r', label=self.name1) 
            plt.setp(l2, linewidth=1, color='g', label=self.name2)
            plt.setp(l3, linewidth=1, color='b', label=self.name3)
            plt.legend()
            plt.xlabel('Year')
            plt.ylabel(self.name)
            plt.grid()
            name_fig = self.name+".png"
            plt.savefig("/tmp/"+name_fig)
            self.write("output", ("/tmp/"+name_fig, name_fig))
        #pdb.set_trace()
        

class B2DROP(GenericPE):
    def __init__(self):
        GenericPE.__init__(self)
        self._add_input('input')
        self._add_output('output')
    
    def _process(self, parameters):

        import owncloud

        name_dir = "enes_usecase"
        src_path = parameters['input'][0]
        upload_path = name_dir+"/"+parameters['input'][1]
        oc = owncloud.Client('https://b2drop.eudat.eu')

        username = '7f64f56c-a286-48fe-bf74-96567edef0d2'
        password = '76Er8-byEQK-m29Jn-FWmq3-jZm7s'

        oc.login(username, password)
        oc.put_file(upload_path, src_path)

        link_info = oc.share_file_with_link(upload_path)
        print("Shared linked is: "+link_info.get_link())


def create_workflow_icclim():

    ###############################
    #Stream Producer, it will scan the json file in input
    ###############################
    streamProducer = StreamProducer()
    streamProducer.name = 'SU_workflow'


    ###############################
    #Workflow for r1i2p1 simulation
    ###############################
    su_calculation_r1i2p1 = NetCDFProcessing()
    su_calculation_r1i2p1.name = 'SU_calculation_r1i2p1'

    read_r1i2p1 = ReadNetCDF()
    read_r1i2p1.name = "read_SU_r1i2p1"

    mean_calculation_r1i2p1 = AverageData()
    mean_calculation_r1i2p1.name = "mean_r1i2p1"

    std_calc_r1i2p1 = StandardDeviation()
    std_calc_r1i2p1.name = "std_r1i2p1"


    ###############################
    #Workflow for r2i2p1 simulation
    ###############################    
    su_calculation_r2i2p1 = NetCDFProcessing()
    su_calculation_r2i2p1.name = 'SU_calculation_r2i2p1'

    read_r2i2p1 = ReadNetCDF()
    read_r2i2p1.name = "read_SU_r2i2p1"

    mean_calculation_r2i2p1 = AverageData()
    mean_calculation_r2i2p1.name = "mean_r2i2p1"

    std_calc_r2i2p1 = StandardDeviation()
    std_calc_r2i2p1.name = "std_r2i2p1"


    ###############################
    #Workflow for r3i2p1 simulation
    ###############################    
    su_calculation_r3i2p1 = NetCDFProcessing()
    su_calculation_r3i2p1.name = 'SU_calculation_r3i2p1'

    read_r3i2p1 = ReadNetCDF()
    read_r3i2p1.name = "read_SU_r3i2p1"

    mean_calculation_r3i2p1 = AverageData()
    mean_calculation_r3i2p1.name = "mean_r3i2p1"

    std_calc_r3i2p1 = StandardDeviation()
    std_calc_r3i2p1.name = "std_r3i2p1"


    ###############################
    #Workflow to combine and plot the calculation together
    ###############################
    combine_std = CombineAndPlot()
    combine_std.name = "SU_Spatial_STD"   

    combine_mean = CombineAndPlot()
    combine_mean.name = "SU_Spatial_MEAN"   


    ###############################
    #Workflow to combine and plot the calculation together
    ###############################   
    b2drop = B2DROP()
    b2drop.name = "b2drop_storage"

    ###############################
    #Workflow starts here
    ###############################
    graph = WorkflowGraph()

    #Calculation for r1i2p1
    graph.connect(streamProducer, 'output', su_calculation_r1i2p1, 'input')
    graph.connect(su_calculation_r1i2p1, 'output', read_r1i2p1, 'input')
    graph.connect(read_r1i2p1, 'output', mean_calculation_r1i2p1, 'input')
    graph.connect(read_r1i2p1, 'output', std_calc_r1i2p1, 'input')

    #Calculation for r2i2p1
    graph.connect(streamProducer, 'output', su_calculation_r2i2p1, 'input')
    graph.connect(su_calculation_r2i2p1, 'output', read_r2i2p1, 'input')  
    graph.connect(read_r2i2p1, 'output', mean_calculation_r2i2p1, 'input')
    graph.connect(read_r2i2p1, 'output', std_calc_r2i2p1, 'input')

    #Calculation for r3i2p1
    graph.connect(streamProducer, 'output', su_calculation_r3i2p1, 'input')
    graph.connect(su_calculation_r3i2p1, 'output', read_r3i2p1, 'input')
    graph.connect(read_r3i2p1, 'output', mean_calculation_r3i2p1, 'input')
    graph.connect(read_r3i2p1, 'output', std_calc_r3i2p1, 'input')

    ###############################
    #We combine the standard deviation and plot it together. Same for the average.
    ###############################
    graph.connect(std_calc_r1i2p1, 'output', combine_std, 'var1')
    graph.connect(std_calc_r2i2p1, 'output', combine_std, 'var2')
    graph.connect(std_calc_r3i2p1, 'output', combine_std, 'var3')

    graph.connect(mean_calculation_r1i2p1, 'output', combine_mean, 'var1')
    graph.connect(mean_calculation_r2i2p1, 'output', combine_mean, 'var2')
    graph.connect(mean_calculation_r3i2p1, 'output', combine_mean, 'var3')

    ###############################
    #We store all the results on b2drop
    ###############################
    graph.connect(combine_std, 'output', b2drop, 'input')
    graph.connect(combine_mean, 'output', b2drop, 'input')

    return graph


graph = create_workflow_icclim()

"""from dispel4py.new import simple_process

with open('test_multiple.json') as inputfile:
    input_data = json.load(inputfile)

result = simple_process.process_and_return(graph, input_data)

from dispel4py.workflow_graph import drawDot
with open('graph.png', 'wb') as f:
   f.write(drawDot(graph)) 
"""
