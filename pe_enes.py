'''
Run as follows:
``python -m dispel4py.new.processor simple ./usecase.py -f usecase_input.json``
'''
#!/usr/bin/env python

from dispel4py.workflow_graph import WorkflowGraph
from dispel4py.base import IterativePE, ProducerPE, ConsumerPE
from dispel4py.core import GenericPE
import pdb

from collections import OrderedDict

save_path = '/tmp/'

def check_order(inputs):
    list_key = [*inputs]
    list_key.sort()
    new_inputs = OrderedDict()
    for input_name in list_key:
        new_inputs[input_name] = inputs[input_name]
    return new_inputs


def remove_absolute_path(string_name, charact):
    pos_char = [pos for pos, char in enumerate(string_name) if char == charact]
    return string_name[pos_char[-1]+1::]

def map_multiple_scenario(inputs):
    #create dictionary to map the scenario
    first_node = [*inputs][0]
    list_scenario = inputs[first_node]['in_files']
    nb_scenario = len(list_scenario)

    map_scenario = OrderedDict()
    map_out_files = OrderedDict()

    for scenario in range(nb_scenario):
        map_scenario['scenario_'+str(scenario+1)] = list_scenario[scenario]
        map_out_files['scenario_'+str(scenario+1)] = 'scenario_'+str(scenario+1)+'.nc'

    inputs['in_files'] = map_scenario
    inputs['out_file'] = map_out_files
    inputs['indice_name'] = inputs[first_node]['indice_name']

    return inputs

def get_netCDFProcessing(list_PE, inputs):

    test = ["NetCDFProcessing" in s for s in list_PE]

    #check if there's any known processing element NetCDFProcessing
    ncdf_bool = [bool_ for bool_, x in enumerate(test) if x]
    if ncdf_bool:
        i = 0
        for ncdf in test:
            if ncdf and inputs[list_PE[i]]['out_file'] is None:
                inputs[list_PE[i]]['out_file'] = save_path+list_PE[i]+'.nc'
            i+=1
    else:
        for l_pe in list_PE:
            inputs[l_pe]['out_file'] = save_path+l_pe+'.nc'

    return inputs


class IcclimProcessing(GenericPE):
    def __init__(self):
        GenericPE.__init__(self)
        self._add_input('input')
        self._add_output('output')

    def _process(self, parameters):
        #Find PE named PE{num}_IcclimProcessing

        from icclim import icclim

        ind_scenario = self.name.find("scenario_")
        name_scenario = self.name[ind_scenario::]
        name_node = self.name[:ind_scenario-1]

        param = parameters['input'][name_node]
        path_files = parameters['input']

        icclim_param = {
            'indice_name':param['indice_name'],
            'slice_mode':param['slice_mode'],
            'var_name':param['var_name'],
            'in_files':path_files['in_files'][name_scenario],
            'out_file':path_files['out_file'][name_scenario]
        }

        icclim.indice(**icclim_param)

        self.write('output', ({'out_file':path_files['out_file'][name_scenario],
        'indice_name':param['indice_name']}))

class PreProcess_multiple_scenario(GenericPE):
    def __init__(self):
        GenericPE.__init__(self)
        self._add_output('output')    

    def _process(self, inputs):
        #Map the scenario in an ordererdict
        inputs = map_multiple_scenario(inputs)

        #We sort the processing element in inputs to be
        new_inputs = check_order(inputs)

        self.write('output', new_inputs)



class StreamProducer(GenericPE):
    def __init__(self):
        GenericPE.__init__(self)
        self._add_output('output')

    def _process(self, inputs):
        list_PE = inputs.keys()
        len_lc = len(list_PE)

        #get processing element NetCDFProcessing
        inputs = get_netCDFProcessing(list_PE, inputs)

        #Sort the Processing Element on the right order
        new_inputs = check_order(inputs)
        self.write('output', new_inputs)


class NetCDF2xarray(GenericPE):
    def __init__(self):
        GenericPE.__init__(self)
        self._add_input('input')
        self._add_output('output')

    def _process(self, parameters):
        import xarray as xr

        ds = xr.open_dataset(parameters['input'][1]['result_nc'])

        self.write('output', (ds, [parameters['input'][-1]]))


class ReadNetCDF(GenericPE):
    def __init__(self):
        GenericPE.__init__(self)
        self._add_input('input')
        self._add_output('output')

    def _process(self, parameters):
        #Load the netcdf file
        from netCDF4 import Dataset
        import netcdftime

        nc = Dataset(parameters['input'][1]['result_nc'])

        #Extracting the time and change the time format from num to date time
        time = nc.variables['time']
        nc_time = netcdftime.utime(time.units, time.calendar)
        date_time = nc_time.num2date(time[:])

        var = nc.variables[parameters['input'][0]['indice_name']][:]

        self.write('output', (date_time, var))


class StandardDeviation(GenericPE):
    def __init__(self):
        GenericPE.__init__(self)
        self._add_input('input')
        self._add_output('output')

    def _process(self, parameters):

        from netCDF4 import Dataset
        import netcdftime

        nc = Dataset(parameters['input']['out_file'])

        #Extracting the time and change the time format from num to date time
        time = nc.variables['time']
        nc_time = netcdftime.utime(time.units, time.calendar)
        date_time = nc_time.num2date(time[:])

        var = nc.variables[parameters['input']['indice_name']][:]
        import numpy as np
        #time = parameters['input'][0]
        #var = parameters['input'][1]
        var = np.reshape(var, (var.shape[0], -1))
        result = np.std(var, axis=1)

        self.write('output',  (time, result, self.name))


class AverageData(GenericPE):
    def __init__(self):
        GenericPE.__init__(self)
        self._add_input('input')
        self._add_output('output')

    def _process(self, parameters):
        from netCDF4 import Dataset
        import netcdftime

        ind_scenario = self.name.find("scenario_")
        name_scenario = self.name[ind_scenario::]

        nc = Dataset(parameters['input']['out_file'])

        #Extracting the time and change the time format from num to date time
        time = nc.variables['time']
        nc_time = netcdftime.utime(time.units, time.calendar)
        date_time = nc_time.num2date(time[:])

        var = nc.variables[parameters['input']['indice_name']][:]
        import numpy as np
        #time = parameters['input'][0]
        #var = parameters['input'][1]
        var = np.reshape(var, (var.shape[0], -1))
        result = np.mean(var, axis=1)

        self.write('output', (time, result, self.name))


class CombineData(GenericPE):
    def __init__(self, nb_scenario):
        GenericPE.__init__(self)
        self._add_input('input')
        self._add_output('output')
    
    def _process(self, parameters):
        import numpy as np

        var = parameters['input'][1]
        var = np.reshape(var, (var.shape[0], -1))
        result = np.mean(var, axis=1)

        self.write('output', (parameters['input'], result))


class CombineScenario(GenericPE):
    def __init__(self, nb_scenario):
        GenericPE.__init__(self)

        import numpy as np

        for i in range(nb_scenario):
            name_scenario = 'scenario_'+str(i+1)
            self._add_input(name_scenario,grouping=[1])

        self.nb_scenario = nb_scenario
        self._add_output('output')
        self.mat=0
        self.time=0
        self.count=0
        
    def _process(self, inputs):
        import numpy as np
        
        name_scenario = [*inputs][0]

        if self.count==0:
            self.time = inputs[name_scenario][0]
            var = inputs[name_scenario][1]
            self.mat = np.zeros((self.nb_scenario,len(var)))

            #We move from num to date format because if we send a netcdf variable format we have this error:
            #*** NotImplementedError: Variable is not picklable
            import netcdftime
            t = netcdftime.utime(self.time.units, self.time.calendar) 
            self.time = t.num2date(self.time[:]) 

        self.mat[self.count,:]=inputs[name_scenario][1]
        self.count+=1

        if self.count==(self.nb_scenario):
            self.write('output', (self.time,self.mat))


class PlotMultipleScenario(GenericPE):
    def __init__(self):
        GenericPE.__init__(self)
        self._add_input('input')


    def _process(self, parameters):

        import matplotlib.pyplot as plt
        import numpy as np
        #plt.switch_backend('agg')
        name_var = [*parameters][0]

        time = parameters[name_var][0]
        var = parameters[name_var][1]
        year_list = np.array([t.year for t in time])
        #year_array = np.tile(year_list,(len(var),1))


        plt.figure()
        for i in range(len(var)):
            plt.plot(year_list, var[i,:], label='scenario_'+str(i+1))
        plt.legend()
        plt.xlabel('Year')
        plt.ylabel(self.name)
        plt.grid()

        name_fig = self.name+".png"
        plt.savefig("/tmp/"+name_fig)

        self.write("output", ("/tmp/"+name_fig, name_fig))


class B2DROP(GenericPE):
    def __init__(self, id):
        GenericPE.__init__(self)
        self._add_input('input')
        self._add_output('output')
        self.username = id['username']
        self.password = id['password']

    def _process(self, parameters):
        import owncloud
        
        param = parameters['input'][0]
        name_dir = "enes_usecase"

        if isinstance(param, str):
            src_path = parameters['input'][0]
            upload_path = name_dir+"/"+parameters['input'][1]
        else:
            param_keys = parameters['input'][0].keys()
            src_path = param[param_keys[-2]]['out_file'] 
            upload_path = remove_absolute_path(src_path, '/')
            upload_path = name_dir+"/"+upload_path

        oc = owncloud.Client('https://b2drop.eudat.eu')

        oc.login(self.username, self.password)

        oc.put_file(upload_path, src_path)

        link_info = oc.share_file_with_link(upload_path)
        print("Shared linked is: "+link_info.get_link())