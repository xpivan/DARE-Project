# Native imports
import math
import os

# dispel4py imports
from dispel4py.examples.graph_testing import testing_PEs as t
from dispel4py.workflow_graph import WorkflowGraph
from dispel4py.core import GenericPE, NAME, TYPE, GROUPING
from dispel4py.base import SimpleFunctionPE, IterativePE, BasePE
from dispel4py.provenance import *

##############################################################################
#  Class definition from file ENES_usecase/pe_enes.py (start)
##############################################################################

###############################
#Stream Producer, it will scan the json file in input
###############################

class StreamProducer(GenericPE):
    def __init__(self):
        GenericPE.__init__(self)
        self._add_output('output')

    def _process(self, inputs):
        list_PE = inputs.keys()
        len_lc = len(list_PE)

        #get processing element NetCDFProcessing
        inputs = get_netCDFProcessing(list_PE, inputs)

        #get

        #Sort the Processing Element on the right order
        new_inputs = check_order(inputs)
        self.write('output', new_inputs)



class NetCDFProcessing(GenericPE):
    def __init__(self):
        GenericPE.__init__(self)
        self._add_input('input')
        self._add_output('output')
        self.count=0

    def _process(self, parameters):
        if type(parameters['input']) is tuple:
            param = parameters['input'][0]
            param[self.name]['in_files'] = parameters['input'][1]
        else:
            param = parameters['input']

        icclim.indice(**param[self.name])
        self.count+=1
        print(param[self.name])

        self.write('output', (param,
                              param[self.name]['out_file'],
                              param[self.name]['indice_name']))

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
        name_var = parameters.keys()[0]

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


class B2DROP(GenericPE):
    def __init__(self):
        GenericPE.__init__(self)
        self._add_input('input')
        self._add_output('output')

    def _process(self, parameters):
        import owncloud

        param = parameters['input'][0]
        name_dir = "enes_usecase"

        if isinstance(param, str):
            username = "FILL THIS SECTION"
            password = "FILL THIS SECTION"
            src_path = parameters['input'][0]
            upload_path = name_dir+"/"+parameters['input'][1]
        else:
            param_keys = parameters['input'][0].keys()
            username = parameters['input'][0][self.name]['username']
            password = parameters['input'][0][self.name]['password']
            src_path = param[param_keys[-2]]['out_file']
            pdb.set_trace()
            upload_path = remove_absolute_path(src_path, '/')
            upload_path = name_dir+"/"+upload_path

        oc = owncloud.Client('https://b2drop.eudat.eu')

        oc.login(username, password)

        oc.put_file(upload_path, src_path)

        link_info = oc.share_file_with_link(upload_path)
        print("Shared linked is: "+link_info.get_link())

##############################################################################
#  Workflow creation from ENES_usecase/PreProcess.py
##############################################################################

def create_multiple_scenario_workflow():
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


graph = create_multiple_scenario_workflow()

##############################################################################
#  Provenance from ENES_usecase/PreProcess.py 
##############################################################################

#Store via service

ProvenanceType.REPOS_URL='http://'+os.getenv('SPROV_SERVICE_HOST')+':'+os.getenv('SPROV_SERVICE_PORT')+'/workflowexecutions/insert'
ProvenanceType.PROV_EXPORT_URL='http://'+os.getenv('SPROV_SERVICE_HOST')+':'+os.getenv('SPROV_SERVICE_PORT')+'/workflowexecutions/'

#Store to local path
ProvenanceType.PROV_PATH='./prov-files/'

#Size of the provenance bulk before sent to storage or sensor
ProvenanceType.BULK_SIZE=1

prov_config =  {
    'provone:User': "cc4idev",
    's-prov:description' : "enes_multiple_scenarios",
    's-prov:workflowName': "enes_multiple_scenarios",
    's-prov:workflowType': "climate:preprocess",
    's-prov:workflowId'  : "workflow process",
    's-prov:save-mode'   : 'service'         ,
    's-prov:WFExecutionInputs':  [{
        "url": "",
        "mime-type": "text/json",
        "name": "input_data"

    }],
    # defines the Provenance Types and Provenance Clusters for the Workflow Components
   #  's-prov:componentsType' :
   #      {self.calc_operation.getValue()+'_Spatial_MEAN': {'s-prov:type':(AccumulateFlow,),
   #                                                        's-prov:prov-cluster':'enes:Processor'},
   #       self.calc_operation.getValue()+'_Spatial_STD': {'s-prov:type':(AccumulateFlow,),
   #                                                       's-prov:prov-cluster':'enes:Processor'},
   #       self.calc_operation.getValue()+'_workflow': {'s-prov:type':(icclimInputParametersDataProvType,),
   #                                                    's-prov:prov-cluster':'enes:dataHandler'},
         #             'PE_filter_bandpass': {'s-prov:type':(SeismoPE,),
         #                                                     's-prov:prov-cluster':'seis:Processor'},
         #             'StoreStream':    {'s-prov:prov-cluster':'seis:DataHandler',
         #                                's-prov:type':(SeismoPE,)},
    #     }
    #            's-prov:sel-rules': None
}

# rid='JUP_ENES_PREPOC_'+getUniqueId()

# self.status.set("Initialising Provenance...", 25)

#Initialise provenance storage to service:
configure_prov_run(graph,
                   provImpClass=(ProvenanceType,),
                   input=prov_config['s-prov:WFExecutionInputs'],
                   username=prov_config['provone:User'],
                   runId=os.getenv('RUN_ID'),
                   description=prov_config['s-prov:description'],
                   workflowName=prov_config['s-prov:workflowName'],
                   workflowType=prov_config['s-prov:workflowType'],
                   workflowId=prov_config['s-prov:workflowId'],
                   save_mode=prov_config['s-prov:save-mode'],
                   # componentsType=prov_config['s-prov:componentsType']
                   #                           sel_rules=prov_config['s-prov:sel-rules']
                   )

