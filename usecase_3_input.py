'''
Simple wrapper for icclim.indice.

Run as follows:
``python -m dispel4py.new.processor simple ./usecase_3_input.py -f node_input.json``
'''

from dispel4py.workflow_graph import WorkflowGraph
from dispel4py.base import IterativePE, ProducerPE, ConsumerPE
from dispel4py.core import GenericPE
import pdb
import icclim
from netCDF4 import Dataset

save_path = '/Users/xavier/Projets/data/test/results/'

class NetCDFProcessing(GenericPE):

    def __init__(self):
        GenericPE.__init__(self)
        self._add_input('input')
        self._add_output('output')
        self.count=0

    def _process(self, parameters):

        icclim.indice(**parameters['input'][self.name])

        self.count+=1
        self.write('output', parameters['input'])


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


class ReadDataInput(GenericPE):
    def __init__(self):
        GenericPE.__init__(self)
        self._add_input('input')
        self._add_output('output')

    def _process(self, inputs):         
        list_calcul = inputs.keys()
        len_lc = len(list_calcul)
        for name_calc in list_calcul:
            pdb.set_trace()
            inputs[name_calc]['out_file'] = name_calc+'.nc'
        
        self.write('output', inputs)

class ReadNetCDF():
    def __init__(self):
        GenericPE.__init__(self)
        self._add_input('input')
        self._add_output('output')

    def _process(self, parameters):
        nc = Dataset(file)
        var = nc.variables[name_var][:]
        self.write('output', var)

class StandardDeviationArray():
    def __init__(self):
        GenericPE.__init__(self)
        self._add_input('input')
        self._add_output('output')

    def _process(self, parameters):
        nc = Dataset(file)
        var = nc.variables[name_var][:]
        self.write('output', var)

class AverageMutipleArrayTogether():
    def __init__(self):
        GenericPE.__init__(self)
        self._add_input('input')
        self._add_output('output')

    def _process(self, parameters):
        nc = Dataset(file)
        var = nc.variables[name_var][:]
        self.write('output', var)

def create_workflow_icclim():

    su_calculation_r1i2p1 = NetCDFProcessing()
    su_calculation_r1i2p1.name = 'SU_calculation_r1i2p1'

    mean_calculation_r1i2p1 = NetCDFProcessing()
    mean_calculation_r1i2p1.name = "Average_SU_r1i2p1"

    su_calculation_r2i2p1 = NetCDFProcessing()
    su_calculation_r2i2p1.name = 'SU_calculation_r2i2p1'

    mean_calculation_r2i2p1 = NetCDFProcessing()
    mean_calculation_r2i2p1.name = "Average_SU_r2i2p1"

    su_calculation_r3i2p1 = NetCDFProcessing()
    su_calculation_r3i2p1.name = 'SU_calculation_r3i2p1'

    mean_calculation_r3i2p1 = NetCDFProcessing()
    mean_calculation_r3i2p1.name = "Average_SU_r3i2p1"

    streamProducer = StreamProducer()
    streamProducer.name = 'SU_workflow'

    graph = WorkflowGraph()

    graph.connect(streamProducer, 'output', su_calculation_r1i2p1, 'input')
    graph.connect(su_calculation_r1i2p1, 'output', mean_calculation_r1i2p1, 'input')

    graph.connect(streamProducer, 'output', su_calculation_r2i2p1, 'input')
    graph.connect(su_calculation_r2i2p1, 'output', mean_calculation_r2i2p1, 'input')  

    graph.connect(streamProducer, 'output', su_calculation_r3i2p1, 'input')
    graph.connect(su_calculation_r3i2p1, 'output', mean_calculation_r3i2p1, 'input')

    return graph

graph = create_workflow_icclim()

