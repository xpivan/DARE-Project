'''
Simple wrapper for icclim.indice.

Run as follows:
``python -m dispel4py.new.processor simple ./dispel4py_icclim.py -f node_input.json``
'''

from dispel4py.workflow_graph import WorkflowGraph
from dispel4py.base import IterativePE, ProducerPE, ConsumerPE
from dispel4py.core import GenericPE
import icclim


class NetCDFProcessing(GenericPE):

    def __init__(self):
        GenericPE.__init__(self)
        self._add_input('input')
        self._add_output('output')

    def _process(self, parameters):

        icclim.indice(**parameters['input'][self.name])
        self.write('output', parameters['input'])


class StreamProducer(GenericPE):
    def __init__(self):
        GenericPE.__init__(self)
        self._add_output('output')

    def _process(self, inputs):         
        list_calcul = inputs.keys()
        len_lc = len(list_calcul)

        for name_calc in list_calcul:
            inputs[name_calc]['out_file'] = name_calc+'.nc'
        
        self.write('output', inputs)


class NetCDFLastProcessing(GenericPE):

    def __init__(self):
        GenericPE.__init__(self)
        self._add_input('input')
        self._add_output('output')

    def _process(self, parameters):

        icclim.indice(**parameters['input'][self.name])
        self.write('output', parameters['input'])

        
su_calculation = NetCDFProcessing()
su_calculation.name = 'SU_calculation'

mean_calculation = NetCDFProcessing()
mean_calculation.name = "Average_SU"

streamProducer = StreamProducer()
streamProducer.name = 'SU_workflow'

graph = WorkflowGraph()

graph.connect(streamProducer, 'output', su_calculation, 'input')
graph.connect(su_calculation, 'output', mean_calculation, 'input')

