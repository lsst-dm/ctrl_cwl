# This file is part of pipe_base.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
from __future__ import annotations

__all__ = ("CWLBuilder", )

from base64 import b64encode
import networkx as nx
import pickle
from typing import TYPE_CHECKING, MutableMapping, Tuple, Union, Mapping, List
import yaml
import zlib

from lsst.daf.butler import Butler
from .. import TaskDef

if TYPE_CHECKING:
    from . import QuantumGraphNew
    from . import QuantumNode

MapOrList = Union[Mapping, List]


def make_step(*, inField: MapOrList, outField: MapOrList, command: str, stdout: str, inputs: Mapping,
              output_key: str) -> Mapping:
    step: MutableMapping = {}
    step['in'] = inField
    step['out'] = outField
    step['run'] = {}
    run = step['run']
    run['class'] = 'CommandLineTool'
    run['baseCommand'] = command
    run['stdout'] = stdout
    run['inputs'] = inputs
    run['outputs'] = {output_key: {"type": "stdout"}}
    return step


class CWLBuilder:
    def __init__(self, butler: Butler, quantumGraph: QuantumGraphNew):
        self._cwl_file: MutableMapping = {}
        self._cwl_data: MutableMapping = {}
        # Add all the top level info
        self._add_header()
        # This will contain all the individual steps
        self._cwl_file['steps'] = {}

        # setup the initialization job
        self._add_init(list(nx.topological_sort(quantumGraph.taskGraph)))

        # setup all the quanta
        for quantum in quantumGraph:
            self.add_quantum(quantum, quantumGraph.graph)

        # put the info in for the butler
        self._cwl_data['butler'] = b64encode(pickle.dumps(butler)).decode()

    def _add_header(self):
        # Add in the top level common stages
        self._cwl_file['cwlVersion'] = 'v1.0'
        self._cwl_file['class'] = 'Workflow'
        self._cwl_file['inputs'] = {"butler": "string", "pipeline": "string"}
        self._cwl_file['outputs'] = {}

    def _add_init(self, pipeline: List[TaskDef]):
        """
        init-job:
          in:
            butler: butler
            pipeline: pipeline
          out: [init-job-output]
          run:
            class: CommandLineTool
            baseCommand: cwlInit
            stddout: init-output.txt
            inputs:
              pipeline:
                type: string
                inputBinding:
                  position: 1
            outputs:
              init-job-output:
                type: stdout
        """
        step = make_step(inField={"butler": "butler", "pipeline": "pipeline"},
                         outField=['init_job_output'],
                         command="cwlInit",
                         stdout="init_output.txt",
                         inputs={"pipeline": {"type": "string", "inputBinding": {"position": 1}},
                                 "butler": {"type": "string", "inputBinding": {"position": 2}}},
                         output_key="init_job_output")

        self._cwl_file['steps']['init_job'] = step

        self._cwl_data['pipeline'] = b64encode(pickle.dumps(pipeline)).decode()

    def add_quantum(self, quantum: QuantumNode, graph: nx.DiGraph):
        """
        This creates nodes that look like this:
        {quantum}-job:
          in:
            dependencies:
              -{quantum}-job/output
              -{quantum}-job/output
            {quantum}-butler: butler
          out: [output]
          run:
            class: CommandLineTool
            baseCommand: cwlExecutor
            stdout: {quantum}-ouput.txt
            inputs:
              {quantum}-quantum:
                type: String
                inputBinding:
                  position: 1
              {quantum}-butler:
                type: String
                inputBinding:
                  position: 2
              dependencies:
                type: array
                items: File
                inputBinding:
                  prefix: --deps
                  separate: false
            outputs:
              output:
                type: stdout

        """
        qhash = abs(hash(quantum))
        predecessors = [f"{abs(hash(pred))}_job/{abs(hash(pred))}_output" for pred in graph.predecessors(quantum)]
        if len(predecessors) == 0:
            predecessors.append("init_job/init_job_output")
        predDict = {"source": predecessors}
        if len(predecessors) == 1:
            predDict.update({"linkMerge": "merge_nested"})

        step = make_step(inField={"dependencies": predDict,
                                  f"{qhash}_butler": 'butler',
                                  f"{qhash}_quantum": f"{qhash}_quantum"},
                         outField=[f"{qhash}_output"],
                         command="cwlExecutor",
                         stdout=f"{qhash}_output.txt",
                         inputs={f'{qhash}_quantum': {"type": "string", "inputBinding": {"position": 1}},
                                 f'{qhash}_butler': {"type": "string", "inputBinding": {"position": 2}},
                                 'dependencies': {"type": "File[]",
                                                  "inputBinding": {"prefix": "--deps=",
                                                                   "itemSeparator": ",",
                                                                   "separate": False}}},
                         output_key=f"{qhash}_output")

        # attach this created quantum
        self._cwl_file['steps'][f"{qhash}_job"] = step
        self._cwl_file['inputs'].update({f"{qhash}_quantum": "string"})
        self._cwl_file['outputs'].update({f"{qhash}_output": {"type": "File",
                                                              "outputSource": f"{qhash}_job/{qhash}_output"}})

        # updated the datafile
        self._cwl_data[f'{qhash}_quantum'] = b64encode(zlib.compress(pickle.dumps(quantum))).decode()

    def to_yaml_strings(self) -> Tuple[str, str]:
        return (yaml.dump(self._cwl_file, sort_keys=False),
                yaml.dump(self._cwl_data, sort_keys=False))
