import os
from copy import deepcopy

import numpy
import pandas as pd

from conf.conf import CFG
from lib.utils.graph import Graph, get_sources_sinks


class Metrics(object):
    def __init__(self, app_file='', container_file='', hardware_file='', network_file='', traffic_file='',
                 operator_connections_file='', state_management_file='', reconfiguration_file='',
                 application_state_file='',
                 input_directory="", name='', data_center=""):
        """
        Args:
            app_file: the directory of the application statistics
            container_file: directory of the container statistics
            hardware_file: directory of the hardware statistics
            network_file: directory of the network statistics
            operator_connections_file: directory of the operator connections statistics
        """
        #
        self.Name = name
        self.Bandwidth = pd.DataFrame()
        self.Throughput_Latency = pd.DataFrame()

        self.Reconfiguration = pd.DataFrame()

        # Reads and converts application statistics
        self.Application = pd.read_csv(app_file, sep=';', header=None)
        self.Application.columns = ['time_stamp', 'node_id', 'latency', 'min_latency', 'max_latency', 'std_latency',
                                    'median_latency', 'operator_id', 'sequence_id', 'output_rate',
                                    'topology_id', 'input_rate', 'input_msg_size', 'output_msg_size', 'processing_time',
                                    'batch', 'buffer1', 'buffer2', 'origin']
        self.Application['ope_id'] = self.Application['operator_id'].str.slice(0, 4)
        self.Application['processing_time'] /= 1000000
        self.time_zero = self.Application['time_stamp'].min()

        if CFG.annotate_reconfiguration or CFG.details:
            # Reads and converts the reconfiguration statistics
            self.Reconfiguration = pd.read_csv(reconfiguration_file, sep=';', header=None)
            self.Reconfiguration.columns = ['time_stamp', 'sequence_id', 'topology_id', 'node_id', 'operator_id',
                                            'operation', 'source_creation_timestamp']
            t = self.Reconfiguration['time_stamp'].min()
            if t < self.time_zero:
                self.time_zero = t

        if CFG.details:
            self.Container = pd.DataFrame()
            if os.stat(container_file).st_size > 0:
                # Reads and converts the container statistics
                self.Container = pd.read_csv(container_file, sep=';', header=None)
                self.Container.columns = ['time_stamp', 'node_id', 'container_id', 'container_name',
                                          'container_allocation',
                                          'container_cpu_utilization', 'container_allocated_cpu_core_count',
                                          'container_memory_usage', 'container_memory_limit']
                self.Container['cpu_utilization_per_core'] = self.Container['container_cpu_utilization'] / \
                                                             self.Container[
                                                                 'container_allocated_cpu_core_count']
                self.Container['mem_percentage_utilization'] = (self.Container['container_memory_usage'] /
                                                                self.Container[
                                                                    'container_memory_limit']) * 100
                t = self.Container['time_stamp'].min()
                if t < self.time_zero:
                    self.time_zero = t

            self.Hardware = pd.DataFrame()
            if CFG.plot_hardware and os.stat(hardware_file).st_size > 0:
                # Reads and converts the hardware statistics
                self.Hardware = pd.read_csv(hardware_file, sep=';', header=None)
                self.Hardware.columns = ['time_stamp', 'worker_name', 'processor', 'slots', 'mem_available',
                                         'mem_total',
                                         'mem_free', 'os', 'type']
                self.Hardware['mem_available'] *= 1024
                self.Hardware['mem_total'] *= 1024
                self.Hardware['mem_free'] *= 1024
                t = self.Hardware['time_stamp'].min()
                if t < self.time_zero:
                    self.time_zero = t

            self.Network = pd.DataFrame()
            if os.stat(network_file).st_size > 0:
                # Reads and converts the network statistics
                self.Network = pd.read_csv(network_file, sep=';', header=None)
                self.Network.columns = ['time_stamp', 'source_device_id', 'dst_device_id', 'ip', 'link_name',
                                        'bandwidth',
                                        'latency']
                self.Network['bandwidth'] *= 1000000
                self.Network = self.Network[(self.Network['source_device_id'] != self.Network['dst_device_id'])]
                t = self.Network['time_stamp'].min()
                if t < self.time_zero:
                    self.time_zero = t

            self.StateManagement = pd.DataFrame()
            if os.stat(state_management_file).st_size > 0:
                # Reads and converts the state_management statistics
                self.StateManagement = pd.read_csv(state_management_file, sep=';', header=None)
                self.StateManagement.columns = ['time_stamp', 'sequence_id', 'topology_id', 'node_id', 'operator_id',
                                                'operation_type', 'keys', 'size_bytes', 'items', 'duration']
                t = self.StateManagement['time_stamp'].min()
                if t < self.time_zero:
                    self.time_zero = t

            self.ApplicationState = pd.DataFrame()
            if os.stat(application_state_file).st_size > 0:
                # Reads and converts the ApplicationState statistics
                self.ApplicationState = pd.read_csv(application_state_file, sep=';', header=None)
                self.ApplicationState.columns = ['time_stamp', 'sequence_id', 'topology_id', 'node_id', 'operator_id',
                                                 'state_type', 'key', 'size_bytes', 'items', 'windowing_time']
                t = self.ApplicationState['time_stamp'].min()
                if t < self.time_zero:
                    self.time_zero = t

            # Reads and converts the traffic statistics
            self.Traffic = pd.DataFrame()
            if CFG.plot_traffic and os.stat(traffic_file).st_size > 0:
                self.Traffic = pd.read_csv(traffic_file, sep=';', header=None)
                self.Traffic.columns = ['time_stamp', 'task_manager_id', 'parent_task_manager_id', 'source_device_id',
                                        'dst_device_id', 'transferred_data']
                self.Traffic['link'] = self.Traffic['task_manager_id'].map(str) + '->' + \
                                       self.Traffic['parent_task_manager_id'].map(str)
                self.Traffic['transferred_data'] /= 1000000  # MB
                t = self.Traffic['time_stamp'].min()
                if t < self.time_zero:
                    self.time_zero = t

        # Reads and converts the operator connections statistics
        self.OperatorConnections = pd.read_csv(operator_connections_file, sep=';', header=None)
        self.OperatorConnections.columns = ['time_stamp', 'sequence_id', 'topology_id', 'node_id',
                                            'transferring_time', 'min_transferring_time', 'max_transferring_time',
                                            'std_transferring_time', 'median_transferring_time',
                                            'transferring_size', 'transferring_counter', 'operator_id',
                                            'previous_operator_id']
        self.OperatorConnections['dst_id'] = self.OperatorConnections['operator_id'].str.slice(0, 4)
        self.OperatorConnections['previous_operator_id'] = self.OperatorConnections['previous_operator_id'].astype(str)
        self.OperatorConnections['src_id'] = self.OperatorConnections['previous_operator_id'].str.slice(0, 4)

        self.OperatorConnections['connection'] = self.OperatorConnections['previous_operator_id'].map(str) + '->' + \
                                                 self.OperatorConnections['operator_id'].map(str)

        self.OperatorConnections['connection_code'] = self.OperatorConnections['src_id'].map(str) + '->' + \
                                                      self.OperatorConnections['dst_id'].map(str)
        t = self.OperatorConnections['time_stamp'].min()
        if t < self.time_zero:
            self.time_zero = t

        # set up initial time
        self.Application['time_stamp'] = ((self.Application['time_stamp'] - self.time_zero) / 1000).astype(int)
        maxTimestamp = self.Application['time_stamp'].max()

        if CFG.annotate_reconfiguration or CFG.details:
            if self.Reconfiguration.size > 0:
                self.Reconfiguration['time_stamp'] = (
                        (self.Reconfiguration['time_stamp'] - self.time_zero) / 1000).astype(int)
                self.Reconfiguration['source_creation_timestamp'] = (
                        (self.Reconfiguration['source_creation_timestamp'] - self.time_zero) / 1000).astype(int)
                MaxTimestamp2 = self.Reconfiguration['time_stamp'].max()
                if maxTimestamp < MaxTimestamp2:
                    maxTimestamp = MaxTimestamp2

        if CFG.details:
            if self.Container.size > 0:
                self.Container['time_stamp'] = ((self.Container['time_stamp'] - self.time_zero) / 1000).astype(int)
                # MaxTimestamp2 = self.Container['time_stamp'].max()
                # if maxTimestamp < MaxTimestamp2:
                #     maxTimestamp = MaxTimestamp2

            if self.StateManagement.size > 0:
                self.StateManagement['time_stamp'] = (
                        (self.StateManagement['time_stamp'] - self.time_zero) / 1000).astype(int)
                # MaxTimestamp2 = self.StateManagement['time_stamp'].max()
                # if maxTimestamp < MaxTimestamp2:
                #     maxTimestamp = MaxTimestamp2

            if self.ApplicationState.size > 0:
                self.ApplicationState['time_stamp'] = (
                        (self.ApplicationState['time_stamp'] - self.time_zero) / 1000).astype(int)
                MaxTimestamp2 = self.ApplicationState['time_stamp'].max()
                if maxTimestamp < MaxTimestamp2:
                    maxTimestamp = MaxTimestamp2

            if CFG.plot_hardware and self.Hardware.size > 0:
                self.Hardware['time_stamp'] = ((self.Hardware['time_stamp'] - self.time_zero) / 1000).astype(int)
                MaxTimestamp2 = self.Hardware['time_stamp'].max()
                if maxTimestamp < MaxTimestamp2:
                    maxTimestamp = MaxTimestamp2

            if self.Network.size > 0:
                self.Network['time_stamp'] = ((self.Network['time_stamp'] - self.time_zero) / 1000).astype(int)
                # MaxTimestamp2 = self.Network['time_stamp'].max()
                # if maxTimestamp < MaxTimestamp2:
                #     maxTimestamp = MaxTimestamp2

            if CFG.plot_traffic and self.Traffic.size > 0:
                self.Traffic['time_stamp'] = ((self.Traffic['time_stamp'] - self.time_zero) / 1000).astype(int)

                MaxTimestamp2 = self.Traffic['time_stamp'].max()
                if maxTimestamp < MaxTimestamp2:
                    maxTimestamp = MaxTimestamp2

        self.OperatorConnections['time_stamp'] = (
                (self.OperatorConnections['time_stamp'] - self.time_zero) / 1000).astype(int)

        MaxTimestamp2 = self.OperatorConnections['time_stamp'].max()
        if maxTimestamp < MaxTimestamp2:
            maxTimestamp = MaxTimestamp2

        CFG.max_timestamp = maxTimestamp

        if CFG.details:
            if not data_center == "":
                self.filter_data_center(data_center=data_center)

        self.load_application_graph()

        if not CFG.min_plotting == -1 or not CFG.max_plotting == -1:
            self.Application = self.set_max_min_values(self.Application)

            if CFG.details:
                if self.Container.size > 0:
                    self.Container = self.set_max_min_values(self.Container)

                if self.Hardware.size > 0:
                    self.Hardware = self.set_max_min_values(self.Hardware)

                if self.Network.size > 0:
                    self.Network = self.set_max_min_values(self.Network)

                if self.Reconfiguration.size > 0:
                    self.Reconfiguration = self.set_max_min_values(self.Reconfiguration)

                if self.ApplicationState.size > 0:
                    self.ApplicationState = self.set_max_min_values(self.ApplicationState)

                if self.StateManagement.size > 0:
                    self.StateManagement = self.set_max_min_values(self.StateManagement)

                if CFG.plot_traffic and self.Traffic.size > 0:
                    self.Traffic = self.set_max_min_values(self.Traffic)
            self.OperatorConnections = self.set_max_min_values(self.OperatorConnections)

        self.load_latency_throughput()

    def filter_data_center(self, data_center=""):
        self.Application = self.Application[(self.Application['node_id'] == data_center)]
        self.Container = self.Container[(self.Container['node_id'] == data_center)]
        self.Hardware = self.Hardware[(self.Hardware['worker_name'] == data_center)]
        self.Network = self.Network[
            (self.Network['source_device_id'] == data_center) | (self.Network['dst_device_id'] == data_center)]
        self.OperatorConnections = self.OperatorConnections[(self.OperatorConnections['node_id'] == data_center)]

    def set_max_min_values(self, dt=pd.DataFrame()):
        if CFG.min_plotting > 0:
            dt = dt[(dt['time_stamp'] >= CFG.min_plotting)]

        if CFG.min_plotting > 0:
            dt = dt[(dt['time_stamp'] <= CFG.max_plotting)]

        return dt

    def load_application_graph(self):
        connections = numpy.unique(self.OperatorConnections['connection_code'])
        self.Operators = numpy.unique(self.Application['ope_id'])
        self.Operators = numpy.append('nan', self.Operators)

        streams = []
        for i in range(len(connections)):
            pos = connections[i].find('->')
            streams.append([connections[i][0:pos], connections[i][pos + 2:len(connections[i])]])

        self.Sources, self.Sinks = get_sources_sinks(streams=streams, operators=self.Operators)
        # add virtual source
        operators = numpy.append('source', self.Operators)

        # add stream from virtual source to sources
        for s in range(len(self.Sources)):
            streams.append(['source', self.Sources[s]])

        # add virtual sink
        self.Operators = numpy.append("sink", operators)
        # add stream from virtual sink to sinks
        for s in range(len(self.Sinks)):
            streams.append([self.Sinks[s], "sink"])

        graph = Graph(len(self.Operators))
        for i in range(len(streams)):
            graph.addEdge(numpy.where(self.Operators == streams[i][0])[0][0],
                          numpy.where(self.Operators == streams[i][1])[0][0])

        # Get all paths between the two nodes by considering the reduced list of devices
        src = numpy.where(self.Operators == 'source')[0][0]
        dst = numpy.where(self.Operators == 'sink')[0][0]

        self.Paths = graph.get_paths(src_device_id=src,
                                     dst_device_id=dst,
                                     virtual_nodes=[src, dst])

    def load_latency_throughput(self):
        application_metrics = self.Application[
            ['time_stamp', 'node_id', 'operator_id', 'sequence_id', 'latency', 'min_latency', 'max_latency',
             'output_rate']]

        application_metrics.to_csv('/home/aveith/Downloads/export_dataframe.csv', index=False, header=True)
        application_metrics['operator_id'] = application_metrics['operator_id'].str.slice(0, 4)

        df = pd.DataFrame()
        for i in range(len(self.Sinks)):
            df2 = application_metrics[(application_metrics['operator_id'] == self.Sinks[i])]

            if df.empty:
                df = deepcopy(df2)
            else:
                df = df.append(deepcopy(df2))

        df = df[['time_stamp', 'latency', 'min_latency', 'max_latency', 'output_rate']]

        df = df.groupby(
            ['time_stamp']
        ).agg(
            {
                'latency': 'mean',
                'min_latency': 'mean',
                'max_latency': 'mean',
                'output_rate': 'mean'
            }
        )
        df = df.reset_index()
        df = df.sort_values(by='time_stamp', ascending=True)

        self.Throughput_Latency = deepcopy(df)
        # self.Throughput_Latency.to_csv('/home/aveith/Downloads/export_dataframe.csv', index = False, header=True)
