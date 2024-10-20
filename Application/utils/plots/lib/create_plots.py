import os

from conf.conf import CFG
from lib.application import plot_application_statistics
from lib.application_state import plot_application_state_statistics
from lib.containers import plot_container_statistics
from lib.hardware import plot_hardware_statistics
from lib.metrics import Metrics
from lib.network_traffic import plot_network_traffic_statistics
from lib.overlap_plots import plot_overlapping
from lib.pre_processing import parse_files
from lib.reconfiguration import plot_reconfiguration_statistics
from lib.state_management import plot_state_management_statistics
from lib.streams import plot_operator_connections_statistics


class plot_object:
    def __init__(self):
        self.Metrics = object

    def generate_plots(self, input_dir='', input_file=''):
        load_parameters(input_dir=input_dir,
                        input_file=input_file)

        if CFG.parse_files:
            parse_files(input_dir=CFG.input_directory, input_file=CFG.input_file, output_dir=CFG.output_directory)
        else:
            os.chdir(CFG.output_directory)

        if os.path.exists(CFG.output_directory):
            self.Metrics = Metrics(app_file=CFG.output_directory + 'application_metrics.dat',
                                   container_file=CFG.output_directory + 'containers_metrics.dat',
                                   hardware_file=CFG.output_directory + 'hardware_metrics.dat',
                                   network_file=CFG.output_directory + 'network_metrics.dat',
                                   traffic_file=CFG.output_directory + 'traffic_metrics.dat',
                                   operator_connections_file=CFG.output_directory + 'operator_connection_metrics.dat',
                                   state_management_file='state_management_file.dat',
                                   reconfiguration_file='reconfiguration_file.dat',
                                   application_state_file='application_state_file.dat',
                                   name=input_file,
                                   data_center=CFG.data_center)

            plot_application_statistics(metrics=self.Metrics)

            if CFG.details:
                if self.Metrics.OperatorConnections.size > 0:
                    plot_operator_connections_statistics(metrics=self.Metrics)

                if self.Metrics.Container.size > 0:
                    plot_container_statistics(metrics=self.Metrics)

                if self.Metrics.ApplicationState.size > 0:
                    plot_application_state_statistics(metrics=self.Metrics)

                if self.Metrics.StateManagement.size > 0:
                    plot_state_management_statistics(metrics=self.Metrics)

                if self.Metrics.Reconfiguration.size > 0:
                    plot_reconfiguration_statistics(reconfiguration_metrics=self.Metrics.Reconfiguration)

                if CFG.plot_hardware and self.Metrics.Hardware.size > 0:
                    plot_hardware_statistics(hardware_metrics=self.Metrics.Hardware)

                if CFG.plot_traffic and self.Metrics.Traffic.size > 0:
                    plot_network_traffic_statistics(traffic_metrics=self.Metrics.Traffic)


def load_parameters(input_dir="", input_file=""):
    CFG.input_directory = input_dir
    CFG.input_file = input_file

    # if CFG.deployment == '':
    CFG.deployment = os.path.splitext(input_file)[0]

    if CFG.automatic_output_directory:
        CFG.output_directory = input_dir + CFG.deployment + '/'

    if CFG.deployment == '':
        CFG.deployment = os.path.splitext(input_file)[0]


def create_plots_directory(input_dir=''):
    if os.path.exists(input_dir):
        if not input_dir[len(input_dir) - 1:len(input_dir)] == '/':
            input_dir += '/'

        # FIXME - create default output extension for log files
        plot_data = []
        for root, dirs, files in os.walk(input_dir):
            for file in files:
                if file.endswith('.log'):
                    plot_data.append(plot_object())
                    plot_data[len(plot_data) - 1].generate_plots(input_dir=input_dir, input_file=file)

        # if len(plot_data) == 2:
        plot_overlapping(metrics=plot_data, output_dir=input_dir)
    else:
        raise NameError("Invalid path: " + input_dir)


def create_plots(input_dir='', input_file=''):
    plots = plot_object()
    plots.generate_plots(input_dir=input_dir, input_file=input_file)


if __name__ == '__main__':
    """Initialize plots"""
    create_plots()
