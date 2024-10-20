import os
import shutil


def parse_files(input_dir='', input_file='', output_dir=''):
    if input_dir != output_dir:
        if os.path.exists(output_dir):
            shutil.rmtree(output_dir)

    os.mkdir(output_dir)
    os.chdir(output_dir)

    names = ['hardware', 'network', 'docker', 'application metric', 'application edge', 'consistency metric',
             'traffic metric', 'state management metric', 'reconfiguration metric', 'application state metric']
    final_names = ['hardware_metrics.dat', 'network_metrics.dat', 'containers_metrics.dat', 'application_metrics.dat',
                   'operator_connection_metrics.dat', 'consistency_metrics.dat', 'traffic_metrics.dat',
                   'state_management_file.dat', 'reconfiguration_file.dat', 'application_state_file.dat']
    file_object = []
    for i in range(len(final_names)):
        file_object.append(open(final_names[i], 'w'))

    file1 = open(input_dir + input_file,
                 'r')

    while True:
        line = file1.readline()
        if 'Received' in line:
            list = line.split(':')

            # Look for the file name
            for f in range(len(names)):
                if names[f] in list[0]:
                    if 'consistency' in list[0]:
                        line_text = list[1].split('[')
                        items = line_text[1].split(',')
                        for item in range(len(items)):
                            if items[item].strip() != ']':
                                file_object[f].write(line_text[0].strip() + items[item].strip() + '\n')

                    else:
                        # get values
                        if ';' in list[1]:
                            file_object[f].write(list[1].strip() + '\n')
                            break

        if not line:
            break

    for i in range(len(final_names)):
        file_object[i].close()
