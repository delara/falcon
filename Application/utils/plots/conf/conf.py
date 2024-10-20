class CFG(object):
    plot_pdf = True
    plot_title = False
    create_sub_folders = True
    automatic_output_directory = True
    input_file = ""
    input_directory = ""
    output_director = ""
    deployment = ""
    output_file_extension = "pdf"

    if not automatic_output_directory:
        output_directory = 'dummy_directory'

    color_palette = ['b', 'r', 'g', 'm', 'c', 'y', 'k', 'gray', 'orange', 'teal', 'indigo', 'purple', 'lime', 'cyan']

    parse_files = True

    annotate_reconfiguration = True
    annotate_startup = True

    min_timestamp = 0
    max_timestamp = 0
    details = True

    min_plotting = -1
    max_plotting = -1

    data_center = ""
    data_center_from_file = ""

    plot_traffic = True
    plot_hardware = True
