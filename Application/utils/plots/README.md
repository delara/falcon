<h1><b>PLOTTING</b></h1>

These scripts allow to create the plots using the output logs of the framework execution. The logs are parsed and then plots are created.

There are two options available on the "bin" folder:
<ul>
    <li><b>Individual</b>: The script "create_individual" requires the parameter "-i", which is the absolute path and file.</li>
    <li><b>Multiple</b>: The script "create_directory" has as input (i.e., -i) the absolute path of the directory where the output files are located. The script considers each file with a given extension and creates the plots for each file.</li> 
</ul>

The scripts also permit to customize your plots. The file inside the folder "conf" gives you a set of options:
<ul>
    <li><b>plot_pdf</b>: True means that pdf files will be generated to the plots, while False will plot tin the IDE.</li> 
    <li><b>plot_title</b>: True plots the title and False removes the title.</li>
    <li><b>create_sub_folders</b>: True organizes the plots in subfolders and False puts all plots in the same folder.</li> 
    <li><b>automatic_output_directory</b>: True uses the input directory to setup the output of plots and False demands the absolute path.</li>
    <li><b>input_file</b>: This field is used by the system, and it does not require any intervention.</li>
    <li><b>input_directory</b>: This field is used by the system, and it does not require any intervention.</li> 
    <li><b>output_director</b> Absolute path to save the plots. It is required if the automatic_output_directory is equal to False.</li> 
    <li><b>deployment</b>: Name of the deployment. If there is any name, the system utilizes the name of the log file.</li>
    <li><b>color_palette</b>: Sequence of colores used by the system.</li>
    <li><b>parse_files</b>: True parses the raw log and False considers that the logs were already parsed.</li>
    <li><b>load_reconfig_file</b>: Reads the files 'reconfig_marker.log and stable_marker.log to annotate the plots.</li>
    <li><b>reconfig_begin</b>: This field is used by the system, and it does not require any intervention.</li>
    <li><b>reconfig_end</b>: This field is used by the system, and it does not require any intervention.</li>
    <li><b>min_timestamp</b>: This field is used by the system, and it does not require any intervention.</li>
    <li><b>max_timestamp</b>: This field is used by the system, and it does not require any intervention.</li>
    <li><b>details</b>: False plots only general information of the execution, and True presents a detail sequence of plots.</li>
    <li><b>min_plotting</b></li>
    <li><b>max_plotting</b></li>
</ul>

TODO
<ul>
    <li>Bandwidth plots</li>
    <li>There is no pattern to the extension of files. The framework must have a default extension.</li>
</ul>
