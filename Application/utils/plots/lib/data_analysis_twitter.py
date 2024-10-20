import matplotlib.pyplot as plt
import pandas as pd
plt.rcParams.update({'font.size': 12})
plt.rcParams["figure.figsize"] = (7, 4)
plt.rcParams['pdf.fonttype'] = 42
plt.rcParams['ps.fonttype'] = 42

df = pd.read_csv('/home/aveith/Downloads/results/20210129_twt_validation/baseline.log', sep=';', header=None)
df.columns = ['time', 'mean', 'min', 'max', 'median', 'std', 'output', 'output_general', 'window']

df2 = pd.read_csv('/home/aveith/Downloads/results/20210129_twt_validation/read_direct_broker.log', sep=';',
                  header=None)
df2.columns = ['time', 'mean', 'min', 'max', 'median', 'std', 'output', 'output_general', 'window']


df3 = pd.read_csv('/home/aveith/Downloads/results/20210129_twt_validation/no_read_direct_broker.log', sep=';',
                  header=None)
df3.columns = ['time', 'mean', 'min', 'max', 'median', 'std', 'output', 'output_general', 'window']

axis_x = df['time'].to_numpy()
axis_y1 = df['mean'].to_numpy()
axis_y1_min = df['min'].to_numpy()
axis_y1_max = df['max'].to_numpy()

axis_y2 = df2['mean'].to_numpy()
axis_y2_min = df2['min'].to_numpy()
axis_y2_max = df2['max'].to_numpy()

axis_y3 = df3['mean'].to_numpy()
axis_y3_min = df3['min'].to_numpy()
axis_y3_max = df3['max'].to_numpy()

plt.cla()
plt.clf()
fig, ax = plt.subplots()
ax.plot(axis_x, axis_y1, color="blue", marker="x", label="Baseline")
plt.fill_between(x=axis_x, y1=axis_y1_min, y2=axis_y1_max, facecolor='blue', alpha=0.5)

ax.plot(axis_x, axis_y2, color="red", marker="o", label="read_direct_broker")
plt.fill_between(x=axis_x, y1=axis_y2_min, y2=axis_y2_max, facecolor='red', alpha=0.5)

ax.plot(axis_x, axis_y3, color="green", marker=".", label="no_read_direct_broker")
plt.fill_between(x=axis_x, y1=axis_y3_min, y2=axis_y3_max, facecolor='green', alpha=0.5)

ax.set_xlabel('Execution Time(s)')
ax.set_ylabel('Milliseconds')
plt.title('Tweet Validation')
plt.legend()

plt.show()
