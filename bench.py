import os
import subprocess
import matplotlib.pyplot as plt
import numpy as np

base_bytes_number = 100
factors = [1, 4, 16]

values_bytes_size = []

for factor in factors:
    values_bytes_size.append(factor * base_bytes_number)

exe_dir = './build/'
log_dir = './logs/'
#f_bench = open(log_dir + '/bench.md','w')
#f_nvm = open(log_dir + '/nvm.md', 'w')


execs = ['db_bench', 'db_bench_dax', 'db_bench_nvm']
execs_path = [exe_dir + exe for exe in execs]


#db_bench_path = exe_dir + '/db_bench'
#db_bench_nvm_path = exe_dir + '/db_bench_nvm'

db_dir = '/mnt/mem/dbbench'

benches = ['latency', 'throughput']
methods = ['fillseq', 'fillrandom', 'readseq', 'readrandom']
bench_result = {exe:dict() for exe in execs}
for exe in bench_result:
    for bench in benches:
        bench_result[exe][bench] = dict()
        for method in methods:
            bench_result[exe][bench][method] = dict()

#bench_result = {'latency':dict(), 'throughput': dict()}
#bench_nvm_result = {'latency':dict(), 'throughput': dict()}



for exe in execs_path:
    index = exe.find(exe_dir)
    index += len(exe_dir)
    remove_prefix_exe = exe[index:]
    f_bench = open(log_dir + remove_prefix_exe +'.md', 'w')
    for values in values_bytes_size:
        bench_output = subprocess.run([exe, '--value_size='+str(values)], stdout=subprocess.PIPE)
        output_str = bench_output.stdout.decode('utf-8')
        subprocess.run(['rm', '-rf', db_dir])


        f_bench.write('bench values bytes size' + str(values) + '\n')
        f_bench.write(output_str)
        output_str = output_str.replace(' ', '')
        for method in methods:
            if method != 'readrandom':
                latency = ''
                seach_method = method + ':'
                index = output_str.find(seach_method)
                index += len(seach_method)
                while (output_str[index] >= '0' and output_str[index] <='9') or output_str[index] == '.':
                    latency += output_str[index]
                    index += 1
                bench_result[remove_prefix_exe]['latency'][method][values] = float(latency)

                throughput_rate = ''
                index = output_str.find(seach_method)
                while output_str[index] != ';':
                    index+=1
                index+=1
                while (output_str[index] >= '0' and output_str[index] <='9') or output_str[index] == '.':
                    throughput_rate+=output_str[index]
                    index+=1
                bench_result[remove_prefix_exe]['throughput'][method][values] = float(throughput_rate)
            else:
                latency = ''
                seach_method = method + ':'
                index = output_str.find(seach_method)
                index+= len(seach_method)
                while (output_str[index] >= '0' and output_str[index] <= '9') or output_str[index] == '.':
                    latency+=output_str[index]
                    index+=1
                bench_result[remove_prefix_exe]['latency'][method][values] = float(latency)
        
        print(output_str)



    f_bench.close()
#f_nvm.close()
 
figs_dir = './figs'
subprocess.run(['rm', '-rf', figs_dir])
subprocess.run(['mkdir', figs_dir])

# values different models for every bench method
#benches = ['latency', 'throughput']
bar_width = float(1 / len(factors))

for bench in benches:
    for method in methods:
        data_arrs = []
        for exe in execs:
            bench_for_one_method = bench_result[exe][bench][method]
            data_arrs.append([mark for mark in bench_for_one_method.values()])
        #data_leveldb = bench_result[bench][method]
        #data_nvm = bench_nvm_result[bench][method]
        
        #data_leveldb_arr = [mark for mark in data_leveldb.values()]
        #data_nvm_arr = [mark for mark in data_nvm.values()]

        X =np.arange(len(data_arrs[0])) * 2
        fig = plt.figure()
        #ax = fig.add_axes([0, 0, 1, 1])
        i = 0
        colors = ['r', 'g', 'b']
        labels = ['LevelDB', 'LevelDB-DAX', 'NVLSM']
        np_data_arrs = np.array(data_arrs)
        np_data_arrs = np_data_arrs / np_data_arrs[1, :]
        for data_arr in np_data_arrs:
            if len(data_arr) ==0:
                continue
            plt.bar(X +i*bar_width, data_arr, color=colors[i], width=bar_width, label=labels[i])
            for x, y in zip(X+ i * bar_width , data_arr):
                plt.text(x, y, '%.2f' %y, ha='center', va='bottom', fontsize='smaller')            
            i+=1


        #plt.bar(X , data_leveldb_arr, color= 'b', width=bar_width, label='LevelDB')
        #plt.bar(X + bar_width, data_nvm_arr, color='r', width=bar_width, label='NVLSM')
        #fig.set_yticks(factors)
        plt.xticks(X + 0.5 * bar_width , values_bytes_size)
        if bench == 'latency':
            plt.ylim(0, np_data_arrs.max()+0.5)
        plt.legend()
        plt.title(method)


        #plt.tight_layout()

        plt.xlabel('value size(B)')
        if bench == 'latency':
            plt.ylabel('Norm ' +bench )
        else:
            plt.ylabel('Norm '+ bench )



        plt.savefig('./figs/' + bench + '-' + method + '.png')


 

'''
for bench in bench_result:
    for method in bench_names:
        bench_result[bench][method] = dict()

for bench in bench_nvm_result:
    for method in bench_names:
        bench_nvm_result[bench][method] = dict()
'''



'''
        nvm_output = subprocess.run([db_bench_nvm_path, '--value_size='+str(values)], stdout=subprocess.PIPE)
        output_str = nvm_output.stdout.decode('utf-8')

        f_nvm.write('bench values bytes size' + str(values) + '\n')
        f_nvm.write(output_str)
        output_str = output_str.replace(' ', '')
        for method in bench_names:
            if method != 'readrandom':
                latency = ''
                seach_method = method + ':'
                index = output_str.find(seach_method)
                index += len(seach_method)
                while (output_str[index] >= '0' and output_str[index] <= '9') or output_str[index] == '.':
                    latency += output_str[index]
                    index += 1
                bench_nvm_result['latency'][method][values] = float(latency)

                throughput_rate = ''
                index = output_str.find(seach_method)
                while output_str[index] != ';':
                    index+=1
                index+=1
                while (output_str[index] >= '0' and output_str[index] <='9') or output_str[index] == '.':
                    throughput_rate+=output_str[index]
                    index+=1
                bench_nvm_result['throughput'][method][values] = float(throughput_rate)
            else:
                latency = ''
                seach_method = method + ':'
                index = output_str.find(seach_method)
                index+= len(seach_method)
                while (output_str[index] >= '0' and output_str[index] <= '9') or output_str[index] == '.':
                    latency+=output_str[index]
                    index+=1
                bench_nvm_result['latency'][method][values] = float(latency)
        
        print(output_str)
'''