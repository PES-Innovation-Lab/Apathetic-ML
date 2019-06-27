import subprocess
import curses
stdscr = curses.initscr()
curses.noecho()
import BasicTUI as BT
import npyscreen

x = BT.screen()
num_workers = int(x[1])

def worker_add(file):
    f = open(file,'r')
    lines = f.readlines()

    for i in lines:
        if '#new worker' in i:
            pos = lines.index(i)
        if '#master code' in i:
            pos_m = lines.index(i)

    copy = lines[0:pos+1]
    l = lines[0:pos + 1]

    for i in range(1,num_workers):
        a = [x.replace('worker0','worker'+str(i)) for x in copy]
        l.extend(a)

    l.extend(lines[pos_m:])
    f.close()

    f1 = open(file,'w')
    f1.writelines(l)
    f1.close()

worker_add('deploy_creator.yaml')
worker_add('svc_create.yaml')

print('First bash script running')
subprocess.call('./deploy_all.sh',shell=True)
print('First bash script ending')


print('Creating cluster on GCP..')
subprocess.call('make -f clusters.makefile create',shell=True)
print('Cluster created on GCP!')







