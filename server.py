import csv
import random
from chunk import _DataChunk


class _Worker():
    def __init__(self, worker_id, worker_type, server, bw, gpu_num=1, cpu_num=1, memory=3):
        self.worker_id = worker_id
        self.worker_type = worker_type
        self.server = server
        self.bw = bw
        self.gpu_num = gpu_num
        self.cpu_num = cpu_num
        self.memory = memory
        self.A = list()



class _PS():
    def __init__(self, ps_id, ps_type, server, gpu_num=0, cpu_num=1, memory=3):
        self.ps_id = ps_id
        self.ps_type = ps_type
        self.server = server
        self.gpu_num = gpu_num
        self.cpu_num = cpu_num
        self.memory = memory


class _Server():
    def __init__(self, server_id, worker_num, ps_num, transmission_delay, isCloud=True):
        self.server_id = server_id
        self.worker_num = worker_num
        self.ps_num = ps_num
        self.worker_list = list()
        self.ps_list = list()
        self.isCloud = isCloud
        self.transmission_delay = transmission_delay

        # if not isCloud:
        for i in range(worker_num):
            worker_id = i
            worker_type = random.randint(8, 10)
            worker_bw = random.randint(100, 5 * 1024)
            gpu_num = 1
            cpu_num = random.randint(1, 4)
            memory = random.randint(3, 6)
            worker = _Worker(worker_id, worker_type, self, worker_bw, gpu_num=gpu_num, cpu_num=cpu_num, memory=memory)
            self.worker_list.append(worker)
        # if not isCloud:
        for i in range(ps_num):
            ps_id = i
            ps_type = random.randint(8, 10)
            gpu_num = 0
            cpu_num = random.randint(1, 4)
            memory = random.randint(3, 6)
            ps = _PS(ps_id, ps_type, self, gpu_num=gpu_num, cpu_num=cpu_num, memory=memory)
            self.ps_list.append(ps)

class _Cluster():
    def __init__(self):
        self.server_list = list()

    def parse_cluster_spec(self, filepath):
        print("Start reading the cluster.csv")
        reader = csv.DictReader(open(filepath, 'r'))
        for idx, server_dict in enumerate(reader):
            server_id = idx
            worker_num = int(server_dict['worker_num'])
            ps_num = int(server_dict['ps_num'])
            isCloud = int(server_dict['is_cloud'])
            if isCloud > 0:
                isCloud = True
                transmission_delay = random.randint(10,15)
            else:
                isCloud = False
                transmission_delay = random.randint(1,4)
            server = _Server(server_id, worker_num, ps_num, transmission_delay, isCloud)
            self.server_list.append(server)
        print(len(self.server_list), "servers have been loaded")



