import math
import time

from jobs import _Job, _Jobs, _Aonline_Job
from server import _Cluster, _Server, _PS, _Worker
from chunk import _DataChunk
import random
import statistics

MINUTE_P_HOUR = 60
SECOND_P_MINUTE = 60


def find_timeslot(t1, t2):
    result = list()
    t1 = math.ceil(t1)
    result.append(t1)
    t1 += 1
    while t1 < t2:
        result.append(t1)
        t1 += 1
    return result


def calculate_workerid(s: _Server, w: _Worker):
    return s.server_id * 100000000000 + w.worker_id


def calculate_N_jsw(j: _Aonline_Job, w: _Worker):
    # TODO:set v_jsw =1
    v_jsw = 1

    if v_jsw == 1:
        n_jsw = calculate_N_j(j, w)
    else:
        n_jsw = 0
    return n_jsw


def calculate_N_j(j: _Aonline_Job, w: _Worker):
    j.kexi = 0  # TODO: initial kexi
    if j.kexi == 0:
        n_j = 1 / (j.m_j + j.G_j + 2 * j.q_j / w.bw)
    else:
        n_j = 1 / (j.m_j + j.G_j)
    return n_j


def calculate_Processing_Rate(j: _Aonline_Job, w: _Worker):
    gamma_jd = calculate_N_jsw(j, w) / (j.E_j * j.D_j * j.B_j)
    return gamma_jd


def calculate_Processing_Time(j: _Aonline_Job, w: _Worker):
    n_jsw = calculate_N_jsw(j, w)
    one_chunk_processing_time = (j.E_j * j.B_j) / n_jsw
    return one_chunk_processing_time


def calculateQ(timeslot, solution, j: _Aonline_Job, d: _DataChunk, s: _Server, w: _Worker):
    avg_dispatching_time = (1 / j.D_j) * s.transmission_delay
    avg_processing_time = (1 / j.D_j) * calculate_Processing_Time(j, w)

    if s.isCloud:
        if d.chunk_id == 0:
            j.kexi = 1
        else:
            j.kexi = 0
        # set v_jsw =1
        v_jsw = 1

        n_j = calculate_N_j(j, w)

        if v_jsw == 1:
            n_jsw = n_j
        else:
            n_jsw = 0

        Q_jdsw = avg_dispatching_time + avg_processing_time
        t_star = j.r_j + s.transmission_delay
        A2 = list()
        return Q_jdsw, t_star, A2
    else:
        # TODO:get A from solution
        A = []
        r_j = j.r_j
        delay = s.transmission_delay
        worker_id = calculate_workerid(s, w)
        timeline = solution[worker_id]
        for t in range(r_j+delay,len(timeline)):
            chunk = timeline[t]
            if chunk is not None:
                if not chunk in A:
                    A.append(chunk)
        A1 = list()
        A2 = list()
        Q_jdsw = avg_dispatching_time + avg_processing_time
        rate = calculate_Processing_Rate(j, w)
        avg_waiting_time = 0
        avg_postponed_time = 0
        t_star = j.r_j + s.transmission_delay
        for chunk in A:
            j_star = chunk.job
            rate_star = calculate_Processing_Rate(j_star, w)
            if rate_star >= rate:
                t_star = t_star + 1
                avg_waiting_time += calculate_Processing_Time(j_star, w)
                A1.append(chunk)
            else:
                avg_postponed_time += (1 / j_star.D_j)
                A2.append(chunk)
        t_star = max(t_star, j.r_j + s.transmission_delay)
        Q_jdsw = Q_jdsw + avg_waiting_time * (1 / j.D_j) + avg_postponed_time * calculate_Processing_Time(j, w)
        return Q_jdsw, t_star, A2


class _AonlineScheduler():
    def __init__(self, JOBS: _Jobs, CLUSTER: _Cluster) -> None:
        self.jobs = JOBS
        self.cluster = CLUSTER

        self.job_list = JOBS.job_list
        self.J_a = list()

        self.T = 1000
        self.t = 1  # 1 hour

        for j in self.job_list:
            j.m_j = j.m_j / (self.t * 1000 * SECOND_P_MINUTE * MINUTE_P_HOUR)
            j.G_j = j.G_j / (self.t * 1000 * SECOND_P_MINUTE * MINUTE_P_HOUR)
        for s in self.cluster.server_list:
            for w in s.worker_list:
                w.bw = w.bw * (self.t * SECOND_P_MINUTE * MINUTE_P_HOUR)

        self.solution = dict()

        for s in self.cluster.server_list:
            for w in s.worker_list:
                worker_id = calculate_workerid(s, w)
                self.solution[worker_id] = [None for i in range(self.T+1)]

        # for j in self.job_list:
        #     for chunk in j.chunk_list:
        #         chunk_id = j.job_id * 100000 + chunk.chunk_id
        #         worker_dict = dict()
        #         for s in self.cluster.server_list:
        #             for w in s.worker_list:
        #                 worker_id = s.server_id * 100000 + w.worker_id
        #                 worker_dict[worker_id] = [0 for i in range(self.T)]
        #         solution[chunk_id] = worker_dict

    def run(self):
        self.J_a = list()
        for time_slot in range(1, self.T + 1):
            print('time: ', time_slot)
            for job in self.job_list:
                r_j = job.r_j
                if r_j >= (
                        time_slot - 1 ) * self.t * MINUTE_P_HOUR * SECOND_P_MINUTE and r_j < self.t * time_slot * MINUTE_P_HOUR * SECOND_P_MINUTE:
                    job.r_j = time_slot
                    self.J_a.append(job)
                    job.submit()
            self.A_Greedy(time_slot)
            self.J_a.clear()
        JCT = []
        for j in self.job_list:
            print('job',j.job_id)
            start = self.T
            end = 0
            worker_list = dict()
            for worker_id, timeline in self.solution.items():
                for idx,chunk in enumerate(timeline):
                    if chunk:
                        job = chunk.job
                        if j.job_id == job.job_id:
                            if idx > end:
                                end = idx
                            if idx < start:
                                start = idx
                            if worker_id in worker_list.keys():
                                tuple = (job.job_id,chunk.chunk_id)
                                if not tuple in worker_list[worker_id]:
                                    worker_list[worker_id].append((job.job_id,chunk.chunk_id))
                            else:
                                worker_list[worker_id] = list()
                                worker_list[worker_id].append((job.job_id, chunk.chunk_id))
            print('start_time_slot:', start)
            print('end_time_slot:', end)
            print('data_chunk_num:',j.D_j)
            print('job_JCT:',end-j.r_j)
            JCT.append(end-j.r_j)
            worker_list = sorted(worker_list.items(), key= lambda kv:sorted(kv[1], key=lambda e:e[1]))
            for tuple in worker_list:
                worker_id = tuple[0]
                value = tuple[1]
                for s in self.cluster.server_list:
                    for w in s.worker_list:
                        if calculate_workerid(s, w) == worker_id:
                            print('--server_id',s.server_id,'--worker_id',w.worker_id,'--delay',s.transmission_delay,'--(job_id,chunk_id)',value)
        print(JCT)
        print(statistics.mean(JCT))
                # self.A_PS()

    def update(self, j: _Aonline_Job, d: _DataChunk, s: _Server, w: _Worker, t_, A2):
        worker_id = calculate_workerid(s, w)
        t1 = t_
        t2 = t_ + calculate_Processing_Time(j, w)
        timeslot = find_timeslot(t1, t2)
        for a2 in A2:
            t_list = list()
            for idx, chunk in enumerate(self.solution[worker_id]):
                if chunk:
                    if chunk.job.job_id == a2.job.job_id and chunk.chunk_id == a2.chunk_id:
                        t_list.append(idx)
            for t in t_list:
                if t >= t_:
                    self.solution[worker_id][t] = None
                    self.solution[worker_id][t + len(timeslot)] = a2
        for t in timeslot:
            self.solution[worker_id][t] = d

    def A_Greedy(self, timeslot):
        i = 1
        for j in self.J_a:
            print('\r', i, '/', len(self.J_a),end='')
            if i == len(self.J_a):
                print('\n')
            i += 1

            j.kexi = 0
            C_j = dict()
            t_star = 0
            A2 = list()
            alpha = [0 for i in range(len(j.chunk_list))]
            for s in self.cluster.server_list:
                for w in s.worker_list:
                    n_jsw = calculate_N_jsw(j, w)
                    if n_jsw > 0:
                        Q_jd1sw, t_star, A2 = calculateQ(timeslot, self.solution, j, j.chunk_list[0], s, w)
                        worker_id = calculate_workerid(s, w)
                        C_j[worker_id] = (Q_jd1sw, t_star, A2)
            for d in j.chunk_list:
                s_d_star = None
                w_d_star = None
                t_star = 0
                A2 = list()
                kv = sorted(C_j.items(), key=lambda kv: kv[1][0])[0]
                worker_id = kv[0]
                tuple = kv[1]
                minQ = tuple[0]
                for s in self.cluster.server_list:
                    for w in s.worker_list:
                        if calculate_workerid(s, w) == worker_id:
                            s_d_star = s
                            w_d_star = w
                            break
                t_star = tuple[1]
                A2 = tuple[2]
                if s_d_star.isCloud:
                    for d2 in j.chunk_list:
                        if d2.chunk_id >= d.chunk_id:
                            alpha[d2.chunk_id] = minQ
                            worker_id = s_d_star.worker_num
                            worker_type = random.randint(8, 10)
                            worker_bw = random.randint(100, 5 * 1024) / 1000
                            gpu_num = 1
                            cpu_num = random.randint(1, 4)
                            memory = random.randint(3, 6)
                            worker = _Worker(worker_id, worker_type, self, worker_bw, gpu_num=gpu_num, cpu_num=cpu_num,memory=memory)
                            worker.bw = worker.bw * (self.t * 1000 * SECOND_P_MINUTE * MINUTE_P_HOUR)
                            s_d_star.worker_list.append(worker)
                            s_d_star.worker_num += 1
                            worker_id = calculate_workerid(s_d_star, worker)
                            self.solution[worker_id] = [None for i in range(self.T + 1)]
                            self.update(j, d2, s_d_star, worker, t_star, A2)
                    break

                if d.chunk_id == 0:
                    cloud = None
                    worker_id_list = list()
                    for s in self.cluster.server_list:
                        if s.isCloud:
                            cloud = s
                            for w in s.worker_list:
                                worker_id_list.append(calculate_workerid(s, w))
                    Q_jds_w_, t_star, A2 = calculateQ(timeslot, self.solution, j, d, cloud, cloud.worker_list[0])
                    for worker_id, item in C_j.items():
                        if worker_id in worker_id_list:
                            C_j[worker_id] = (Q_jds_w_, t_star, A2)

                Q_jds_w_, t_star, A2 = calculateQ(timeslot, self.solution, j, d, s_d_star, w_d_star)
                worker_id = calculate_workerid(s_d_star, w_d_star)
                C_j[worker_id] = (Q_jds_w_, t_star, A2)
                kv = sorted(C_j.items(), key=lambda kv: kv[1][0])[0]
                tuple = kv[1]
                minQ = tuple[0]
                alpha[d.chunk_id] = minQ
                worker_id = kv[0]
                for s in self.cluster.server_list:
                    for w in s.worker_list:
                        if calculate_workerid(s,w)==worker_id:
                            s_d_star = s
                            w_d_star = w
                            break
                A2 = tuple[2]
                t_star = tuple[1]
                # print(w_d_star.server.isCloud)
                self.update(j, d, s_d_star, w_d_star, t_star, A2)
