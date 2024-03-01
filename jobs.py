import csv
import random

from model import default_model
from chunk import _DataChunk


MINUTE_P_HOUR = 60
SECOND_P_MINUTE = 60

class _Job():
    def __init__(self, job_id, r_j):
        self.job_id = job_id
        self.r_j = r_j
        self.status = None

    def submit(self):
        self.status = 'SUBMITTED'

    def running(self):
        self.status = 'RUNNING'

    def pending(self):
        self.status = 'PENDING'

    def completed(self):
        self.status = 'COMPLETED'


class _Aonline_Job(_Job):
    def __init__(self, job_id, r_j, D_j, E_j, B_j, m_j, G_j, q_j):
        super().__init__(job_id, r_j)
        self.D_j = D_j
        self.E_j = E_j
        self.B_j = B_j
        self.m_j = m_j
        self.G_j = G_j
        self.q_j = q_j
        self.chunk_list = list()
        self.kexi = 0
        self.C_j = dict()


class _Jobs(object):
    def __init__(self):
        self.num_job = 0
        self.job_list =  list()

    # Parse the application scenario Settings file
    def parse_trace_file(self, filepath, type):
        print('reading trace file...')
        csv_reader = csv.DictReader(open(filepath, 'r'), delimiter=',')
        for idx, job_dict in enumerate(csv_reader):
            if type == 'Aonline':
                job_id = int(job_dict['job_id'])
                r_j = int(job_dict['submit_time'])
                model = default_model[random.randint(0,len(default_model)-1)]
                D_j = model[2]
                B_j = model[3]
                E_j = random.randint(20,60)
                m_j = random.randint(3600,180000)
                G_j = random.randint(10, 100)
                q_j = random.randint(30, 575)
                job = _Aonline_Job(job_id, r_j, D_j, E_j, B_j, m_j, G_j, q_j)
                for i in range(D_j):
                    chunk_id = i
                    chunk = _DataChunk(job, chunk_id)
                    job.chunk_list.append(chunk)
                self.job_list.append(job)
                self.num_job += 1
            else:
                job_id = job_dict['job_id']
                r_j = job_dict['submit_time']
                self.job_list.append(_Job(job_id, r_j))
        print(self.num_job,'jobs have been loaded')

    def sort_all_jobs(self, mode=None):
        return
