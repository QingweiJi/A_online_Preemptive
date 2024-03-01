import argparse
import ast

# parse input arguments
class Options:
    def __init__(self, ):
        parser = argparse.ArgumentParser(description='Eunomia')
        parser.add_argument('--trace', default='training_trace/2_job.csv', type=str, help='training trace path')
        parser.add_argument('--save_log_dir', default='result/', type=str, help='log path')
        parser.add_argument('--scheduler', default='Aonline', type=str, choices=['Aonline'], help='schedule policy')
        parser.add_argument('--cluster', default='settings/cluster.csv', type=str, help='cluster settings path')
        self.args = parser.parse_args()

    def init(self, ):
        return self.args

Singleton = Options()

_allowed_symblos = [
    'Singleton'
]