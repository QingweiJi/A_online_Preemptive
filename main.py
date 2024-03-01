import options
from jobs import _Jobs
from server import _Cluster
from Aonline import _AonlineScheduler

opt = options.Singleton.init()


def main():
    CLUSTER = _Cluster()
    CLUSTER.parse_cluster_spec(opt.cluster)
    JOBs = _Jobs()
    JOBs.parse_trace_file(opt.trace, opt.scheduler)

    if opt.scheduler == 'Aonline':
        scheduler = _AonlineScheduler(JOBs, CLUSTER)
    else:
        print("other schedulers haven't created yet")

    scheduler.run()

main()



