import cProfile
import pstats


def cprofile(sort_args=['cumulative'], print_args=[10]):
    """
    Decorator to profile a callable object using cProfile. Stats will be output to log file, e.g. docker logs
    Usage:
    1. import profiling_util from codalab.lib
    2. decorate your function as following:
        @profiling_util.cprofile(print_args=[20])
        def function_to_test():
            ...

    View the result:
    1. figure out which container your decorated function goes to, e.g. codalab_bundle-manager_1
    2. check the result by
        docker logs codalab_bundle-manager_1

    Example result:
    Fri Jan 10 00:39:01 2020    _stage_bundles.profile

         1081 function calls (1056 primitive calls) in 0.024 seconds

        Ordered by: cumulative time
        List reduced from 255 to 20 due to restriction <20>

        ncalls  tottime  percall  cumtime  percall filename:lineno(function)
             1    0.000    0.000    0.024    0.024 /opt/codalab-worksheets/codalab/server/bundle_manager.py:96(_stage_bundles)
             2    0.000    0.000    0.024    0.012 /opt/codalab-worksheets/codalab/model/bundle_model.py:677(batch_get_bundles)
             2    0.000    0.000    0.013    0.006 /usr/local/lib/python3.6/dist-packages/sqlalchemy/engine/base.py:1974(execute)
             2    0.000    0.000    0.012    0.006 /usr/local/lib/python3.6/dist-packages/sqlalchemy/engine/base.py:846(execute)
             2    0.000    0.000    0.012    0.006 /usr/local/lib/python3.6/dist-packages/sqlalchemy/sql/elements.py:322(_execute_on_connection)
             ...
    :param sort_args: a list of string that is used to sort the output, e.g. 'cumulative', 'pcalls', 'totime'
    :param print_args: a list of arguments (mixed type, e.g. string, integer, decimal fraction) used to
                       limit the output down to the significant entries.
    :return: the decorator function that contain calls to the original function "f".
    """

    def decorator(f):
        def wrapper(*args, **kwargs):
            output_file = f.__name__ + ".profile"
            profiler = cProfile.Profile()
            result = profiler.runcall(f, *args, **kwargs)
            profiler.dump_stats(output_file)
            stats = pstats.Stats(output_file)
            stats.sort_stats(*sort_args).print_stats(*print_args)
            return result

        return wrapper

    return decorator
