import os
import sys
import subprocess

from codalab.lib import (
  canonicalize,
  path_util,
)

from codalab.objects.machine import Machine

class LocalMachine(Machine):
    '''
    Run commands on the local machine.  This is for simple testing or personal
    use only, since there is no security.
    '''
    def __init__(self):
        self.bundle = None
        self.process = None
        self.temp_dir = None

    def start_bundle(self, bundle, bundle_store, parent_dict):
        '''
        Start a bundle in the background.
        '''
        if self.bundle != None: raise InternalError('Bundle already started')
        temp_dir = canonicalize.get_current_location(bundle_store, bundle.uuid)
        path_util.make_directory(temp_dir)

        # We don't follow symlinks (for consistency with remote
        # machine, where it is more secure, so people can't make us
        # copy random files on the system).  Of course in local mode,
        # if some of those symlinks are absolute, the run can
        # read/write those locations.  But we're not sandboxed, so
        # anything could happen.  The dependencies are copied, so in
        # practice, this is not a bit worry.
        pairs = bundle.get_dependency_paths(bundle_store, parent_dict, temp_dir)
        print >>sys.stderr, 'LocalMachine.start_bundle: copying dependencies of %s to %s' % (bundle.uuid, temp_dir)
        for (source, target) in pairs:
            path_util.copy(source, target, follow_symlinks=False)

        script_file = temp_dir + '.sh'
        with open(script_file, 'w') as f:
            f.write("cd %s &&\n" % temp_dir)
            f.write('(%s) > stdout 2>stderr\n' % bundle.command)
        # Use stdbuf to turn off buffering so we get real-time feedback.
        process = subprocess.Popen("stdbuf -o0 bash " + script_file, shell=True)

        #with path_util.chdir(temp_dir):
            #with open('stdout', 'wb') as stdout, open('stderr', 'wb') as stderr:
                #process = subprocess.Popen("stdbuf -o0 " + bundle.command, stdout=stdout, stderr=stderr, shell=True)

        self.bundle = bundle
        self.temp_dir = temp_dir
        self.process = process
        return {
            'bundle': bundle,
            'temp_dir': temp_dir,
            'job_handle': str(process.pid)
        }

    def kill_bundle(self, bundle):
        if not self.bundle or self.bundle.uuid != bundle.uuid: return False
        self.process.kill()
        return True

    def get_bundle_statuses(self):
        if self.process == None: return []

        self.process.poll()
        # TODO: include time and memory
        status = {
            'job_handle': str(self.process.pid),
            'exitcode': self.process.returncode,
        }
        status['success'] = status['exitcode'] == 0 if status['exitcode'] != None else None
        return [status]

    def finalize_bundle(self, bundle):
        if not self.bundle or self.bundle.uuid != bundle.uuid: return False

        try:
            script_file = self.temp_dir + '.sh'
            for f in [script_file]:
                if os.path.exists(f):
                    path_util.remove(f)
            ok = True
        except Exception, e:
            print '=== INTERNAL ERROR: %s' % e
            traceback.print_exc()
            ok = False

        self.bundle = None
        self.temp_dir = None
        self.process = None
        return ok
