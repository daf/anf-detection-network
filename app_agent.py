#!/usr/bin/env/python

"""
@file app_agent.py 
@author Dave Foster <dfoster@asascience.com>
@brief Application Agent to listen to requests from the Application Controller.
"""

import os, string, tempfile, shutil

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer, reactor

from ion.core.process.process import Process
from ion.util.async_fsm import AsyncFSM
from ion.util.state_object import BasicStates
from ion.services.coi.attributestore import AttributeStoreClient
from ion.core.messaging import messaging
from ion.core.messaging.receiver import Receiver
from ion.util.task_chain import TaskChain
from ion.util.os_process import OSProcess

from support import TopicWorkerReceiver

import uuid
import pprint

try:
    import multiprocessing  # python 2.6 only
    NO_MULTIPROCESSING = False
except:
    NO_MULTIPROCESSING = True

from app_controller_service import SSD_BIN, SSC_BIN, SQLTDEFS_KEY, SSD_READY_STRING

class SSStates(object):
    """
    States for SQLstream instances.
    Bidirectional FSM:
        INIT <-> INSTALLED <-> READY <-> DEFINED <-> RUNNING
    """
    S_INIT = "INIT"
    S_INSTALLED = "INSTALLED"
    S_READY = "READY"
    S_DEFINED = "DEFINED"
    S_RUNNING = "RUNNING"
    #S_ERROR = "ERROR"

    E_INSTALL = "install"
    E_RUNDAEMON = "rundaemon"
    E_LOADDEFS = "loaddefs"
    E_PUMPSON = "pumpson"

    E_PUMPSOFF = "pumpsoff"
    E_UNLOADDEFS = "unloaddefs"
    E_STOPDAEMON = "stopdaemon"
    E_UNINSTALL = "uninstall"

#
#
# ########################################################
#
#

class SSFSMFactory(object):

    def create_fsm(self, target, ssid):
        """
        Creates a FSM for SQLstream instance state/transitions.
        The entry for its SSID must be defined in the target's sqlstreams dict.
        """
        fsm = AsyncFSM(SSStates.S_INIT, None)

        # extract SSID from kwargs, set up vars needed for install/uninstall
        sdpport     = target.sqlstreams[ssid]['sdpport']
        hsqldbport  = target.sqlstreams[ssid]['hsqldbport']
        dirname     = target.sqlstreams[ssid]['dirname']
        installerbin= os.path.join(os.path.dirname(__file__), "app_controller_service", "install_sqlstream.sh")

        # 1. INIT <-> INSTALLED
        proc_installer  = OSProcess(binary=installerbin, spawnargs=[sdpport, hsqldbport, dirname])
        forward_task    = proc_installer.spawn
        backward_task   = lambda: shutil.rmtree(dirname)

        fsm.add_transition(SSStates.E_INSTALL, SSStates.S_INIT, forward_task, SSStates.S_INSTALLED)
        fsm.add_transition(SSStates.E_UNINSTALL, SSStates.S_INSTALLED, backward_task, SSStates.S_INIT)

        # 2. INSTALLED <-> READY
        proc_server = OSSSServerProcess(binroot=dirname)
        proc_server.addCallback(target._sqlstream_ended, sqlstreamid=ssid)
        proc_server.addReadyCallback(target._sqlstream_started, sqlstreamid=ssid)
        target.sqlstreams[ssid]['_serverproc'] = proc_server   # store it here, it still runs
        forward_task = proc_server.spawn
        backward_task = proc_server.close

        fsm.add_transition(SSStates.E_RUNDAEMON, SSStates.S_INSTALLED, forward_task, SSStates.S_READY)
        fsm.add_transition(SSStates.E_STOPDAEMON, SSStates.S_READY, backward_task, SSStates.S_INSTALLED)

        # 3. READY <-> DEFINED
        proc_loaddefs = OSSSClientProcess(spawnargs=[sdpport], sqlcommands=target.sqlstreams[ssid]['sql_defs'], binroot=dirname)
        forward_task = proc_loaddefs.spawn

        # 18 Jan 2011 - Disabled due to lack of ability to turn consumer off - will freeze sqllineClient
        #proc_unloaddefs = OSSSClientProcess(spawnargs=[sdpport], sqlcommands="DROP SCHEMA UCSD CASCADE;", binroot=dirname)
        #backward_task = proc_unloaddefs.spawn
        backward_task = lambda: False     # can't do pass here?

        fsm.add_transition(SSStates.E_LOADDEFS, SSStates.S_READY, forward_task, SSStates.S_DEFINED)
        fsm.add_transition(SSStates.E_UNLOADDEFS, SSStates.S_DEFINED, backward_task, SSStates.S_READY)

        # 4. DEFINED <-> RUNNING
        proc_pumpson = OSSSClientProcess(spawnargs=[sdpport], sqlcommands=target._get_sql_pumps_on(), binroot=dirname)
        forward_task = proc_pumpson.spawn

        proc_pumpsoff = OSSSClientProcess(spawnargs=[sdpport], sqlcommands=target._get_sql_pumps_off(), binroot=dirname)
        backward_task = proc_pumpsoff.spawn

        fsm.add_transition(SSStates.E_PUMPSON, SSStates.S_DEFINED, forward_task, SSStates.S_RUNNING)
        fsm.add_transition(SSStates.E_PUMPSOFF, SSStates.S_RUNNING, backward_task, SSStates.S_DEFINED)

        return fsm

#
#
# ########################################################
#
#

class AppAgent(Process):
    """
    Application Agent - lives on the opunit, communicates status with app controller, recieves
    instructions.
    """

    def __init__(self, receiver=None, spawnargs=None, **kwargs):
        """
        Constructor.
        Gathers information about the system this agent is running on.
        """
        Process.__init__(self, receiver=receiver, spawnargs=spawnargs, **kwargs)

        sa = spawnargs
        if not sa:
            sa = {}

        self._opunit_id = sa.get("opunit_id", str(uuid.uuid4())[:8]) # if one didn't get assigned, make one up to report in to the app controller

        self.metrics = { 'cores' : self._get_cores() }
        self.sqlstreams = {}
        self._fsm_factory_class = kwargs.pop("fsm_factory", SSFSMFactory)

    @defer.inlineCallbacks
    def plc_init(self):
        self.target = self.get_scoped_name('system', "app_controller")

        self.attribute_store_client = AttributeStoreClient()

        # check spawn args for sqlstreams, start them up as appropriate
        # expect a stringify'd python array of dicts
        if self.spawn_args.has_key('agent_args') and self.spawn_args['agent_args'].has_key('sqlstreams'):
            sqlstreams = eval(self.spawn_args['agent_args']['sqlstreams'])        # TODO: unstringify this

            for ssinfo in sqlstreams:
                ssid = ssinfo['ssid']
                inp_queue = ssinfo['sqlt_vars']['inp_queue']
                defs = yield self._get_sql_defs(uconf=ssinfo['sqlt_vars'])

                self.start_sqlstream(ssid, inp_queue, defs)

        # let controller know we're starting and have some sqlstreams starting, possibly
        # we call later in order to let it transition out of init state
        reactor.callLater(0, self.opunit_status)

    @defer.inlineCallbacks
    def plc_terminate(self):
        """
        Termination of this App Agent process.
        Attempts to shut down sqlstream clients and sqlstream daemon instances cleanly. If
        they exceed a timeout, they are shut down forcefully.
        """
        yield self.kill_sqlstream_clients()  # kill clients first, they prevent sqlstream daemons from shutting down
        yield self.kill_sqlstreams()
        yield self.opunit_status(BasicStates.S_TERMINATED)

    def kill_sqlstreams(self):
        dl = []
        for sinfo in self.sqlstreams.values():
            if sinfo.has_key('_serverproc') and sinfo['_serverproc'] != None:
                dl.append(self.kill_sqlstream(sinfo['ssid']))

        deflist = defer.DeferredList(dl)
        return deflist

    def kill_sqlstream(self, ssid):
        """
        Shuts down and deletes a single SQLstream instance.
        @return A deferred which will be called back when the steps to stop and delete a SQLstream
                instance are complete.
        """
        return self.sqlstreams[ssid]['_fsm'].run_to_state(SSStates.S_INIT)

    def kill_sqlstream_clients(self):
        dl = []
        for sinfo in self.sqlstreams.values():
            if sinfo.has_key('_taskchain') and sinfo['_taskchain'] != None:
                dl.append(sinfo['_taskchain'].close())

        deflist = defer.DeferredList(dl)
        return deflist

    def _get_sql_pumps_on(self):
        sql_cmd = """
                  ALTER PUMP "SignalsPump" START;
                  ALTER PUMP "DetectionsPump" START;
                  ALTER PUMP "DetectionMessagesPump" START;
                  """
        return sql_cmd

    def _get_sql_pumps_off(self):
        sql_cmd = """
                  ALTER PUMP "DetectionMessagesPump" STOP;
                  ALTER PUMP "DetectionsPump" STOP;
                  ALTER PUMP "SignalsPump" STOP;
                  """
        return sql_cmd

    @defer.inlineCallbacks
    def _get_sql_defs(self, sqldefs=None, uconf={}, **kwargs):
        """
        Returns a fully substituted SQLStream SQL definition string.
        Using keyword arguments, you can update the default params passed in to spawn args.
        """
        assert self.spawn_args['agent_args'].has_key('sqlt_vars'), "Required SQL substitution vars have not been set yet."

        conf = self.spawn_args['agent_args']['sqlt_vars'].copy()
        conf.update(uconf)
        conf.update(kwargs)

        defs = sqldefs
        if defs == None:
            # no defs passed here, pull from attribute store
            defs = yield self.attribute_store_client.get(SQLTDEFS_KEY)

        assert defs != None and len(defs) > 0, "No definitions found!"

        template = string.Template(defs)

        defer.returnValue(template.substitute(conf))

    def _get_cores(self):
        """
        Gets the number of processors/cores on the current system.
        Adapted from http://codeliberates.blogspot.com/2008/05/detecting-cpuscores-in-python.html
        """
        if NO_MULTIPROCESSING:
            if hasattr(os, "sysconf"):
                if os.sysconf_names.has_key("SC_NPROCESSORS_ONLN"):
                    # linux + unix
                    ncpus = os.sysconf("SC_NPROCESSORS_ONLN")
                    if isinstance(ncpus, int) and ncpus > 0:
                        return ncpus
                else:
                    # osx
                    return int(os.popen2("sysctl -n hw.ncpu")[1].read())

            return 1
        else:
            return multiprocessing.cpu_count()

    def _get_opunit_status(self, cur_state=None):
        """
        Builds this Agent's status.
        @param cur_state    The current state that should be reported. Expected to be
                            any state of the BasicLifecycleObject states. If left blank,
                            uses the current process' state. This param is used for when
                            reporting state from a state transition method, such as
                            plc_terminate, and that state hasn't taken effect yet, but
                            we want to report as such.
        @returns            A dict containing status.
        """
        status = { 'id'          : self._opunit_id,
                   'proc_id'     : self.id.full,
                   'metrics'     : self.metrics,
                   'state'       : cur_state or self._StateObject__fsm.current_state }

        # filter out any private vars to the Agent inside of the sqlstream dict
        # "private" vars start with _ in the key name
        sqlstreams = {}
        for ssid,sinfo in self.sqlstreams.items():
            sqlstreams[ssid] = {}
            if sinfo.has_key('_fsm'):
                sqlstreams[ssid]['state'] = sinfo['_fsm'].current_state
            else:
                sqlstreams[ssid]['state'] = "?"
            for k,v in sinfo.items():
                if k[0:1] == "_":
                    continue
                sqlstreams[ssid][k] = v

        status['sqlstreams'] = sqlstreams

        return status

    def op_get_opunit_status(self, content, headers, msg):
        """
        Handles a request from the AppController to give it status of this AppAgent.
        """
        status = self._get_opunit_status()
        self.reply_ok(msg, status, {})

    def opunit_status(self, cur_state=None):
        """
        Sends the current status of this Agent/Op Unit.
        """
        content = self._get_opunit_status(cur_state)
        return self.rpc_send(self.target, 'opunit_status', content)

    @defer.inlineCallbacks
    def op_start_sqlstream(self, content, headers, msg):
        """
        Begins the process of starting and configuring a SQLStream instance on this op unit.
        The app controller calls here when it determines the op unit should spawn a new
        processing SQLStream.
        """
        log.info("op_start_sqlstream") # : %s" % str(self.sqlstreams[ssid]))

        ssid        = content['ssid']
        sqlt_vars   = content['sqlt_vars']
        defs        = yield self._get_sql_defs(uconf=sqlt_vars)
        failed      = False
        ex          = None

        try:
            self.start_sqlstream(ssid, sqlt_vars['inp_queue'], defs)
        except ValueError,e:
            failed = True
            ex = e

        if failed:
            resp = { 'response':'failed',
                     'exception':ex }
        else:
            resp = { 'response':'ok' }

        yield self.reply_ok(msg, resp, {})

    def _sqlstream_state_changed(self, fsm):
        """
        Called back when a SQLstream's FSM state has changed.
        Used to update the controller.
        """
        self.opunit_status() # report

    def start_sqlstream(self, ssid, inp_queue, sql_defs):
        """
        Returns a deferred you can yield on. When finished, the sqlstream should be up
        and running.
        """

        if ssid == None:
            raise ValueError("ssid is None")

        if ssid in self.sqlstreams.keys():
            log.error("Duplicate SSID requested")
            raise ValueError("Duplicate SSID requested (%s)" % ssid)

        chain = TaskChain()

        # vars needed for params
        sdpport     = str(5575 + int(ssid))
        hsqldbport  = str(9000 + int(ssid))
        dirname     = os.path.join(tempfile.gettempdir(), 'sqlstream.%s.%s' % (sdpport, hsqldbport))

        # record state
        self.sqlstreams[ssid] = {'ssid'         : ssid,
                                 'hsqldbport'   : hsqldbport,
                                 'sdpport'      : sdpport,
                                 'inp_queue'    : inp_queue,
                                 'dirname'      : dirname,
                                 'sql_defs'     : sql_defs,
                                 '_task_chain'  : chain}

        fsm = self._fsm_factory_class().create_fsm(self, ssid)
        self.sqlstreams[ssid]['_fsm'] = fsm
        fsm.add_state_change_notice(self._sqlstream_state_changed)

        # now move the fsm from its stock state to running
        for sym in fsm.get_path(SSStates.S_RUNNING):
            chain.append((fsm.process, [sym]))

        chaindef = chain.run()
        chaindef.addCallback(self._sqlstream_start_chain_success, sqlstreamid=ssid)
        chaindef.addErrback(self._sqlstream_start_chain_failure, sqlstreamid=ssid)

        return chaindef

    def _sqlstream_start_chain_success(self, result, **kwargs):
        ssid = kwargs.get("sqlstreamid")
        log.info("SQLstream (%s) started" % ssid)

        self.sqlstreams[ssid].pop('_task_chain')    # remove ref to chain

        self.opunit_status() # report

    def _sqlstream_start_chain_failure(self, failure, **kwargs):
        ssid = kwargs.get("sqlstreamid")
        log.error('SQLstream (%s) startup ERROR - %s' % (ssid, str(failure)))

        self.sqlstreams[ssid].pop('_task_chain')    # remove ref to chain TODO: keep for errors?

        failure.trap(StandardError)

        self.opunit_status() # report

    def _sqlstream_ended(self, result, *args):
        """
        SQLStream daemon has ended.
        """
        ssid = args[0].get('sqlstreamid')
        log.debug("SQLStream (%s) has ended" % ssid)
        self.sqlstreams[ssid].pop('_serverproc', None)

        self.opunit_status()

    def _sqlstream_started(self, result, *args, **kwargs):
        """
        SQLStream daemon has started.
        """
        ssid = args[0].get('sqlstreamid')
        log.debug("SQLStream server (%s) has started" % ssid)

        return result   # pass result down the chain

    def op_ctl_sqlstream(self, content, headers, msg):
        """
        Instructs a SQLStream to perform an operation like start or stop its pumps.
        The app controller calls here.
        """
        log.info("ctl_sqlstream received " + content['action'])

        self.reply_ok(msg, {'value':'ok'}, {})

        ssid = content['sqlstreamid']

        if content['action'] == 'pumps_on':
            self.sqlstreams[ssid]['_fsm'].run_to_state(SSStates.S_RUNNING)

        elif content['action'] == 'pumps_off':
            self.sqlstreams[ssid]['_fsm'].run_to_state(SSStates.S_DEFINED)

#
#
# ########################################################
#
#

class OSSSClientProcess(OSProcess):
    """
    SQLStream client process protocol.
    Upon construction, looks for a sqlcommands keyword argument. If that parameter exists,
    it will execute the commands upon launching the client process.
    """
    def __init__(self, binary=None, spawnargs=[], **kwargs):
        """
        @param sqlcommands  SQL commands to run in the client. May be None, which leaves stdin open.
        @param binroot      Root path to prepend on the binary name.
        """
        self.sqlcommands = kwargs.pop('sqlcommands', None)
        binroot = kwargs.pop("binroot", None)

        if binroot != None:
            binary = os.path.join(binroot, SSC_BIN)

        OSProcess.__init__(self, binary=binary, spawnargs=spawnargs, **kwargs)

        self.temp_file  = None

    def spawn(self, binary=None, args=[]):
        """
        Spawns the sqllineClient process.
        This override is to dump our sqlcommands into a file and tell sqllineClient to
        run that file. We modify the args keyword arg and send it up to the baseclass
        spawn implementation.
        """
        newargs = args[:] # XXX must copy becuase otherwise, args keep appending to some 
                          # shared args var?!?! makes no sense
        if len(args) == 0:
            newargs.extend(self.spawnargs)

        if self.sqlcommands != None:

            # dump sqlcommands into a temporary file
            f = tempfile.NamedTemporaryFile(delete=False)   # TODO: requires python 2.6
            f.write(self.sqlcommands)
            f.close()

            self.temp_file = f.name

            newargs.insert(0, "--run=%s" % self.temp_file)

        return OSProcess.spawn(self, binary, newargs)

    def processEnded(self, reason):
        OSProcess.processEnded(self, reason)

        # remove temp file if we created one earlier
        if self.temp_file != None:
            os.unlink(self.temp_file)

#
#
# ########################################################
#
#

class OSSSServerProcess(OSProcess):
    """
    OSProcess for starting a SQLstream daemon.
    
    Note the spawn method is overridden here to return a deferred which is called back
    when the server reports it is ready instead of when it exits. The deferred for when
    it exits can still be accessed by the deferred_exited attr, or you can simply add
    callbacks to it using the addCallback method. This override is so it can be used in a
    TaskChain.
    """
    def __init__(self, binary=None, spawnargs=[], **kwargs):

        binroot = kwargs.pop("binroot", None)

        if binroot != None:
            binary = os.path.join(binroot, SSD_BIN)

        OSProcess.__init__(self, binary=binary, spawnargs=spawnargs, **kwargs)
        self.ready_deferred = defer.Deferred()

    def spawn(self, binary=None, args=[]):
        """
        Calls the baseclass spawn but returns a deferred that's fired when the daemon
        announces it is ready.
        Then it can be used in a chain.
        """

        OSProcess.spawn(self, binary, args)

        return self.ready_deferred

    def addReadyCallback(self, callback, **kwargs):
        """
        Adds a callback to the deferred returned by spawn in this server override.
        Similar to addCallback, just for a different internal deferred.
        """
        self.ready_deferred.addCallback(callback, kwargs)

    def close(self, force=False, timeout=30):
        """
        Shut down the SQLstream daemon.
        This override exists simply to change the timeout param's default value.
        """
        return OSProcess.close(self, force, timeout)

    def _close_impl(self, force):
        """
        Safely exit SQLstream daemon.
        Make sure you use a long timeout when calling close, it takes a while.
        """

        # will do the quick shutdown if force, or if we never became ready
        if force or not self.ready_deferred.called:
            self.transport.signalProcess("KILL") # TODO: INT does not interrupt foreground process in a bash script!
                                                 # need to get PID of java process so we can shut it down nicer than this!
            self.transport.loseConnection()
        else:
            self.transport.write('!kill\n')

    def outReceived(self, data):
        OSProcess.outReceived(self, data)
        if (SSD_READY_STRING in data):

            # must simulate processEnded callback value
            cba = { 'exitcode' : 0,
                    'outlines' : self.outlines,
                    'errlines' : self.errlines }

            self.ready_deferred.callback(cba)
            #yield self.ready_callback(**self.ready_callbackargs)

    def processEnded(self, reason):
        # Process ended override.
        # This override is to signal ready deferred if we never did, just in case.
        if self.ready_deferred and not self.ready_deferred.called:

            # must simulate processEnded callback value
            cba = { 'exitcode' : 0,
                    'outlines' : self.outlines,
                    'errlines' : self.errlines }

            self.ready_deferred.callback(cba)

        OSProcess.processEnded(self, reason)


