#!/usr/bin/env/python

"""
@file ion/play/app_controller_service.py
@author Dave Foster <dfoster@asascience.com>
@brief Application Controller for load balancing
"""

import os, uuid, time
from twisted.internet import defer, reactor
from pkg_resources import resource_stream

try:
    import json
except:
    import simplejson as json

from ion.core import ioninit

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess
from ion.services.coi.attributestore import AttributeStoreClient
from ion.core.messaging import messaging
from ion.services.cei.epucontroller import PROVISIONER_VARS_KEY
from ion.services.cei.epu_reconfigure import EPUControllerClient

from support import TopicWorkerReceiver

CONF = ioninit.config(__name__)
INP_EXCHANGE_NAME   = CONF.getValue('INP_EXCHANGE_NAME', 'magnet.topic')
OUT_EXCHANGE_NAME   = CONF.getValue('OUT_EXCHANGE_NAME', 'magnet.topic')
DETECTION_TOPIC     = CONF.getValue('DETECTION_TOPIC',   'anf.detections')
STATIONS_PER_QUEUE  = CONF.getValue('STATIONS_PER_QUEUE', 2)
ANNOUNCE_QUEUE      = CONF.getValue('ANNOUNCE_QUEUE',    'instrument_announce')

SQLTDEFS_KEY = 'anf.seismic.sqltdefs' # the key to use in the store for sqlt defs
CORES_PER_SQLSTREAM = 2     # SQLStream instances use 2 cores each: a 4 core machine can handle 2 instances
SSD_READY_STRING = "Server ready; enter"
SSD_BIN = "bin/SQLstreamd"
SSC_BIN = "bin/sqllineClient"

DEBUG_WRITE_PROV_JSON=CONF.getValue("DEBUG_WRITE_PROV_JSON", False) # DEBUG

class AppControllerService(ServiceProcess):
    """
    Defines an application controller service to perform load balancing.
    """

    declare = ServiceProcess.service_declare(name = "app_controller",
                                             version = "0.1.0",
                                             dependencies=["attributestore"])

    def __init__(self, *args, **kwargs):
        ServiceProcess.__init__(self, *args, **kwargs)

        self.routing = {}   # mapping of queues to a list of bindings (station ids/sensor ids)
        self.workers = {}   # mapping of known worker vms to info about those vms (cores / running instances)

        # get configs for current exchange setup from exchange space, queues as per what TopicWorkerReceiver (below) uses
        exchcnfg = self.container.exchange_manager.exchange_space.exchange
        msgcnfg = messaging.worker('temp')

        # for timing
        self._timer = time.time()

        # for reconfigure events
        self._reconfigure_timeout = None

        # provisioner vars are common vars for all worker instances
        self.prov_vars = { 'sqlt_vars' : { 'inp_exchange'           : INP_EXCHANGE_NAME,
                                           'inp_exchange_type'      : exchcnfg.exchange_type,
                                           'inp_exchange_durable'   : str(exchcnfg.durable).lower(),
                                           'inp_exchange_autodelete': str(exchcnfg.auto_delete).lower(),
                                           'inp_queue_durable'      : msgcnfg['durable'],
                                           'inp_queue_autodelete'   : msgcnfg['auto_delete'],
                                           'det_topic'              : DETECTION_TOPIC,
                                           'det_exchange'           : OUT_EXCHANGE_NAME,
                                           'det_exchange_type'      : exchcnfg.exchange_type,
                                           'det_exchange_durable'   : str(exchcnfg.durable).lower(),
                                           'det_exchange_autodelete': str(exchcnfg.auto_delete).lower() } }

    @defer.inlineCallbacks
    def slc_init(self):
        # Service life cycle state.

        # consume the announcement queue
        self.announce_recv = TopicWorkerReceiver(name=ANNOUNCE_QUEUE,
                                                 scope='global',
                                                 process=self,
                                                 handler=self._recv_announce)

        # declares queue and starts listening on it
        yield self.announce_recv.attach()

        # get topic based routing to all sensor data (for anything missed on the announcement queue)
        #self.all_data_recv = TopicWorkerReceiver(name="ta_alldata",
        #                                         scope='global',
        #                                         binding_key = "ta.*.BHZ",
        #                                         process=self,
        #                                         handler=self._recv_data)

        #yield self.all_data_recv.attach()
        #yield self.all_data_recv.initialize()
        #self.counter = 0

        self.epu_controller_client = EPUControllerClient()

        self.attribute_store_client = AttributeStoreClient()
        yield self._load_sql_def()

    @defer.inlineCallbacks
    def _recv_announce(self, data, msg):
        """
        Received an instrument announcement. Set up a binding for it.
        """
        jsdata = json.loads(data)
        station_name = jsdata['content']

        log.info("Instrument Station Announce: " + station_name)

        found = self.has_station_binding(station_name)

        if found:
            log.error("Duplicate announcement")
        else:
            yield self.bind_station(station_name)

        yield msg.ack()

    #def _recv_data(self, data, msg):
    #    #log.info("<-- data packet" + msg.headers.__str__())
    #    log.info("data " + self.counter.__str__())
    #    self.counter += 1
    #    msg.ack()

    @defer.inlineCallbacks
    def bind_station(self, station_name, queue_name = None):
        """
        Binds a station to a queue. Typically you do not specify the queue name, this method
        will find a queue with room. If a queue name is given, no checking will be done - it 
        will simply be added.
        """
        if queue_name == None:
            queue_name = "W%s" % (len(self.routing.keys()) + 1)

            # find a queue with enough room
            added = False
            for queues in self.routing.keys():
                qlen = len(self.routing[queues])
                if qlen < STATIONS_PER_QUEUE:
                    queue_name = queues
                    break

        binding_key = '%s' % station_name

        yield self._create_queue(queue_name, binding_key)

        if not self.routing.has_key(queue_name):
            self.routing[queue_name] = []
            self.request_sqlstream(queue_name)

        self.routing[queue_name].append(station_name)

        log.info("Created binding %s to queue %s" % (binding_key, queue_name))

    @defer.inlineCallbacks
    def _create_queue(self, queue_name, binding_key):
        """
        Creates a queue and/or binding to a queue (just the binding if the queue exists).
        TODO: replace this with proper method of doing so.
        """
        recv = TopicWorkerReceiver(name=queue_name,
                                   scope='global',
                                   binding_key = binding_key,
                                   process=self)
        yield recv.initialize()   # creates queue but does not listen

    def request_sqlstream(self, queue_name, op_unit_id=None):
        """
        Requests a SQLStream operational unit to be created, or an additional SQLStream on an exiting operational unit.
        @param queue_name   The queue the SQL Stream unit should consume from.
        @param op_unit_id   The operational unit id that should be used to create a SQL Stream instance. If specified, will always create on that op unit. Otherwise, it will find available space on an existing VM or create a new VM.
        """

        # if this var is true, at the end of this method, instead of reconfiguring via
        # the decision engine, we will directly ask the agent on op_unit_id to spawn the 
        # sqlstream engine. This will hopefully be taken out when we can reconfigure 
        # workers on the fly.
        direct_request = False

        if op_unit_id != None and not self.workers.has_key(op_unit_id):
            log.error("request_sqlstream: op_unit (%s) requested but unknown" % op_unit_id)

        if op_unit_id == None:
            # find an available op unit
            for (worker,info) in self.workers.items():
                availcores = info['metrics']['cores'] - (len(info['sqlstreams']) * CORES_PER_SQLSTREAM)
                if availcores >= CORES_PER_SQLSTREAM:
                    log.info("request_sqlstream - asking existing operational unit (%s) to spawn new SQLStream" % worker)
                    # Request spawn new sqlstream instance on this worker
                    # wait for rpc message to app controller that says sqlstream is up
                    op_unit_id = worker

                    direct_request = True

                    # record the fact we are using this worker now
                    # TODO : needs to be an integer to indicate number of starting up, or a
                    # unique key per each starter
                    #info['sqlstreams']['spawning'] = True
                    break

        if op_unit_id == None:
            op_unit_id = str(uuid.uuid4())[:8]
            log.info("request_sqlstream - requesting new operational unit %s" % op_unit_id)

        # now we have an op_unit_id, update the config
        if not self.workers.has_key(op_unit_id):
            self.workers[op_unit_id] = {'metrics' : {'cores':2}, # all workers should have at least two, will be updated when status is updated
                                        'state' : '',
                                        'sqlstreams' : {}}
            streamcount = 0
        else:
            streamcount = len(self.workers[op_unit_id]['sqlstreams'])

        ssid = str(streamcount + 1)

        stream_conf = { 'sqlt_vars' : { 'inp_queue' : queue_name },
                        'ssid'      : ssid }

        self.workers[op_unit_id]['sqlstreams'][ssid] = { 'conf' : stream_conf,
                                                         'state': '' }

        if direct_request == True:
            self._start_sqlstream(op_unit_id, stream_conf)
        else:
            self.request_reconfigure() # schedule a reconfigure event!

    def request_reconfigure(self):
        """
        Rate limiter for actual request reconfigure call.
        Waits 4 seconds for any more reconfigure attempts, each of which delays the call by another 4 seconds.
        When the timeout finally calls, the real reconfigure is sent.
        """
        if self._reconfigure_timeout != None and self._reconfigure_timeout.active():
            log.info("request_reconfigure: delay already active, resetting to 4 seconds")
            self._reconfigure_timeout.reset(4)
        else:
            def callReconfigure():
                log.info("request_reconfigure: delay complete, actually performing reconfigure")
                self._reconfigure_timeout = None
                self._request_reconfigure()

            log.info("request_reconfigure: starting delay to 4 seconds to prevent flooding EPU controller")
            self._reconfigure_timeout = reactor.callLater(4, callReconfigure)

    def _request_reconfigure(self):
        """
        Requests a reconfiguration from the Decision Engine. This takes care of provisioning
        workers.

        This method builds the JSON required to reconfigure/configure the decision engine.
        """

        # TODO: likely does not need to send prov vars every time as this is reconfigure

        provvars = self.prov_vars.copy()
        #provvars['sqldefs'] = provvars['sqldefs'].replace("$", "$$")    # escape template vars once so it doesn't get clobbered in provisioner replacement

        conf = { 'preserve_n'         : len(self.workers),
                 #PROVISIONER_VARS_KEY : self.prov_vars,
                 'unique_instances'   : {} }

        for (wid, winfo) in self.workers.items():
            conf['unique_instances'][wid] = { 'agent_args': { 'sqlstreams' : [] } }
            conf['unique_instances'][wid]['agent_args'].update(self.prov_vars)
            ssdefs = conf['unique_instances'][wid]['agent_args']['sqlstreams']
            for (ssid, ssinfo) in winfo['sqlstreams'].items():
                ssdefs.append( { 'ssid'      : ssinfo['conf']['ssid'],
                                 'sqlt_vars' : ssinfo['conf']['sqlt_vars'] } )

        if DEBUG_WRITE_PROV_JSON:
            f = open('/tmp/prov.json', 'w')
            json.dump(conf, f, indent=1)
            f.close()
            log.debug("Wrote /tmp/prov.json due to DEBUG_WRITE_PROV_JSON being on in the config.")

            for (wid, winfo) in conf['unique_instances'].items():
                wdict = winfo.copy()
                wdict['agent_args']['opunit_id'] = wid

                f = open('/tmp/sa-' + wid + '.json', 'w')
                json.dump(wdict, f, indent=1)
                f.close()

                log.debug("Wrote /tmp/sa-%s.json." % wid)

            # merge and write individual worker configs while we're at it
            #for (wid, winfo) in self.workers.items():
            #    wdict = { 'agent_args': { 'opunit_id' : wid,
            #                              'sqlstreams': str(conf['unique_instances'][wid]['sqlstreams']),   # TODO: unstringify this
            #                              'sqlt_vars' : self.prov_vars['sqlt_vars'] } }

            #    f = open('/tmp/sa-' + wid + '.json', 'w')
            #    json.dump(wdict, f, indent=1)
            #    f.close()

        self.epu_controller_client.reconfigure(conf)

        # record the time we sent this
        self._timer = time.time()

    def has_station_binding(self, station_name):
        """
        Returns true if we know about this station.
        """
        for queues in self.routing.keys():
            found = station_name in self.routing[queues]
            if found:
                return True

        return False

    def op_opunit_status(self, content, headers, msg):
        """
        Handles an application agent reporting an operational unit's status.
        Details include its current state, metrics about the system, status of
        SQLstream instances.
        """
        self._update_opunit_status(content)
        self.reply_ok(msg, {'value': 'ok'}, {})

    def request_opunit_status(self, opunit_id):
        """
        Asks an AppAgent to report in its status.
        """
        proc_id = self.workers[opunit_id]['proc_id']
        d = self.rpc_send(proc_id, 'get_opunit_status', {})
        d.addCallback(lambda res: self._update_opunit_status(res[0]))

    def _update_opunit_status(self, status):
        """
        Internal method to handle updating an op unit's status.
        Status updates can either come from heartbeats initiated by the AppAgent, or
        on request from the AppController. This method handles both of those.
        """
        opunit_id   = status['id']
        proc_id     = status['proc_id']
        state       = status['state']
        metrics     = status['metrics']
        sqlstreams  = status['sqlstreams']

        sstext = ""
        for ssid, sinfo in sqlstreams.items():
            sstext += "(id: %s status: %s queue: %s)" % (ssid, sinfo['state'], sinfo['inp_queue'])

        # get amount of time since we requested opunits
        timediff = time.time() - self._timer

        log.info("Op Unit (%s) status update (+%s sec) : state (%s), sqlstreams (%d): %s" % (opunit_id, str(timediff), state, len(sqlstreams), sstext))

        if not self.workers.has_key(status['id']):
            self.workers[status['id']] = {}

        self.workers[opunit_id].update({'metrics':metrics,
                                        'state': state,
                                        'proc_id': proc_id,
                                        'sqlstreams':sqlstreams})

        # display a message if all known opunits are running
        allstate = [ssinfo.get('state', None) for ssinfo in [winfo['sqlstreams'] for winfo in self.workers.values() if len(winfo['sqlstreams'])> 0]]
        if set(allstate) == set(["SUCCESS"]):
            log.info("All known workers are running (+%s sec)" % timediff)

    def _start_sqlstream(self, op_unit_id, conf):
        """
        Tells an op unit to start a SQLStream instance.
        """
        proc_id = self.workers[op_unit_id]['proc_id']
        self.rpc_send(proc_id, 'start_sqlstream', conf)

    def _load_sql_def(self):
        """
        Loads SQL Templates from disk and puts them in a store.
        Called at startup.

        XXX fix:
        Gets SQLStream detection application SQL definitions, either from
        disk or in memory. SQL files stored on disk are loaded once and stored
        in memory after they have been translated through string.Template.

        You may override the SQL defs by sending an RPC message ("set_sql_defs") to
        the Application Controller. These defs will take the place of the current
        in memory defs. They are expected to be templates, in which certain vars will be
        updated. See op_set_sql_defs for more information.
        """
        fulltemplatelist = []
        for filename in ["catalog.sqlt", "funcs.sqlt", "detections.sqlt"]:
            f = resource_stream(__name__, "data/%s" % filename)
            #f = open(os.path.join(os.path.dirname(__file__), "app_controller_service", filename), "r")
            fulltemplatelist.extend(f.readlines())
            f.close()

        fulltemplate = "".join(fulltemplatelist)

        self.attribute_store_client.put(SQLTDEFS_KEY, fulltemplate)

    def op_set_sql_defs(self, content, headers, msg):
        """
        Updates the current cached SQL defs for the SQLStream detection application.
        This overrides what is found on the disk.

        Note it does not update the SQL files on disk, so if the AppControllerService is
        restarted, it will need to be updated with the current defs again.

        This method expects that the only key in content, also named content, is a full 
        SQL definition (the concatenation of "catalog.sqlt" and "detections.sqlt") with
        Python string.Template vars as substitution points for the following variables:

        * inp_queue                 - The input queue name to read messages from.
        * inp_queue_autodelete      - The input queue's auto_delete setting.
        * inp_queue_durable         - The input queue's durable setting.
        * inp_exchange              - The exchange where the input queue resides.
        * inp_exchange_type         - The exchange's type (topic/fanout/direct).
        * inp_exchange_durable      - The exchange's durable setting.
        * inp_exchange_autodelete   - The exchange's auto_delete setting.
        * det_topic                 - The topic string that should be used for detections.
        * det_exchange              - The exchange where detections should be published.
        * det_exchange_type         - The detection exchange's type (topic/fanout/direct).
        * det_exchange_durable      - The detection exchange's durable setting.
        * det_exchange_autodelete   - The detection exchange's auto_delete setting.

        If these variables are not present, no error is thrown - it will use whatever you
        gave it. So your updated SQL definitions may hardcode the variables above.
        """
        defs = content['content']
        self.attribute_store_client.put(SQLTDEFS_KEY, defs)
        self.reply_ok(msg, { 'value': 'ok'}, {})

# Spawn of the process using the module name
factory = ProcessFactory(AppControllerService)

