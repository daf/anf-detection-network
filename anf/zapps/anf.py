#!/usr/bin/env python

"""
@file anf/zapps/anf.py
@author Dave Foster <dfoster@asascience.com>
@brief ANF Detection Network - App Controller Service / DetectionConsumer
"""

from twisted.internet import defer

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core.process.process import ProcessDesc
from ion.core.pack import app_supervisor

@defer.inlineCallbacks
def start(container, starttype, app_definition, *args, **kwargs):
    anf_services =[{ 'name':'attributestore',
                     'module':'ion.services.coi.attributestore',
                     'class':'AttributeStoreService'},
                   { 'name':'app_controller',
                     'module': 'anf.app_controller_service',
                     'class': 'AppControllerService'}]

    app_sup_desc = ProcessDesc(name="app-supervisor-" + app_definition.name,
                               module=app_supervisor.__name__,
                               spawnargs={'spawn-procs':anf_services})

    supid = yield app_sup_desc.spawn()

    res = (supid.full, [app_sup_desc])
    log.info("Started ANF App Controller")
    defer.returnValue(res)

@defer.inlineCallbacks
def stop(container, state):
    log.info("Stopping ANF App Controller")
    supdesc = state[0]
    yield supdesc.terminate()

