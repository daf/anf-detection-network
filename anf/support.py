#!/usr/bin/env/python

"""
@file support.py 
@author Dave Foster <dfoster@asascience.com>
@brief Support classes for ANF.
"""

import os
from twisted.internet import reactor

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer

from ion.core.messaging import messaging
from ion.core.messaging.receiver import Receiver
from ion.util.os_process import OSProcess

class TopicWorkerReceiver(Receiver):
    """
    A TopicWorkerReceiver is a Receiver from a worker queue that pays attention to its binding_key property. It also turns auto_delete off so consumers can detach without destroying the queues.

    TODO: This should be replaced by appropriate pubsub arch stuff.
    """

    def __init__(self, *args, **kwargs):
        """
        @param binding_key The binding key to use. By default, uses the computed xname, but can take a topic based string with wildcards.
        """
        binding_key = kwargs.pop("binding_key", None)
        Receiver.__init__(self, *args, **kwargs)
        if binding_key == None:
            binding_key = self.xname

        self.binding_key = binding_key

    @defer.inlineCallbacks
    def on_initialize(self, *args, **kwargs):
        """
        @retval Deferred
        """
        assert self.xname, "Receiver must have a name"

        name_config = messaging.worker(self.xname)
        # TODO: this needs auto_delete: False, but only for the queues, not the exchange. No way to specify that.
        name_config.update({'name_type':'worker', 'binding_key':self.binding_key, 'routing_key':self.binding_key})

        #log.info("CONF IN " + name_config.__str__())

        yield self._init_receiver(name_config, store_config=True)

#
#
# ########################################################
#
#

class DetectionConsumer(TopicWorkerReceiver):
    """
    Sample detection message consumer, mainly used for testing.
    """
    @defer.inlineCallbacks
    def on_initialize(self, *args, **kwargs):
        yield TopicWorkerReceiver.on_initialize(self, *args, **kwargs)

        self._detections = []
        self.add_handler(self.on_detection)

    def on_detection(self, content, msg):
        msg.ack()
        log.info("Detection:\n %s" % str(content))

        lines = content.split("\n")
        detdata = {}
        for s in lines:
            if len(s.strip()) == 0:
                continue

            (k, v) = s.split(":", 1)
            detdata[k] = v

        self._detections.append(detdata)

#
#
# ########################################################
#
#

class EnvOSProcess(OSProcess):
    """
    Temporary fixup to OSProcess to allow environment mangling in its spawn method.
    """

    def __init__(self, extraenv={}, **kwargs):
        OSProcess.__init__(self, **kwargs)
        self._extraenv = extraenv

    def spawn(self, binary=None, args=[]):
        """
        Spawns an OS process via twisted's reactor.

        @returns    A deferred that is called back on process ending.
                    WARNING: it is not safe to yield on this deferred as the process
                    may never terminate! Use the close method to safely close a
                    process. You may yield on the deferred returned by that.
        """
        if self.used:
            raise RuntimeError("Already used this process protocol")

        if binary == None:
            binary = self.binary

        if binary == None:
            log.error("No binary specified")
            raise RuntimeError("No binary specified")

        theargs = [binary]

        # arguments passed in here always take precedence.
        if len(args) == 0:
            theargs.extend(self.spawnargs)
        else:
            theargs.extend(args)

        # adjust env
        env = os.environ.copy()
        env.update(self._extraenv)

        log.debug("EnvOSProcess::spawn %s %s" % (str(binary), " ".join(theargs)))
        reactor.spawnProcess(self, binary, theargs, env=env)
        self.used = True

        return self.deferred_exited

