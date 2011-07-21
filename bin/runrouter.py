#!/usr/bin/env python
# vim: ai ts=4 sts=4 et sw=4


import os
import select
import sys
import time
import threading
import traceback
import urllib2
import logging, logging.handlers
import Queue

from datetime import datetime

sys.path.append(os.path.join('.'))
os.environ['DJANGO_SETTINGS_MODULE'] = os.getenv('DJANGO_SETTINGS_MODULE','settings')
from rapidsms.conf import settings
from rapidsms.backends.base import BackendBase
from rapidsms.log.mixin import LoggerMixin
from rapidsms.messages import OutgoingMessage, IncomingMessage

from django import http
from django.http import HttpResponse, HttpResponseBadRequest
from django.core.handlers.wsgi import WSGIHandler, STATUS_CODE_TEXT
from django.core.servers.basehttp import WSGIServer, WSGIRequestHandler


class RapidWSGIHandler(WSGIHandler, LoggerMixin):
    """ WSGIHandler without Django middleware and signal calls """

    def _logger_name(self):
        return "%s/%s" % (self.backend._logger_name(), 'handler')

    def __call__(self, environ, start_response):
        request = self.request_class(environ)
        self.debug('Request from %s' % request.get_host())
        try:
            response = self.backend.handle_request(request)
        except Exception, e:
            self.exception(e)
            response = http.HttpResponseServerError()
        try:
            status_text = STATUS_CODE_TEXT[response.status_code]
        except KeyError:
            status_text = 'UNKNOWN STATUS CODE'
        status = '%s %s' % (response.status_code, status_text)
        response_headers = [(str(k), str(v)) for k, v in response.items()]
        start_response(status, response_headers)
        return response


class RapidHttpServer(WSGIServer):
    """ WSGIServer that doesn't block on handle_request """

    def handle_request(self, timeout=1.0):
        reads, writes, errors = (self, ), (), ()
        reads, writes, errors = select.select(reads, writes, errors, timeout)
        if reads:
            WSGIServer.handle_request(self)


class Router(object, LoggerMixin):
    """
    This Router is a simple threaded 
    """


    def __init__(self):

        self.backends = {}
        self.logger = None

        self.running = False
        """TODO: Docs"""

        self.accepting = False
        """TODO: Docs"""

        self._queue = Queue.Queue()
        """Pending incoming messages, populated by Router.incoming_message."""
        
        self.host = '127.0.0.1'
        self.port = 8888
        self.gateway = '127.0.0.1'
        self._server = None


    def add_backend(self, name, module_name, config=None):
        """
        Find the backend named *module_name*, instantiate it, and add it
        to the dict of backends to be polled for incoming messages, once
        the router is started. Return the backend instance.
        """

        cls = BackendBase.find(module_name)
        if cls is None: return None

        config = self._clean_backend_config(config or {})
        backend = cls(self, name, **config)
        self.backends[name] = backend
        return backend


    @staticmethod
    def _clean_backend_config(config):
        """
        Return ``config`` (a dict) with the keys downcased. (This is
        intended to make the backend configuration case insensitive.)
        """

        return dict([
            (key.lower(), val)
            for key, val in config.iteritems()
        ])


    @staticmethod
    def _wait(func, timeout):
        """
        Keep calling *func* (a lambda function) until it returns True,
        for a maximum of *timeout* seconds. Return True if *func* does,
        or False if time runs out.
        """

        for n in range(0, timeout*10):
            if func(): return True
            else: time.sleep(0.1)

        return False


    def _init_web_server(self):
        server_address = (self.host, int(self.port))
        self.info('Starting HTTP server on {0}:{1}'.format(*server_address))
        self.handler = RapidWSGIHandler()
        self.handler.backend = self
        self.server = RapidHttpServer(server_address, WSGIRequestHandler)
        self.server.set_app(self.handler)

        worker = threading.Thread(
            name="http",
            target=self._run_web_server,
        )
        worker.daemon = True
        worker.start()
        self._server = worker


    def _run_web_server(self):
        while self.running or self._starting_backends:
            self.server.handle_request()


    def _start_backend(self, backend):
        """
        Start *backend*, and return True when it terminates. If an
        exception is raised, wait five seconds and restart it.
        """

        while True:
            try:
                self.debug("starting backend")
                started = backend.start()
                self.debug("backend %s terminated normally" % backend)
                return True
            
            except Exception, e:
                self.debug("caught exception in backend %s: %s" % (backend, e))
                backend.exception()

                # this flows sort of backwards. wait for five seconds
                # (to give the backend a break before retrying), but
                # abort and return if self.accepting is ever False (ie,
                # the router is shutting down). this ensures that we
                # don't delay shutdown, because that causes me to SIG
                # KILL, which prevents things from stopping cleanly.
                # also check _starting_backends to see if we're in the startup
                # state.  if we are, don't exit, because accepting won't be
                # True until we've finished starting up
                def should_exit():
                    return not (self._starting_backends or self.accepting)
                self.debug('waiting 15 seconds before retrying')
                if self._wait(should_exit, 15):
                    self.debug('returning from _start_backend')
                    return None


    def _start_all_backends(self):
        """
        Start all backends registed via Router.add_backend, by calling
        self._start_backend in a new daemon thread for each.
        """

        for backend in self.backends.values():
            worker = threading.Thread(
                name=backend._logger_name(),
                target=self._start_backend,
                args=(backend,))

            worker.daemon = True
            worker.start()

            # stash the worker thread in the backend, so we can check
            # whether it's still alive when _stop_all_backends is called
            backend.__thread = worker


    def _stop_all_backends(self):
        """
        Notify all backends registered via Router.add_backend that they
        should stop. This method cannot guarantee that backends **will**
        stop in a timely manner.
        """

        for backend in self.backends.values():
            alive = backend.__thread.is_alive
            if not alive(): continue
            backend.stop()

            if not self._wait(lambda: not alive(), 5):
                backend.error("Worker thread did not terminate")


    def start(self):
        """
        Start polling the backends registered via Router.add_backend for
        incoming messages, and keep doing so until a KeyboardInterrupt
        or SystemExit is raised, or Router.stop is called.
        """

        # dump some debug info for now
        #self.info("BACKENDS: %r" % (self.backends))

        self.info("Starting %s..." % settings.PROJECT_NAME)
        self._starting_backends = True
        self._start_all_backends()

#         Kick off the web server
        self._init_web_server()
        self.running = True
        self.debug("Started")

        # now that all of the backends are started, we are ready to start
        # accepting messages. (if we tried to dispatch an message to an
        # app before it had started, it might not be configured yet.)
        self.accepting = True
        self._starting_backends = False

        try:
            while self.running:

                # fetch the next pending incoming message, if one is
                # available immediately. this increments the number of
                # "tasks" on the queue, which MUST be decremented later
                # to avoid deadlock during graceful shutdown. (it calls
                # _queue.join to ensure that all pending messages are
                # processed before stopping.).
                #
                # for more infomation on Queues, see:
                # help(Queue.Queue.task_done)
                try:
                    self.incoming(self._queue.get(block=False))
                    self._queue.task_done()

                # if there were no messages waiting, wait a very short
                # (in human terms) time before looping to check again.
                except Queue.Empty:
                    time.sleep(0.1)

        # stopped via ctrl+c
        except KeyboardInterrupt:
            self.warn("Caught KeyboardInterrupt")
            self.running = False

        # stopped via sys.exit
        except SystemExit:
            self.warn("Caught SystemExit")
            self.running = False

        # while shutting down, refuse to accept any new messages. the
        # backend(s) might have to discard them, but at least they can
        # pass the refusal back to the device/gateway where possible
        self.accepting = False

        self.debug("Stopping...")
        self._stop_all_backends()
        self.info("Stopped")


    def stop(self, graceful=False):
        """
        Stop the router, which unblocks the Router.start method as soon
        as possible. This may leave unprocessed messages in the incoming
        or outgoing queues.

        If the optional *graceful* argument is True, the router does its
        best to avoid discarding any messages, by refusing to accept new
        incoming messages and blocking (by calling Router.join) until
        all currently pending messages are processed.
        """

        if graceful:
            self.accepting = False
            self.join()

        self.running = False


    def join(self):
        """
        Block until the incoming message queue is empty. This method
        can potentially block forever, if it is called while this Router
        is accepting incoming messages.
        """

        self._queue.join()
        return True


    def incoming_message(self, msg):
        """
        Add *msg* to the incoming message queue and return True, or
        return False if this router is not currently accepting new
        messages (either because the queue is full, or we are busy
        shutting down).

        Adding a message to the queue is no guarantee that it will be
        processed any time soon (although the queue is regularly polled
        while Router.start is blocking), or responded to at all.
        """

        if not self.accepting:
            return False

        try:
            self._queue.put(msg)
            return True

        # if the queue is of a limited size, it may raise the Full
        # exception. there's no sense exploding (especially since we
        # have a bunch of pending messages), so just refuse to accept
        # it. hopefully, the backend can in turn refuse it
        except Queue.Full:
            return False


    def fetch_url(self, url):
        """
        Wrapper around url open, mostly here so we can monkey patch over it in unit tests.
        """
        response = urlopen(url, timeout=15)
        return response.getcode()


    def incoming(self, msg, backend):
        """
        This sends incoming message to the web thread
        """
        self.info("Incoming (%s): %s" %\
            (msg.connection, msg.text))
        # FIXME configuration
        url_dict = {
            'backend':backend.name,
            'sender':connection.identity,
            'message':msg.text
        }
        url = 'http://127.0.0.1:8000/router/receive/?backend=%(backend)s&sender=%(sender)s&message=%(message)s' % url_dict
        status_code = self.fetch_url(url)
        if int(status_code/100) == 2:
            self.info("Incoming (%s: %s) SENT" % ( msg.connection, msg.text))
        else:
            self.error("Incoming (%s: %s) NOT SENT, got status: %s .. queued for later delivery." % ( msg.connection, msg.text, status_code))
            try:
                # try to re-queue it and retry sending
                self._queue.put(msg)
            except Queue.Full:
                # Bummer, this message just got completely dropped.
                # FIXME fix this problem?
                pass


    def handle_request(self, request):
        """
        This sends messages to the backends from the web thread
        """
        self.debug('Received request: %s' % request.GET)
        
        recipient = request.GET.get('recipient','')
        message = request.GET.get('message','')
        backend = request.GET.get('backend','')

        msg = OutgoingMessage(recipient, message)
        # FIXME better error handling
        try:
            self.backends[backend].send(msg)
            return HttpResponse('OK')
        except Exception, exc:
            self.error(traceback.format_exc(exc))
            return HttpResponse(status=400)


# a single instance of the router singleton is available globally, like
# the db connection. it shouldn't be necessary to muck with this very
# often (since most interaction with the Router happens within an App or
# Backend, which have their own .router property), but when it is, it
# should be done via this process global
router = Router()


numeric_level = getattr(logging, settings.LOG_LEVEL.upper())
format = logging.Formatter(settings.LOG_FORMAT)

router.logger = logging.getLogger()
router.logger.setLevel(numeric_level)

# start logging to the screen (via stderr)
# TODO: allow the values used here to be
# specified as arguments to this command
handler = logging.StreamHandler()
router.logger.addHandler(handler)
handler.setLevel(numeric_level)
handler.setFormatter(format)

# start logging to file
file_handler = logging.handlers.RotatingFileHandler(
    settings.LOG_FILE, maxBytes=settings.LOG_SIZE,
    backupCount=settings.LOG_BACKUPS)
router.logger.addHandler(file_handler)
file_handler.setFormatter(format)

# add each backend
for name, conf in settings.INSTALLED_BACKENDS.items():
    router.add_backend(name, conf.pop("ENGINE"), conf)

router.start()




