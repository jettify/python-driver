import asyncio
import atexit
import weakref

from functools import partial
import logging
from threading import Event, Thread, Lock

from cassandra import OperationTimedOut
from cassandra.connection import Connection, ConnectionShutdown
from cassandra.protocol import RegisterMessage


log = logging.getLogger(__name__)


def _cleanup(cleanup_weakref):
    try:
        cleanup_weakref()._cleanup()
    except ReferenceError:
        return


class AioLoopThread(object):

    _lock = None
    _thread = None

    def __init__(self):
        self._lock = Lock()
        self._loop = asyncio.get_event_loop()

    def maybe_start(self):
        with self._lock:
            if (not self._loop) or not self._loop.is_running():
                self._thread = Thread(target=self._bootstrap,
                                      name="cassandra_driver_event_loop")
                self._thread.daemon = True
                self._thread.start()
                atexit.register(partial(_cleanup, weakref.ref(self)))

    def _bootstrap(self):
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    def _cleanup(self):
        if self._thread:
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._thread.join(timeout=1.0)
            if self._thread.is_alive():
                log.warning("Event loop thread could not be joined, so "
                            "shutdown may not be clean. Please call "
                            "Cluster.shutdown() to avoid this.")
            log.debug("Event loop thread was joined")


class AsyncioConnection(Connection):

    _loop_thread = None
    _total_reqd_bytes = 0

    @classmethod
    def initialize_reactor(cls):
        if not cls._loop_thread:
            cls._loop_thread = AioLoopThread()

    @classmethod
    def factory(cls, *args, **kwargs):
        """
        A factory function which returns connections which have
        succeeded in connecting and are ready for service (or
        raises an exception otherwise).
        """
        timeout = kwargs.pop('timeout', 5.0)
        conn = cls(*args, **kwargs)
        conn.connected_event.wait(timeout)
        if conn.last_error:
            raise conn.last_error
        elif not conn.connected_event.is_set():
            conn.close()
            raise OperationTimedOut("Timed out creating connection")
        else:
            return conn


    def __init__(self, *args, **kwargs):

        self._loop_thread.maybe_start()
        self._loop = self._loop_thread._loop
        super().__init__(*args, **kwargs)

        self.connected_event = Event()
        self._loop_connected_event = Event()

        self._reader = None
        self._writer = None

        self._callbacks = {}
        self._loop.call_soon_threadsafe(asyncio.async, self.add_connection())
        self._loop_connected_event.wait(5)
        if not self._loop_connected_event.is_set():
            raise OperationTimedOut("Timed out creating connection")

        self._send_options_message()
        self._loop.call_soon_threadsafe(asyncio.async, self.handle_read())


    @asyncio.coroutine
    def add_connection(self):
        self._reader, self._writer = yield from asyncio.open_connection(
            host=self.host, port=self.port)
        self._loop_connected_event.set()

    def close(self):
        """
        Disconnect and error-out all callbacks.
        """
        with self.lock:
            if self.is_closed:
                return
            self.is_closed = True

        log.debug("Closing connection (%s) to %s", id(self), self.host)
        self._writer.transport.close()
        self._writer = None
        self._reader = None

        log.debug("Closed socket to %s", self.host)

        if not self.is_defunct:
            self.error_all_callbacks(
                ConnectionShutdown("Connection to %s was closed" % self.host))
            # don't leave in-progress operations hanging
            self.connected_event.set()

    @asyncio.coroutine
    def handle_read(self):
        """
        Process the incoming data buffer.
        """
        while True:
            try:
                while True:
                    buf = yield from self._reader.read(self.in_buffer_size)
                    self._iobuf.write(buf)
                    if len(buf) < self.in_buffer_size:
                        break
                else:
                    log.debug("Connection %s closed by server", self)
                    self.close()
                    return

            except OSError as exc:
                log.debug("Exception during socket recv for %s: %s", self, exc)
                self.defunct(exc)

                self.close()

            if self._iobuf.tell():
                self.process_io_buffer()
            else:
                log.debug("Connection %s closed by server", self)
                self.close()
                return

    def push(self, data):
        self._loop.call_soon_threadsafe(self._writer.write, data)

    def register_watcher(self, event_type, callback, register_timeout=None):
        self._push_watchers[event_type].add(callback)
        self.wait_for_response(
            RegisterMessage(event_list=[event_type]),
            timeout=register_timeout)

    def register_watchers(self, type_callback_dict, register_timeout=None):
        for event_type, callback in type_callback_dict.items():
            self._push_watchers[event_type].add(callback)
        self.wait_for_response(
            RegisterMessage(event_list=type_callback_dict.keys()),
            timeout=register_timeout)
