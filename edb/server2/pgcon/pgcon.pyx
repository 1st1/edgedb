#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

cimport cython
cimport cpython

from libc.stdint cimport int8_t, uint8_t, int16_t, uint16_t, \
                         int32_t, uint32_t, int64_t, uint64_t, \
                         UINT32_MAX

from edgedb.pgproto.pgproto cimport (
    WriteBuffer,
    ReadBuffer,
)

import asyncio

include "./corepgcon.pyx"


cdef class PGProto(CorePGProto):

    def __init__(self, addr, connected_fut, con_params, loop):
        # type of `con_params` is `_ConnectionParameters`
        CorePGProto.__init__(self, con_params)

        self.loop = loop
        self.waiter = connected_fut
        self.cancel_waiter = None
        self.cancel_sent_waiter = None

        self.address = addr

        self.return_extra = False

        self.last_query = None

        self.closing = False
        self.is_reading = True

        self.completed_callback = self._on_waiter_completed

        self.queries_count = 0

    def set_connection(self, connection):
        self.connection = connection

    def get_server_pid(self):
        return self.backend_pid

    def is_in_transaction(self):
        # PQTRANS_INTRANS = idle, within transaction block
        # PQTRANS_INERROR = idle, within failed transaction
        return self.xact_status in (PQTRANS_INTRANS, PQTRANS_INERROR)

    cdef inline resume_reading(self):
        if not self.is_reading:
            self.is_reading = True
            self.transport.resume_reading()

    cdef inline pause_reading(self):
        if self.is_reading:
            self.is_reading = False
            self.transport.pause_reading()

    @cython.iterable_coroutine
    async def execute_anonymous(self, egdecon, bytes query,
                                WriteBuffer bind_data):

        if self.cancel_waiter is not None:
            await self.cancel_waiter
        if self.cancel_sent_waiter is not None:
            await self.cancel_sent_waiter
            self.cancel_sent_waiter = None

        self._check_state()

        waiter = self._new_waiter()
        try:
            self._execute_anonymous(egdecon, query, bind_data)  # network op
            self.last_query = query
        except Exception as ex:
            waiter.set_exception(ex)
            self._coreproto_error()
        finally:
            return await waiter

    def is_closed(self):
        return self.closing

    def is_connected(self):
        return not self.closing and self.con_status == PGCON_OK

    def abort(self):
        if self.closing:
            return
        self.closing = True
        self._handle_waiter_on_connection_lost(None)
        self._terminate()
        self.transport.abort()

    @cython.iterable_coroutine
    async def close(self):
        if self.closing:
            return

        self.closing = True

        if self.cancel_sent_waiter is not None:
            await self.cancel_sent_waiter
            self.cancel_sent_waiter = None

        if self.cancel_waiter is not None:
            await self.cancel_waiter

        if self.waiter is not None:
            # If there is a query running, cancel it
            self._request_cancel()
            await self.cancel_sent_waiter
            self.cancel_sent_waiter = None
            if self.cancel_waiter is not None:
                await self.cancel_waiter

        assert self.waiter is None

        # Ask the server to terminate the connection and wait for it
        # to drop.
        self.waiter = self._new_waiter()
        self._terminate()
        try:
            await self.waiter
        except ConnectionResetError:
            # There appears to be a difference in behaviour of asyncio
            # in Windows, where, instead of calling protocol.connection_lost()
            # a ConnectionResetError will be thrown into the task.
            pass
        finally:
            self.waiter = None
        self.transport.abort()

    def _request_cancel(self):
        self.cancel_waiter = self.loop.create_future()
        self.cancel_sent_waiter = self.loop.create_future()
        self.connection._cancel_current_command(self.cancel_sent_waiter)
        self._set_state(PGPROTO_CANCELLED)

    def _on_waiter_completed(self, fut):
        if fut is not self.waiter or self.cancel_waiter is not None:
            return
        if fut.cancelled():
            self._request_cancel()

    cdef _handle_waiter_on_connection_lost(self, cause):
        if self.waiter is not None and not self.waiter.done():
            exc = RuntimeError(
                'connection was closed in the middle of '
                'operation')
            if cause is not None:
                exc.__cause__ = cause
            self.waiter.set_exception(exc)
        self.waiter = None

    cdef _check_state(self):
        if self.cancel_waiter is not None:
            raise RuntimeError(
                'cannot perform operation: another operation is cancelling')
        if self.closing:
            raise RuntimeError(
                'cannot perform operation: connection is closed')
        if self.waiter is not None:
            raise RuntimeError(
                'cannot perform operation: another operation is in progress')

    def _is_cancelling(self):
        return (
            self.cancel_waiter is not None or
            self.cancel_sent_waiter is not None
        )

    @cython.iterable_coroutine
    async def _wait_for_cancellation(self):
        if self.cancel_sent_waiter is not None:
            await self.cancel_sent_waiter
            self.cancel_sent_waiter = None
        if self.cancel_waiter is not None:
            await self.cancel_waiter

    cdef _coreproto_error(self):
        try:
            if self.waiter is not None:
                if not self.waiter.done():
                    raise RuntimeError(
                        'waiter is not done while handling critical '
                        'protocol error')
                self.waiter = None
        finally:
            self.abort()

    cdef _new_waiter(self):
        if self.waiter is not None:
            raise RuntimeError(
                'cannot perform operation: another operation is in progress')
        self.waiter = self.loop.create_future()
        self.waiter.add_done_callback(self.completed_callback)
        return self.waiter

    cdef _on_result__connect(self, object waiter):
        waiter.set_result(True)

    cdef _on_result__execute_anonymous(self, object waiter):
        waiter.set_result(True)

    cdef _dispatch_result(self):
        waiter = self.waiter
        self.waiter = None

        if PG_DEBUG:
            if waiter is None:
                raise RuntimeError('_on_result: waiter is None')

        if waiter.cancelled():
            return

        if waiter.done():
            raise RuntimeError('_on_result: waiter is done')

        if self.result_type == RESULT_FAILED:
            if isinstance(self.result, dict):
                exc = RuntimeError(f'RESULT_FAILED: {self.result}')
                # exc = apg_exc_base.PostgresError.new(
                #     self.result, query=self.last_query)
            else:
                exc = self.result
            waiter.set_exception(exc)
            return

        try:
            if self.state == PGPROTO_AUTH:
                self._on_result__connect(waiter)

            elif self.state == PGPROTO_EXECUTE_ANONYMOUS:
                self._on_result__execute_anonymous(waiter)

            else:
                raise RuntimeError(
                    'got result for unknown protocol state {}'.
                    format(self.state))

        except Exception as exc:
            waiter.set_exception(exc)

    cdef _on_result(self):
        if self.cancel_waiter is not None:
            # We have received the result of a cancelled command.
            if not self.cancel_waiter.done():
                # The cancellation future might have been cancelled
                # by the cancellation of the entire task running the query.
                self.cancel_waiter.set_result(None)
            self.cancel_waiter = None
            if self.waiter is not None and self.waiter.done():
                self.waiter = None
            if self.waiter is None:
                return

        try:
            self._dispatch_result()
        finally:
            self.last_query = None
            self.return_extra = False

    cdef _on_notice(self, parsed):
        # self.connection._process_log_message(parsed, self.last_query)
        pass

    cdef _on_notification(self, pid, channel, payload):
        # self.connection._process_notification(pid, channel, payload)
        pass

    cdef _on_connection_lost(self, exc):
        if self.closing:
            # The connection was lost because
            # Protocol.close() was called
            if self.waiter is not None and not self.waiter.done():
                if exc is None:
                    self.waiter.set_result(None)
                else:
                    self.waiter.set_exception(exc)
            self.waiter = None
        else:
            # The connection was lost because it was
            # terminated or due to another error;
            # Throw an error in any awaiting waiter.
            self.closing = True
            self._handle_waiter_on_connection_lost(exc)
