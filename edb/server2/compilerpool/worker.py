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


import argparse
import gc
import os
import pickle
import socket
import struct

import setproctitle


_len_unpacker = struct.Struct('!I').unpack
_len_packer = struct.Struct('!I').pack


class _Spawn(Exception):
    pass


class BaseWorker:
    def __init__(self, sockname):
        self._sock = None
        self._sockname = sockname

    def _sock_recvall(self, size):
        data = b''
        to_recv = size
        while to_recv:
            block = self._sock.recv(to_recv)
            if not block:
                raise ConnectionAbortedError
            data += block
            to_recv -= len(block)
        return data

    def recv_msg(self):
        payload_len = _len_unpacker(self._sock_recvall(4))[0]
        return pickle.loads(self._sock_recvall(payload_len))

    def send_msg(self, msg):
        payload = pickle.dumps(msg)
        payload = _len_packer(len(payload)) + payload
        self._sock.sendall(payload)

    def connect(self):
        assert self._sock is None
        self._sock = socket.socket(socket.AF_UNIX)
        self._sock.connect(self._sockname)
        self._sock.sendall(struct.pack('!i', os.getpid()))
        if self._sock_recvall(1) != b'\x01':
            exit(1)

    def listen(self):
        self.on_listen()

        try:
            while True:
                incoming = self.recv_msg()
                outgoing = self.on_message(incoming)
                self.send_msg(outgoing)
        except _Spawn:
            self._sock.close()
            self._sock = None
            raise

    def on_listen(self):
        pass

    def on_message(self, msg):
        raise NotImplementedError


class TemplateWorker(BaseWorker):

    def on_listen(self):
        setproctitle.setproctitle('edgedb-template')

        gc.collect()
        gc.collect()
        gc.collect()
        gc.freeze()

    def on_message(self, msg):
        mtype = msg[0]

        if mtype == 'spawn':
            pid = os.fork()
            if pid:
                # parent
                return ('spawned', pid)
            else:
                # child
                raise _Spawn
        else:
            raise RuntimeError(f'template worker: unknown message {mtype}')


class CompileWorker(BaseWorker):

    def on_listen(self):
        setproctitle.setproctitle('edgedb-compiler')

    def on_message(self, msg):
        print('compile1!!', msg)
        return [12]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--control', type=str)
    parser.add_argument('--type', type=str)
    args = parser.parse_args()

    if args.type == 'template':
        worker = TemplateWorker(args.control)
    else:
        raise RuntimeError(f'unknown worker type {args.type!r}')

    worker.connect()

    spawn = False
    try:
        worker.listen()
    except _Spawn:
        spawn = True

    if spawn:
        worker = CompileWorker(args.control)
        worker.connect()
        worker.listen()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        exit(1)
