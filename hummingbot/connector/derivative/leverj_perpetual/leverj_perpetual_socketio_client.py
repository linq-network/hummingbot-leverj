import asyncio
import socketio

from typing import List, Tuple, Any

from hummingbot.connector.derivative.leverj_perpetual.leverj_perpetual_auth import LeverjPerpetualAuth



class LeverjPerpetualSocketIOClient():
    def __init__(self, output: asyncio.Queue, domain: str, auth: LeverjPerpetualAuth = None):
        self.domain = domain
        self._output: asyncio.Queue = output
        self._leverj_auth = auth
        sio = socketio.AsyncClient(logger=False, reconnection_delay_max=1)
        
        @sio.event
        async def connect():
            try:
                if self._leverj_auth is not None:
                    headers = self._leverj_auth.generate_request_headers('GET', '/register', {})
                    req_data = {
                        "headers": headers,
                        "method": 'GET',
                        "uri": "/register",
                        "params": {},
                        "body": {},
                        "retry": False
                    }
                    await sio.emit('GET /register', req_data)
                else:
                    await sio.emit('GET /instrument', {})
            except Exception as e:
                print(e)

        @sio.on("difforderbook")
        async def difforderbook(msg):
            array_msg = ["difforderbook", msg]
            self._output.put_nowait(array_msg)
        
        @sio.on("orderbook")
        async def orderbook(msg):
            array_msg = ["orderbook", msg]
            self._output.put_nowait(array_msg)

        @sio.on("index")
        async def index(msg):
            array_msg = ["index", msg]
            self._output.put_nowait(array_msg)

        @sio.on("order_add")
        async def order_add(msg):
            array_msg = ["order_add", msg]
            self._output.put_nowait(array_msg)

        @sio.on("order_update")
        async def order_update(msg):
            array_msg = ["order_update", msg]
            self._output.put_nowait(array_msg)

        @sio.on("order_execution")
        async def order_execution(msg):
            array_msg = ["order_execution", msg]
            self._output.put_nowait(array_msg)

        @sio.on("position")
        async def position(msg):
            array_msg = ["position", msg]
            self._output.put_nowait(array_msg)

        @sio.on("order_del")
        async def order_del(msg):
            array_msg = ["order_del", msg]
            self._output.put_nowait(array_msg)

        @sio.on("account_balance")
        async def account_balance(msg):
            array_msg = ["account_balance", msg]
            self._output.put_nowait(array_msg)

        @sio.on("order_cancelled")
        async def order_cancelled(msg):
            array_msg = ["order_cancelled", msg]
            self._output.put_nowait(array_msg)

        self._sio = sio

    @property
    def sio(self):
        return self._sio

    async def run_client(self):
        await self._sio.connect(self.domain, socketio_path='/futures/socket.io', transports=['websocket'])