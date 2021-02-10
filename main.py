import asyncio

from typing import List, Dict

from fastapi import FastAPI, WebSocket, WebSocketDisconnect

app = FastAPI()


class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        print('active_connections: ', self.active_connections)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def send_message(self, message: str, websocket: WebSocket):
        await websocket.send_text(message)

    async def broadcast_all(self, data: Dict):
        for connection in self.active_connections:
            await connection.send_json(data)


class SubscriptionConnectionManager(ConnectionManager):
    def __init__(self):
        super().__init__()

        # format: {'2bPxCpPF...': [client1, client2,..]}
        self.subscriptions: Dict[str, List[WebSocket]] = {}

        self.pending_notify: List = []

    async def subscribe_to(self, websocket: WebSocket, id: str):
        if id in self.subscriptions:
            self.subscriptions[id].append(websocket)
        else:
            self.subscriptions[id] = [websocket]
        print('subscriptions: ', self.subscriptions)

    async def broadcast_to(self, id: str, data: Dict):
        if id in self.subscriptions:
            for connection in self.subscriptions[id]:
                await connection.send_json(data)

    def unsubscribe_from(self, websocket: WebSocket, id: str):
        if id in self.subscriptions:
            self.subscriptions[id].remove(websocket)

    async def wait_for_changes(self, id: str):
        while True:
            if id in self.pending_notify:
                return self.pending_notify.remove(id)
            await asyncio.sleep(0.3)

    def notify(self, id: str):
        self.pending_notify.append(id)


manager = SubscriptionConnectionManager()


async def wait_first(*futures):
    # https://stackoverflow.com/a/45169115
    # https://stackoverflow.com/a/65505529
    done, pending = await asyncio.wait(futures, return_when=asyncio.FIRST_COMPLETED)

    for task in done:
        exception = task.exception()
        if exception:
            raise exception

    gather = asyncio.gather(*pending)
    gather.cancel()
    try:
        await gather
    except asyncio.CancelledError:
        pass
    return done.pop().result()


@app.websocket("/tables")
async def ws_tables_endpoint(
    websocket: WebSocket,
    id: str,
):
    # Clients connect to Faust's Table ID that they'd like to receive updates
    # `id` is general purpose, could be a dataset ID, or it could store any other content, example:
    # ws://localhost:8000/tables?id=2bPxCpPFvmK4PU9eaGaUNC

    await manager.connect(websocket)
    await manager.subscribe_to(websocket, id)
    try:
        while True:
            # It monitors for 2 possible events: wait_for_changes OR receive_text
            # receive_text is required (and only used for) to the proper throwing of WebSocketDisconnect
            result = await wait_first(manager.wait_for_changes(id), websocket.receive_text())

            # TODO: Request to the correct Faust Shard (using Faust's own table router) to get the content
            # The faust agent could live here in the same process (not necessarily in the same loop)
            content = {'a': 10, 'b': 20}  # hard-coded content

            print(f'there is changes for table {id} - notifying all connected clients')
            await manager.broadcast_to(id, content)

    except WebSocketDisconnect:
        print(f'disconnecting client: {websocket}...')
        manager.disconnect(websocket)
        manager.unsubscribe_from(websocket, id)


@app.get("/")
def root():
    return {}


@app.on_event("startup")
async def startup_event():
    loop = asyncio.get_running_loop()
    loop.call_later(5, manager.notify, '2bPxCpPFvmK4PU9eaGaUNC')
    loop.call_later(10, manager.notify, '2bPxCpPFvmK4PU9eaGaUNC')