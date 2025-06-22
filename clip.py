from obswebsocket import obsws, requests

websocket = obsws("localhost", 4455, "")
websocket.connect()
websocket.call(requests.SaveReplayBuffer())
websocket.disconnect()