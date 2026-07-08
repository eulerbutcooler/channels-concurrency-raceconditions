# A websocket chat server in Go.
Features:
- Clients connect over WS
- One client can broadcast a message to every other connected client
- Clients can join room and send room scoped messages
- Clients can leave room and disconnect.

The cool part is all of this happens concurrenctly across hundreds/thousands of goroutines without a single mutex


*"Don't communicate by sharing memory. Share memory by communicating"*
