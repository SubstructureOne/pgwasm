pgwasm
======

pgwasm is a Python interface to PostgreSQL proxied over WebSockets for use in
WebAssembly (specifically using Pyodide). It is based on pg8000 and uses
wasmsockets for communication.

wasmsockets handles the proxying of WebSocket calls to the Javascript 
interface when it detects it is being run in a WebAssembly environment. When 
run in a native Python environment, it instead uses the websockets package. 
This allows pgwasm to be tested in a native environment. However in both 
cases, since all network traffic is proxied over WebSockets, a WebSocket 
proxy is also required for the Postgres server; it cannot connect to a 
PostgreSQL server directly. The websockify package is convenient for 
implementing this proxy to PostgreSQL.
