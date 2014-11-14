Sillynet
===================
It is just a one file python program to simulate skynet(https://github.com/cloudwu/skynet).
It only implements some basic functions to just work. It has similar architecture with
skynet, a global message queue with each entry corresponding to a specific service's
private message queue which ensure each service's callback function only run once at any time;
also a dedicated thread pool is used to dispatch message and run the callback function.

It contains 4 built-in services: Log,Tcp(most retarded Tcp framwork ever :) ,Timer and Echo. 
The user can extend it  by writing service in python, see test.py for detail.

Config
===================
See sillynet.cfg for detail

Requirements
===================
To make it work, you need twisted + python 2.7.
Only partially tested and written in couple of hours , just for fun :)
