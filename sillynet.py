# ===================================================
# A silly skynet(https://github.com/cloudwu/skynet)
# simulation in a single Python file .
# Only craft some major feature that makes it just work
# For fun, enjoy :)
# ===================================================

import ConfigParser
import threading
import logging
import imp
import time

from twisted.internet import protocol,reactor,task
from twisted.internet.endpoints import TCP4ClientEndpoint
from Queue import Queue
from messenger import Messenger

class UIDGenerator:
    """ This class is used to generate UID atomically

        No atomic int existed without using extension, so
        I just use lock to simulate one here , which is
        supposed to have BAD performance :(
    """

    _lock = None
    """ Lock to protect the integer """

    _counter = 0
    """ The current counter """

    def nextUID(self):
        with self._lock:
            self._counter += 1
            return self._counter

    def __init__(self):
        self._lock = threading.Lock()

_UIDGenerator = UIDGenerator()

class Message:
    """ The message represents a basic schedualable message"""

    uid=None
    """ The id for this message """

    source=None
    """ The source for this message """

    dest=None
    """ The dest for this message """

    tag=None
    """ A tag that is specifically designed for underlying service

        The service could tag a type value or whatever to tell the
        peer side how to interpret the payload. For a remote message,
        it will be tag with 0 , which means the payload actually is 
        a byte buffer.
    """

    payload=None
    """ This the payload for this message 

        Although python is a dynamic language, but
        keep constraint to original C implementaion 
        seems have no harm. User wants to send object
        could definitly insert the object in payload
        field. 
    """

    def __init__(self,uid,source,dest,payload,tag):
        self.uid = uid
        self.source = source
        self.dest = dest
        self.payload = payload
        self.tag = tag
    
    @staticmethod
    def makeMessage(source,dest,payload,tag):
        return Message(_UIDGenerator.nextUID(),
                       source,
                       dest,
                       payload,
                       tag)

class Service:
    queue = None
    """ The internal message queue for this service
       
        This queue stores all the message specifically
        sends to this specific service.
    """
    _entry = None
    """ The service entry function """

    name = None
    """ This service's name """

    def __init__(self,entry,name,queue_len):
        self.queue = Queue(queue_len)
        # create the service object
        self._entry = entry
        # get this service's name
        self.name = name

    def process(self):
        """ The main function for processing message 

            This function will be called in the back thread, so
            keep it thread safe with global states.
        """
        message = None
        
        try:
            message = self.queue.get(True)
        except:
            return
        self._entry.process(message)
        
    @staticmethod
    def createBuiltInService(serviceObj,name,queue_size,messenger):
        return Service(serviceObj(messenger),name,queue_size) 
    
    @staticmethod
    def createExternalService(path,name,queue_size,messenger):
        s = None
        s = imp.load_source("Service",path)
        return Service(s.Service(messenger),name,queue_size)
        
# This class is used to handle IO/Timer message 

class IOMessageService():
    """ This class is an implementation for IO message bulit-in service

         It allows user to 1) sechedule a callback function at a timer based
                           2) send out a piece of packet through TCP ( for simplicity, one one send out message )
    """
    _queue = None

    _loopTask = None

    _thread = None

    _exit = False

    def __init__(self,queue_size):
        self.name = "remote"
        self._queue = Queue(queue_size)

    class TcpSender(protocol.Protocol):
        _data = None
        def send(self):
            self.transport.write(self._data)

    class TcpSenderFactory(protocol.Factory):
        _data = None
        def __init__(self,data):
            self._data = data

        def buildProtocol(self,addr):
            return self.TcpSender(self._data)
        
    @staticmethod
    def _fire(p):
        p.send()
        return p

    def _handleTcpTraffic(self,data):
        point = TCP4ClientEndpoint(reactor,data.payload["address"],data.payload["port"])

        d = point.connect(self.TcpSenderFactory())
        d.addCallback(self._fire)

    @staticmethod
    def _timerCallback(data):
        data.payload["callback"](data.payload["data"])

    def _handleTimerEvent(self,data):
        reactor.callLater(
            data.payload["timeout"],
            self._timerCallback,
            data)
    
    def _schedule(self):
        while not self._exit:
            data = None
            try :
                data = self._queue.get_nowait()
            except:
                return

            if data.tag == Messenger.tcpServiceTag:
                self._handleTcpTraffic(data)
            elif data.tag == Messenger.timerServiceTag:
                self._handleTimerEvent(data)
            else:
                continue

        # Stop here
        self._loopTask.stop()
        reactor.stop()

    def _twistedMain(self):
        self._loopTask = task.LoopingCall(self._schedule)
        self._loopTask.start(0.025)
        # Since we are not in main thread, stop using signal handler
        reactor.run(installSignalHandlers=0)

    # thread safe
    def stopService(self):
        self._exit = True
        self._thread.join()
            
    def startService(self):
        self._thread = threading.Thread(target=self._twistedMain)
        self._thread.start()

    def enqueue(self,msg):
        try :
            self._queue.put_nowait(msg)
        except:
            return False
        return True

class LogMessageService():
    """ This class is used to handle the log service """
    _loggerName = "sillynet"
    
    def process(self,message,messenger):
        logger = logging.getLogger(self._loggerName)
        logger.log( message.payload["severity"] , message.payload["message"] )
    
# =====================================
# Built-in service
# =====================================

class EchoMessageService():
    _messenger = None

    def __init__(self,messenger):
        self._messenger = messenger

    """ This is a simple echo message service for debug usage """
    def process(self,message):
        print "[ECHO:(%d)]:%s"%(message.uid,message.payload)


class MessengerImp(Messenger):

    """ Messenger implementation class 

        This messenger is just a wrapper function to send out the message 
    """

    _serviceManager = None
    """ The central service manager """

    def __init__(self,sm):
        self._serviceManager = sm

    def sendMessage(self,source,dest,payload,tag):
        self._serviceManager.enqueueMessage(
            Message.makeMessage(source,dest,payload,tag))
        
class ServiceAccessor:
    """ This class is a helper class for getting service 

        Use it for with expression for getting a blocked queue 
        and push it back to the queue

        with ServiceAccessor(manager,weight) as queue:
            process_with_queue(queue)
    """

    _manager = None
    """ A static reference for internal queue of service_manager"""

    _returnService = None 
    """ The return queue """

    def __init__(self,service_manager):
        self._manager = service_manager

    def __enter__(self):
        self._returnService = self._detachService()
        return self._returnService

    def __exit__(self,t,v,tb):
        self._attachService(self._returnService)

    def _detachService(self):
        """ This function is used to dequeue the request 

            The back up thread use this function to retreive
            the message/request in the global queue. The return
            value is a _QUEUE_ not a message. This function may
            _BLOCK_
        """

        while True:                
            
            # At least probing each service internal queue once
            for _ in range(self._manager.serviceSize):
                service = None
                try :
                    service = self._manager._queue.get_nowait()
                except:
                    continue

                # Now we have a queue and we need to check whether it has message
                if service.queue.empty():
                    self._manager._queue.put_nowait(service)
                    continue
                else:
                    return service
                
            # Wait until we have any message pending there
            with self._manager._cv:
                while self._manager._msgCount == 0:
                    self._manager._cv.wait()

       
    def _attachService(self,service):
        self._manager._queue.put(service)

class ServiceManager:
    messengerImp = None
    """ Messenger implementation """
    
    serviceSize = 0
    """ All service registered inside of the service manager"""
    
    
    _queue = None
    """ The internal queue for collecting task 

        Similar with skynet, the queue is designed with
        2 level queue, with this global queue, each of
        its entry will point to another queue releated
        to a specific service's private message queue 

    """

    _serviceMap = dict()
    """ The map registers all the named service 

        Although the dictionary is not thread safe, however
        the dictionary will remain immutable during the process,
        so no lock is used here
    """

    _lock = None
    """ This lock is used to signal backend thread they can work 

        Since we use 2 level of queue , a condition variable is 
        needed here to make backend thread sleep at a greate time.
        We use this _lock and condition variable to protect the 
        real message number regardless of whichever the dest is
    """
    _cv = None
    """ Condition variable """

    _msgCount = 0
    """ The total alive message count """

    _exit = False
    """ The indicator to let all the thread exit """

    _ioService = None
    """ IO service """

    _threadPool = []
    
    _builtInService = [
        ("echo",EchoMessageService)
    ]

    def __init__(self):
        self.messengerImp = MessengerImp(self)
        self._lock = threading.Lock()
        self._cv = threading.Condition(self._lock)

    def _enqueueRemoteMessage(self,message):
        """ This function is used to enqueue remote message """
        
        if message.dest == "io":
            return self._ioService.enqueue(message)
        else:
            return False

    def enqueueMessage(self,message):

        if self._enqueueRemoteMessage(message) == True:
            return True;

        if message.dest in self._serviceMap:
            service = self._serviceMap[message.dest]
            try :
                service.queue.put_nowait(message)
            except:
                return False

            # Python condition variable cannot notify while not holding 
            # the lock ! Which make us _MUST_ do the real lock enter and
            # leave.
            with self._cv :
                self._msgCount += 1
                self._cv.notify()
                 
            return True

        return False

    def _threadMain(self,messenger):
        """ This function is main function for thread

            The thread will be assigned with a weight value which is
            used to avoid herding while polling. The weight is the
            lowest number of available message in queue to wake up
            a thread. So at least one thread needs to be assigned with 1
        """
        while not self._exit:
            with ServiceAccessor(self) as service:
                # assert not service.queue.emtpy(), "The service queue is empty!!!"
                service.process()

    def _startThreadPool(self,num):

        for _ in range(num):
            th = threading.Thread(target=self._threadMain,args=(self.messengerImp,))
            th.start()
            self._threadPool.append(th)

    def _initIOService(self,queue_size):
        self._ioService = IOMessageService(queue_size)
        self._ioService.startService()
        
    def _initBuiltInService(self,queue_size):
        for entry in self._builtInService:
            serv = Service.createBuiltInService(entry[1], entry[0], queue_size,self.messengerImp)
            self._queue.put_nowait(serv)
            self._serviceMap[entry[0]] = serv
    
    def _initExternalService(self,serviceList,queue_size):
        for service in serviceList:
            service_name = service[0]
            service_path = service[1]
            serv = Service.createExternalService(service_path,service_name,queue_size,self.messengerImp)
            self._queue.put_nowait(serv)
            self._serviceMap[service_name] = serv
            

    def startService(self):
        """ This function is the main function for user to start service """

        # load configuration file
        parser = ConfigParser.RawConfigParser()
        parser.read("sillynet.cfg")

        outstandingQueueSize = parser.getint("Server","outstandingQueueSize")
        threadSize = parser.getint("Server","threadSize")
        services = parser.items("Service")
		self.serviceSize = len(self._builtInService) + len(services)
		# initialize global queue
        self._queue = Queue(self.serviceSize)
		
		# initialize IO service
        self._initIOService(outstandingQueueSize)
        
        # initialize built-in service
        self._initBuiltInService(outstandingQueueSize)
		
		# initialize all the external service
        self._initExternalService(services,outstandingQueueSize)

        # initialize the thread pool
        self._startThreadPool(threadSize)
        
        print "Silly net service starts now!"
        print "Stay hungry , stay foolish!"

    def stopService(self):
        self._exit = True

        for th in self._threadPool:
            th.join()

        self._ioService.stopService()
            
        print "Silly net service stops now!"

if __name__ == "__main__":
    mgr = ServiceManager()
    mgr.startService()
    
    while True:
        time.sleep(10)


