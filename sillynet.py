# ===================================================
# A silly skynet(https://github.com/cloudwu/skynet)
# simulation in a single python file .
# Only craft some major feature that makes it just work
# For fun, enjoy :)
# ===================================================

import sys
import ConfigParser
import threading
import logging
from twisted.internet import protocol,reactor,task
from twisted.internet.endpoints import TCP4ClientEndpoint
from Queue import Queue
from thread import LockType
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
        with _lock:
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

    def __init__(self,uid,source,dest,tag,payload):
        self.uid = uid
        self.source = source
        self.dest = dest
        self.tag = tag
        self.payload = payload

    def makeMessage(source,dest,tag,payload):
        return Message(_UIDGenerator.nextUID(),
                       source,
                       dest,
                       tag,
                       payload)

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

    def create(self,path,name,queue_len):
        self.queue = Queue(queue_len)
        service = None
        try:
            service = imp.load_source("Service",path)
        except:
            raise Exception("Cannot load service by path:%s"%(path))

        # create the service object
        self._entry = service()

        # get this service's name
        self.name = name

    def process(self,messenger):
        """ The main function for processing message 

            This function will be called in the back thread, so
            keep it thread safe with global states.
        """
        message = None

        try:
           message = self._queue.get_nowait()
        except:
            return

        self.entry.process(message,messenger)

class KernelService:
    """ Represent the kernel service interface """

    name = None
    """ The service name """

    def startService(self):
        """ Start the service 

            This will typically cause a new thread generated,
            so startService will not block in main thread
        """
        pass
    def stopService(self):
        """ Stop the service """
        pass
    def enqueueMessage(self,msg):
        """ Enqueue the message into this kernel service internal queue """
        pass

class RemoteMessageService(KernelService):
    """ This class is an implementation for remote message kernel service 

        This service is just a service for dumping out the message through
        the TCP network using a extreamly simple protocol. 
    """
    _queue = None

    _loopTask = None

    _thread = None

    _exit = False

    def __init__(self,queue):
        self.name = "remote"
        self._queue = queue

    class TcpSender(protocol.Protocol):
        _data = None
        def send(self):
            self.transport.write(_data)

    class TcpSenderFactory(protocol.Factory):
        _data = None
        def __init__(self,data):
            self._data = data

        def buildProtocol(self,addr):
            return TcpSender(self._data)

    def _fire(p):
        p.send()
    
    def _schedule(self):
        while not self._exit:
            data = None
            try :
                data = self._queue.get_nowait()
            except:
                return

            point = TCP4ClientEndpoint(reactor, 
                                       data.payload["addr"] , 
                                       data.payload["port"])

            d = point.connect(self.TcpSenderFactory())
            d.addCallback(_fire)
        # Stop here
        self._loopTask.stop()
        reactor.stop()

    def _twistedMain(self):
        self._loopTask = task.LoopingCall(self._schedule)
        self.start(0.025)
        reactor.run()

    # thread safe
    def stop(self):
        self._exit = true
        self._thread.join()
            
    def start(self):
        self._thread = threading.Thread(target=self._twistedMain)
        self._thread.start()

    def enqueue(self,msg):
        try :
            self._queue.put_nowait(msg)
        except:
            return False
        return True

class LogMessageService(KernelService):
    """ This class is used to handle the log service """
    _logger = None
    """ The logger handler """
    _queue = None
    _thread = None
    _exit = False

    def __init__(self,name,queue):
        self.name = "logger"
        self._logger = logging.getLogger(name)
        self._queue = queue

    def _process(self,data):

        self._logger.log(
            data.payload["severity"],
            data.payload["message"])

    def _threadMain(self):
        while not self._exit:
            data = None
            try :
                data = _queue.get(True,1)
            except :
                continue
            self._process(data)


    def startService(self):
        self._thread = threading.Thread(
            target = self._threadMain)
        self._thread.start()

    def stopService(self):
        self._exit = True
        self._thread.join()

    def enqueue(self,msg):
        try :
            self._queue.put_nowait(msg)
        except:
            return False
        return True

class MessengerImp(Messenger):

    """ Messenger implementation class 

        This messenger is just a wrapper function to send out the message 
    """

    _serviceManager = None
    """ The centeral service manager """

    def __init__(self,sm):
        self._serviceManager = sm

    def sendMessage(self,source,dest,tag,payload):
        self._serviceManager.enqueueMessage(
            Message.makeMessage(source,dest,tag,payload))
        
class ServiceAccessor:
    """ This class is a helper class for getting service 

        Use it for with expression for getting a blocked queue 
        and push it back to the queue

        with ServiceAccessor(manager,weight) as queue:
            process_with_queue(queue)
    """

    _manager = None
    """ A static reference for internal queue of service_manager"""

    _weight = 1
    """ The weight value """

    _returnService = None 
    """ The return queue """

    def __init__(self,service_manager,weight):
        self._manager = service_manager
        self._weight= weight

    def __enter__(self):
        self._returnService = self._detachService(self,self._weight)
        return self._returnService

    def __exit__(self):
        self._attachServiceQueue(self._returnValue)

    def _detachService(self,weight):
        """ This function is used to dequeue the request 

            The back up thread use this function to retreive
            the message/request in the global queue. The return
            value is a _QUEUE_ not a message. This function may
            _BLOCK_
        """
        max_sleep_time = 512
        min_sleep_time = 4
        max_prob_time = 10

        while True:

            # Before trying to grab the queue, first check if we have data
            # in the queue now , otherwise we need to sleep until the message
            # are there. 
            sleep_time = min_sleep_time
            self._manager._lock.lock()
            while self._msgCount < weight:

                slee_time *= 2
                if sleep_time > max_sleep_time :
                    slee_time = max_sleep_time

                self._manager._cv.wait(sleep_time)

            # When we reach here, it means we should have got at least one 
            # message in queue, however, we cannot resolve the herding problem.
            # The input weight is used to make the hearding not that painful
            for _ in range(max_prob_time):

                service = None
                try :
                    service = self._manager._queue.get_nowait()
                except:
                    continue

                # Now we have a queue and we need to check whether it has message
                if service.queue.empty():
                    self._manager._queue.put(service)
                    continue
                else:
                    return service
       
    def _attachService(self,service):
        self._manager._queue.put(service)

class ServiceManager:
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

    _lock = threading.Lock()
    """ This lock is used to signal backend thread they can work 

        Since we use 2 level of queue , a condition variable is 
        needed here to make backend thread sleep at a greate time.
        We use this _lock and condition variable to protect the 
        real message number regardless of whichever the dest is
    """
    _cv = threading.Condition()
    """ Condition variable """

    _msgCount = 0
    """ The message count totally """

    _exit = False
    """ The indicator to let all the thread exit """

    _kernelService = None
    """ kernel service """

    _messengerImp = None
    """ Messenger implementation """

    _threadPool = []

    def __init__(self):
        self._messengerImp = MessengerImp(self)

    def _enqueueKernelMessage(self,message):
        """ This function is used to enqueue those specific message

            The kernel message is basic message that provides internally
            by the service. Originally, the skynet use command to handle
            this and also has log/gate services which is actually built-in.
            I am just simulating it, so some limited kernel message is OK
            for us.
        """
        if message.dest in self._kernelService.keys():
            service = self._kernelService[message.dest]
            return service.enqueue(message)
        else:
            return False

    def enqueueMessage(self,message):

        if self._enqueueKernelMessage(message) == True:
            return True;

        if message.dest in self._serviceMap:
            service = self._serviceMap[message.dest]
            try :
                serivce.queue.put_nowait(message)
            except:
                return False

            # Notice here, we don't acquire the lock in order to achieve
            # performance. However this may result in the backend thread
            # lost the signal. We remody this situation by making the 
            # back end thread wake up periodically .
            self._msgCount+=1
            self._cv.notify()
            return True

        return False

    def _threadMain(self,weight,messenger):
        """ This function is main function for thread

            The thread will be assigned with a weight value which is
            used to avoid hearding while polling. The weight is the
            lowest number of available message in queue to wake up
            a thread. So at least one thread needs to be assigned with 1
        """
        while not self._exit:
            with ServiceAccessor(self,weigth) as service:
                assert not service.queue.emtpy(), "The service queue is empty!!!"
                service.process(messenger)

    def _formatWeight(self,num):
        if num <= 4:
            return [1 for _ in range(num)]
        else:
            ret = [1,1,1,1]
            num-=4

            round1 = min(num,12)

            for i in range(round1):
                ret.append( i + 1 )

            num -= round1

            if num == 0:
                return ret

            for i in range(num):
                ret.append( round1 + i*2 )

            return ret

    def _startThreadPool(self,num):
        w = self._formatWeight(num)

        for i in range(num):
            th = threading.Thread(target=self.__threadMain,
                                  args=(w[i],self._messengerImp,))
            th.start()
            self._threadPool.append(th)

    def _initKernelService(self,queue_size):
        self._kernelService = {
            "remote" : RemoteMessageService(queue_size) ,
            "logger" : LogMessageService(queue_size)
        }

    def startService(self):
        """ This function is the main function for user to start service """

        # load configuraton file
        parser = ConfigParser.RawConfigParser()
        parser.read("simplenet.cfg")

        serviceSize = parser.readint("Server","serviceSize")
        outstandingQueueSize = parser.readint("Server","outstandingQueueSize")
        threadSize = parser.readint("Server","threadSize")

        # initialize global queue
        self._queue = Queue(serviceSize)

        # initialize kernel service
        self._initKernelService(outstandingQueueSize)

        # loading all the service now
        services = parser.items("Service")
        for service in services:
            service_name = service[0]
            service_path = service[1]
            serv = Service(service_path,service_name,outstandingQueueSize)
            self._queue.put_nowait(serv)
            self._serviceMap[service_name] = serv

        # initialize the thread pool
        self._startThreadPool(threadSize)

    def stopService(self):
        self._exit = True

        for th in self._threadPool:
            th.join()

        for th in self._kernelService:
            th.stopService()

if __name__ == "__main__":
    mgr = ServiceManager()
    mgr.startService()

    while True:
        time.sleep(10)


