class Messenger:
    """ This class provides protocol for using the message interface 

        The user will get a instance of this object in its process callback
        function, then user can use it to send message to other user's queue
    """
    # =========================
    # Constant tag value 
    # =========================
    userTag = 100
    """ This tag value is the minimum user allowed message tag value """
    unusedTag = 0
    tcpServiceTag = 1
    timerServiceTag = 2
    logServiceTag = 3

    # ===========================
    # Message routine
    # ===========================
    def sendMessage(self,source,dest,payload,tag):
        """ The main interface used to send the message 

            Simple usage will be like this:
            messenger.sendMessage(somePayload,self.name,"OtherService")
        """
        pass

    def sendTcpMessage(self,source,payload,addr,port):
        self.sendMessage(source,"io",{
            "address":addr,
            "port":port,
            "packet":payload
        },Messenger.tcpServiceTag)

    def sendTimerMessage(self,source,timeout,callback,data):
        self.sendMessage(source,"io",{
            "callback":callback,
            "data":data,
            "timeout":timeout
            },Messenger.timerServiceTag)

    def sendLogMessage(self,source,severity,msg):
        """ This function is used to write log in center log service

            The input severity MUST be python logging severity, and
            the underlying service will use the related python module
            to produce the log information 
        """
        self.sendMessage(source,"logger",{
            "severity":severity,
            "message":msg
            },Messenger.logServiceTag)

