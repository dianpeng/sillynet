class Messenger:
    """ This class provides protocol for using the message interface 

        The user will get a instance of this object in its process callback
        function, then user can use it to send message to other user's queue
    """

    userTag = 100
    unusedTag = 0
    
    """ This tag value is the minimum user allowed message tag value """

    def sendMessage(self,source,dest,tag,payload):
        """ The main interface used to send the message 

            Simple usage will be like this:
            messenger.sendMessage(somePayload,self.name,"OtherService")
        """
        pass

    def sendRemoteMessage(self,source,payload,addr,port):
        self.sendMessage(source,"remote",1,{
            "address":addr,
            "port":port,
            "packet":payload
        })

    def sendLogMessage(self,source,severity,msg):
        """ This function is used to write log in center log service

            The input severity MUST be python logging severity, and
            the underlying service will use the related python module
            to produce the log information 
        """
        self.sendMessage(source,"logger",2,{
            "severity":severity,
            "message":msg
            })

