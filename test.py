from messenger import Messenger

# ======================================================================================
# Message object layout:
# Message.UID : integer ( unique ID )
# Message.source : str ( source sender name )
# Message.dest : str ( destination name )
# Message.payload : object ( the payload that the source want to send to you )
# Message.tag : int ( You could utilize it to multipex different type of payload 
#                     or leave it to zero , built-in message use this tag for different
#                     services.For customization, use any value bigger than Messenger.userTag
# See real Message definition in sillynet.py
# =======================================================================================

# Class name must be Service
class Service:
    _messenger = None

    def _producingGarbage(self,data):
        self._messenger.sendMessage("reecho","echo",data,Messenger.unusedTag )
        self._messenger.sendTimerMessage(
            "reecho",1.0,self._producingGarbage,"Hello World!")
	
	# __init__ signature must be this, and messenger is the interface
	# that you could issue how to send message to the sillynet , so store
	# it in your member object
    def __init__(self,messenger):
        self._messenger = messenger
        ## Currently only built-in service works
        self._messenger.sendTimerMessage(
            "reecho",1.0,self._producingGarbage,"Hello World!")
    """ Just for testing """
	
	# This function is the main entry function that you wanna provide to 
	# process the message that is corresponding to you, just a callback .
    def process(self,message):
        ## Just re-dispatch the echo message to the echo service _AGAIN_
        self._messenger.sendMessage("reecho","echo",message.payload,Messenger.unusedTag )
                            