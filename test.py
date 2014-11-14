import messenger

class Service:
    """ Just for testing """
    def process(self,message,messenger):
        messenger.sendMessage("reecho","echo",Messenger.unusedTag,message.payload)
                            