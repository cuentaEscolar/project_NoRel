from abc import ABC, abstractmethod
class CRUD:

    # IN cassandra this would be the where, in mongodb the find.
    #advancedQueryFactory :: [String], [String] -> String
    @abstractmethod
    def advancedQueryFactory( self, names, vals):
        ...

    @abstractmethod
    def createX(self, xClass):
        ...

    @abstractmethod
    def queryFromReqRes():
        ...

    @abstractmethod
    def updateXbyY(self, xName, yName):
        ...
    
    #
    @abstractmethod
    def deleteXbyY(self, xName, yName):
        ...

    @abstractmethod
    def getXbyY(self, xName, yName):
        ...


