from abc import ABC, abstractmethod
class CRUD:

    # IN cassandra this would be the where, in mongodb the find.
    #advancedQueryFactory :: [String], [String] -> String
    @abstractmethod
    def advancedQueryFactory( self, names, vals):
        ...

    #createX :: Object -> (Model) -> (req, res) -> None 
    @abstractmethod
    def createX(self, xClass):
        ...

    #queryFromReqRes :: [String] -> (req,res)
    @abstractmethod
    def queryFromReqRes():
        ...

    @abstractmethod
    def updateXbyY(self, xName, yName):
        ...
    
    #
    # deleteXbyY :: (xName, yName) -> Model -> (req, res) -> None
    @abstractmethod
    def deleteXbyY(self, xName, yName):
        ...

    @abstractmethod
    def getXbyY(self, xName, yName):
        ...


