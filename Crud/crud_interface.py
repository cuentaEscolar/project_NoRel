from abc import ABC, abstractmethod
class CRUD:

    # IN cassandra this would be the where, in mongodb the find.
    #advancedQueryFactory :: Map([String],Map()) -> String
    @abstractmethod
    def advancedQueryFactory( names_val_map ):
        ...

    #createX :: Object -> (Model) -> (req, res) -> None 
    @abstractmethod
    def createX(xClass):
        ...

    #queryFromReqRes :: [String] -> (req,res)
    @abstractmethod
    def queryFromReqRes():
        ...

    @abstractmethod
    def updateXbyY(xName, yName):
        ...

    # deleteXbyY :: (xName, yName) -> Model -> (req, res) -> None
    @abstractmethod
    def deleteXbyY(xName, yName):
        ...


