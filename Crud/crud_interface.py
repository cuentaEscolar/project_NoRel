class CRUD:

    # IN cassandra this would be the where, in mongodb the find.
    #advancedQueryFactory :: ([String],[String]) -> String
    @staticmethod
    def advancedQueryFactory(names, val_ranges):
        ...

    #createX :: Object -> (Model) -> (req, res) -> None 
    @staticmethod
    def createX(xClass):
        ...

    #queryFromReqRes :: [String] -> (req,res)
    @staticmethod
    def queryFromReqRes():
        ....

    @staticmethod
    def updateXbyY(xName, yName):
        ...

    # deleteXbyY :: (xName, yName) -> Model -> (req, res) -> None
    @staticmethod
    def deleteXbyY(xName, yName):
        ...


