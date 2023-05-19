# Implementation of the UML data model via Python classes

class IdentifiableEntity(object):
    # -- Constructor
    def __init__(self, identifiers):
        self.id = set() # Constraint is [1..*], hence the set
        for identifier in identifiers:
            self.id.add(identifier)

    # -- Methods
    def getIds(self):
        result = list()
        for ids in self.id:
            result.append(ids)
        return result


class Person(IdentifiableEntity):
    # -- Constructor
    def __init__(self, givenName, familyName, identifiers):
        self.givenName = givenName  # Must be exactly 1 string
        self.familyName = familyName  # Must be exactly 1 string

        # --- Upperclass parameters
        super().__init__(identifiers)

    # -- Methods
    def getGivenName(self):
        return self.givenName

    def getFamilyName(self):
        return self.familyName


class Organization(IdentifiableEntity):
    # -- Constructor
    def __init__(self, name, identifiers):
        self.name = name

        # --- Upperclass parameters
        super().__init__(identifiers)

    # -- Methods
    def getName(self):
        return self.name


class Venue(IdentifiableEntity):
    # -- Constructor
    def __init__(self, title, identifiers, publisher):
        self.title = title

        # --- Relations
        self.publisher = publisher

        # --- Upperclass parameters
        super().__init__(identifiers)

    # -- Methods
    def getTitle(self):
        return self.title

    def getPublisher(self):  # Returns an Organization object
        return self.publisher


class Publication(IdentifiableEntity):
    # -- Constructor
    def __init__(self, title, identifiers, author, cites, publicationVenue=None, publicationYear=None):
        self.publicationYear = publicationYear
        self.title = title

        # --- Relations
        self.publicationVenue = publicationVenue
        self.author = author  # This can be an input list/set as the relation has constraints 1..*
        self.cites = cites  # Input list/set, as constraint is 0..*

        # --- Upperclass parameters
        super().__init__(identifiers)

    # -- Methods
    def getPublicationYear(self):
        return self.publicationYear

    def getTitle(self):
        return self.title

    def getCitedPublications(self):
        return self.cites

    def getPublicationVenue(self):
        return self.publicationVenue

    def getAuthors(self):
        result = set()
        for person in self.author:
            result.add(person)
        return result


class JournalArticle(Publication):
    # -- Constructor
    def __init__(self, title, identifiers, author, cites, publicationVenue=None, publicationYear=None, issue=None, volume=None):
        self.issue = issue
        self.volume = volume

        # --- Upperclass parameters
        super().__init__(title, identifiers, author, cites, publicationVenue, publicationYear)
    
    # -- Methods
    def getIssue(self):
        return self.issue
    
    def getVolume(self):
        return self.volume
    


class BookChapter(Publication):
    # -- Constructor
    def __init__(self, chapterNumber, title, identifiers, author, cites, publicationVenue=None, publicationYear=None):
        self.chapterNumber = chapterNumber

        # --- Upperclass parameters
        super().__init__(title, identifiers, author, cites, publicationVenue, publicationYear)
    
    # -- Methods
    def getChapterNumber(self):
        return self.chapterNumber



class ProceedingsPaper(Publication):
    pass



class Journal(Venue):
    pass



class Book(Venue):
    pass



class Proceedings(Venue):
    # -- Constructor
    def __init__(self, event, title, identifiers, publisher):
        self.event = event

        # --- Upperclass parameters
        super().__init__(title, identifiers, publisher)
    
    def getEvent(self):
        return self.event
    

class QueryProcessor(object):
    pass