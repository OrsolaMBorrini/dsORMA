import rdflib as rdf

# classes
IdentifiableEntity = rdf.URIRef("http://purl.org/vocab/frbr/core#ResponsibleEntity")
Person = rdf.URIRef("http://purl.org/vocab/frbr/core#Person")
Publication = rdf.URIRef("http://purl.org/spar/fabio/Expression")
Venue = rdf.URIRef("http://purl.org/spar/fabio/Manifestation")
Organization = rdf.URIRef("http://purl.org/vocab/frbr/core#ResponsibleEntity")
JournalArticle = rdf.URIRef("http://purl.org/spar/fabio/JournalArticle")
BookChapter = rdf.URIRef("https://schema.org/Chapter")
ProceedingsPaper = rdf.URIRef("http://purl.org/spar/fabio/ProceedingsPaper")
Journal = rdf.URIRef("http://purl.org/spar/fabio/Journal")
Book = rdf.URIRef("http://purl.org/spar/fabio/Book")
Proceedings = rdf.URIRef("https://schema.org/EventSeries")

# attributes related to classes
Id = rdf.URIRef("https://schema.org/identifier")
authorId = rdf.URIRef("http://purl.org/cerif/frapo/hasORCID")
crossref = rdf.URIRef("http://purl.org/cerif/frapo/RegistrationAgency")

givenName = rdf.URIRef("https://schema.org/givenName")
familyName = rdf.URIRef("https://schema.org/familyName")
legalName = rdf.URIRef("http://data.tbfy.eu/ontology/ocds#legalName")

publicationYear = rdf.URIRef("https://schema.org/datePublished")
title = rdf.URIRef("https://schema.org/name")

issue = rdf.URIRef("https://schema.org/issueNumber")
volume = rdf.URIRef("https://schema.org/volumeNumber")
chapterNumber = rdf.URIRef("https://schema.org/Integer")  #it is a datatype and it's ok
event = rdf.URIRef("https://schema.org/event")

# relations among classes
author = rdf.URIRef("http://purl.org/vocab/frbr/core#creator")
publicationVenue = rdf.URIRef("http://purl.org/vocab/frbr/core#embodimentOf")
cites = rdf.URIRef("https://schema.org/citation")
publisher = rdf.URIRef("https://schema.org/publisher")
