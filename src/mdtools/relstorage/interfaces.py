from zope.interface import Interface


class IReferencesDatabase(Interface):

    def createDatabase():
        """Prepare the database."""

    def checkDatabase():
        """Return true if the database is usable and ready."""

    def getUnUsedOIDs():
        """Return a set of unused OIDs present in the database that are no
        longer referenced from the root object
        """

    def getMissingOIDs():
        """Return a set of OIDs referenced in the database but missing from
        it.
        """

    def getForwardReferences(oid):
        """Return a set of forward references."""

    def getBackwardReferences(oid):
        """Return a set of backward references."""
