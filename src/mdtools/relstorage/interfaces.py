from zope.interface import Interface


class IDatabase(Interface):

    def create_database():
        """Prepare the database."""

    def check_database():
        """Return true if the database is usable and ready."""

    def get_unused_oids():
        """Return a set of unused OIDs present in the database that are no
        longer referenced from the root object
        """

    def get_missing_oids():
        """Return a set of OIDs referenced in the database but missing from
        it.
        """

    def get_forward_references(oid):
        """Return a set of forward references."""

    def get_backward_references(oid):
        """Return a set of backward references."""

    def get_linked_to_oid(oid, depth):
        """Return a set of oid linked to oid up until the given depth.
        """
