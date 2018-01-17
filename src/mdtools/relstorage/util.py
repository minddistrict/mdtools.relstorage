import os
import stat
import ZODB.DB
import ZODB.config
import ZODB.FileStorage.FileStorage
import ZEO.ClientStorage



def open_database(opts):
    if opts.db and opts.zeo:
        raise ValueError('you specified both ZEO and FileStorage; pick one')
    if opts.db and opts.config:
        raise ValueError('you specified both ZConfig and FileStorage; pick one')
    if opts.config and opts.zeo:
        raise ValueError('you specified both ZConfig and ZEO; pick one')
    if opts.storage and not opts.zeo:
        raise ValueError('a ZEO storage was specified without ZEO connection')

    if opts.db:
        filename = opts.db
        db = ZODB.DB.DB(ZODB.Filestorage.FileStorage.FileStorage(
            filename, read_only=opts.readonly))
    elif opts.zeo:
        if ':' in opts.zeo:
            # remote hostname:port ZEO connection
            zeo_address = opts.zeo.split(':', 1)
            try:
                zeo_address[1] = int(zeo_address[1])
            except ValueError:
                raise ValueError('specified ZEO port must be an integer')
            zeo_address = tuple(zeo_address)
        else:
            # socket filename
            zeo_address = opts.zeo
            if os.path.exists(zeo_address):
                # try ZEO connection through UNIX socket
                mode = os.stat(zeo_address)
                if not stat.S_ISSOCK(mode.st_mode):
                    raise ValueError(
                        'specified file is not a valid UNIX socket')
            else:
                # remote ZEO connection
                zeo_address = (zeo_address, 8100)
        if opts.storage:
            zeo_storage = opts.storage
        else:
            zeo_storage = '1'
        db = ZODB.DB.DB(ZEO.ClientStorage.ClientStorage(
            zeo_address, storage=zeo_storage, read_only=opts.readonly))
    elif opts.config:
        db = ZODB.config.databaseFromFile(open(opts.config))
    else:
        raise ValueError('please specify a database')
    return db
