class Main2:
    from remote_transfer import RemoteTransfer

    remote = RemoteTransfer()

    remote.get_filenames()
    remote.add_to_sql()