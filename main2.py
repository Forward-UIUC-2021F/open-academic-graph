class Main2:
    from remote_transfer import RemoteTransfer

    # Run this file on the remote server
    remote = RemoteTransfer()

    remote.connect_to_database()
    print("******* CONNECTED TO DB *********")
    remote.add_to_sql()
    print("******** FINISHED ADDING TO DB ***********")