class Main:
    from transfer_data import TransferData

    # --- create TransferData object, pass in username and password ---
    data_transfer = TransferData("ssarkar8", "")
    # --- connect to forward lab remote SSH and mysql db ---
    data_transfer.connect_to_SSH()
    # --- unzip files ---
    data_transfer.unzip_files()
    data_transfer.connect_to_database()
    # --- create tables in DB ---
    # data_transfer.create_tables_dict()
    # data_transfer.create_tables()
    data_transfer.show_tables()


    # --- close connections ---
    data_transfer.disconnect_from_db()
    data_transfer.disconnect_from_SSH()


