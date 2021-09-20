# Functional Design

### unzip_data_file
```python
def unzip_data_file(zip_file):
    '''
    This method unzips a zip file with some data that 
    needs to be transferred to our database. No output.
    '''
```

### get_schema
```python
def get_schema(csv_file):
    '''
    Take the unzipped file with data in it and extract schema 
    information from it. This information will be outputted in the 
    form of a list of tuples (col_name, data_type)
    '''
    return schema
```

### transfer_data
```python
def transfer_data(csv_file, schema):
    '''
    Take data file and corresponding schema information and transfer
    it to our own database.
    '''
```

# Algorithmic Design
First step in this process is to transfer all the data from Open Academic Graph to our own database.
This will be achieved using Python, MySQL, and a remote workstation where the database will be housed. 
Once this has been done, we will build an API around the data in order to query it easily and efficiently.