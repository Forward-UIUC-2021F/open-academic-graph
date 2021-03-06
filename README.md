# Setup 
1. Download necessary dependencies detailed in requirements.txt using the following command:
    ```python
    pip install -r requirements.txt
    ```    
2. Install MySQL to open oagdump.sql
3. Run *interact.py* to see module in action 
    ```python
    python3 interact.py
    ```
# Repo File Structure
```python
open-academic-graph
    - README.md
    - AlgorithmicDesign.jpeg
    - interact.py
    - main.py
    - main2.py
    - mysql_connect_es.py
    - remote_transfer.py 
    - requirements.txt 
    - transfer_data.py
```

 

# Functional Design

### search_author_by_keyword
```python
def search_author_by_keyword(keyword):
    '''
    This method searches OAG for authors by using provided keywords.
    It returns a list of relevant authors. 
    '''
    return [authors]
```

### search_author_name
```python
def search_author_name(name):
    '''
    This method searches OAG for author by using provided name.
    It returns given author. 
    '''
    return [authors]
```

### search_paper_by_author_name
```python
def search_paper_by_author_name(name):
    '''
    This method searches OAG for papers by using provided name.
    It returns a list of papers by given author.
    '''
    return [papers]
```

### search_paper_by_keyword
```python
def search_paper_by_keyword(keyword):
    '''
    This method searches OAG for papers by using provided keyword.
    It returns a list of papers written by given keyword. 
    '''
    return [papers]
```

### search_paper_by_doi
```python
def search_paper_by_doi(doi):
    '''
    This method searches OAG for papers by using provided DOI.
    It returns paper associated with DOI. Returns error if invalid
    DOI provided.
    '''
    return paper
```



# Algorithmic Design
First step in this process is to transfer all the data from Open Academic Graph to our own database.
This will be achieved using Python, MySQL, and a remote workstation where the database will be housed. 
Once this has been done, we will build an API around the data in order to query it easily and efficiently.

The API will use ElasticSearch to query the data. In the diagram below we see that we are utilizing 
logstash to connect the MySQL databse and ElasticSearch. Logstash has the additional benefit of being 
capable of collecting, normalizing, and writing application data to multiple sources. It is not clear yet
if we will need to utilize *all* of these capabilities, but it will certainly help to collect and normalize
the data. 

The diagram below also shows how users can make queries and the API built will convert it into an ElasticSearch
query that communicates with ElasticSearch to return the requested data. Will also potentially use Kibana
for its monitoring and reporting capabilities. 

![Algorithmic Design](Algorithmic%20Design.jpeg)

# Issues and Future Work 
- The module runs slowly, needs to be optimized to use ElasticSearch