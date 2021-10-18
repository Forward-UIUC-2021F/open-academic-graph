# Functional Design

### search_by_keyword
```python
def search_by_keyword(keyword):
    '''
    This method searches OAG for papers by using provided keywords.
    It returns a list of relevant papers. 
    '''
    return [papers]
```

### search_by_author
```python
def search_by_author(author):
    '''
    This method searches OAG for papers by using provided author.
    It returns a list of papers written by given author. 
    '''
    return [papers]
```

### search_by_date
```python
def search_by_date(start_date, end_date=present):
    '''
    This method searches OAG for papers by using provided start
    and end date. Default end date is present and default start
    date is data of oldest paper.
    It returns a list of papers published in given time range.
    '''
    return [papers]
```

### search_by_journal
```python
def search_by_journal(journal):
    '''
    This method searches OAG for papers by using provided journal.
    It returns a list of papers written by given journal. 
    '''
    return [papers]
```

### search_by_doi
```python
def search_by_doi(doi):
    '''
    This method searches OAG for papers by using provided DOI.
    It returns paper associated with DOI. Returns error if invalid
    DOI provided.
    '''
    return paper
```

### search_by_authors_with_n_papers
```python
def search_by_authors_with_n_papers(author, n):
    '''
    This method searches OAG for papers by using provided author
    and number.
    It returns papers associated with author only if the author
    published more than n papers.
    '''
    return [papers]
```

### search_by_papers_with_n_citations
```python
def search_by_papers_with_n_citations(paper=None, n):
    '''
    This method searches OAG for papers by using provided paper
    and number.
    It returns papers associated with n or more citations. If no
    paper is provided then the query will simply return all papers
    with n or more citations.
    '''
    return [papers]
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