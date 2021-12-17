from mysql_connect_es import ConnectToES

elastic = ConnectToES()

choice = int(input("Would you like to search for: 1. Author 2. Paper ..."))
if choice == 1:
    print("hey")
    auth_choice = int(input("Search by: 1. Name 2. Keyword"))
    if auth_choice == 1:
        name_to_search = input("Please enter a name to search: ")
        elastic.search_author_name(name_to_search)
    elif auth_choice == 2:
        keyword_to_search = input("Please enter a keyword to search by: ")
        elastic.search_author_by_keyword(keyword_to_search)
    else:
        print("Invalid input provided")
elif choice == 2:
    pap_choice = int(input("Search by: 1. Keyword 2. Author Name 3. DOI"))
    if pap_choice == 1:
        key_search = input("Please enter a keyword to search by: ")
        elastic.search_paper_by_keyword(key_search)
    elif pap_choice == 2:
        auth_search = input("Please enter a name to search: ")
        elastic.search_paper_by_author_name(auth_search)
    elif pap_choice == 3:
        doi_search = input("Please enter a doi to search by: ")
        elastic.search_paper_by_doi(doi_search)
    else:
        print("Invalid input provided")
else:
    print("Invalid input provided")
