# This file extracts data from a remote repository in GitHub and creates a .csv file with
# any valuable leaks the file contains
import git
import pandas as pd
import re
import os
import sys
from IPython.display import display

pd.set_option('display.max_rows', 15)
pd.set_option('display.max_columns', 5)
pd.set_option('display.width', 1000000)
pd.set_option('display.colheader_justify', 'center')
pd.set_option('display.precision', 3)


def extract(url: str, **kwargs) -> git.Repo:
    if os.path.isdir('skale-manager-HTTPS'):
        print('Extracting repo from local dir: {}'.format('skale-manager-HTTPS'))
        return git.Repo('skale-manager-HTTPS', search_parent_directories=True)
    print('Extracting remote repo: {}'.format(url))
    if 'branch' in kwargs.items():
        repo = git.Repo.clone_from(url=url, to_path='skale-manager-HTTPS', branch=kwargs.get('branch'))
    else:
        repo = git.Repo.clone_from(url=url, to_path='skale-manager-HTTPS')
    print('Finished extracting remote repository: {}'.format(url))
    return repo


def transform(repo: git.Repo, keys: list) -> pd.DataFrame():
    print('Leaking data:')
    dataframe = pd.DataFrame(columns=['author', 'date', 'message'])
    patterns = re.compile("|".join(keys), re.UNICODE)
    for commit in repo.iter_commits():
        if patterns.search(commit.message, re.IGNORECASE):
            dataframe.loc[len(dataframe)] = [commit.author, commit.committed_date, commit.message]
    print('Finished leaking data')
    return dataframe


def load(dataframe: pd.DataFrame):
    print('Creating leaks.csv:')
    dataframe.to_json('leaks.json', orient='records', lines=True)
    return display(dataframe)


def main():
    try:
        url = 'https://github.com/skalenetwork/skale-manager'
        repository = extract(url=url)
        dataframe = transform(repo=repository, keys=['password', 'key'])
        load(dataframe=dataframe)
        print('Finished loading leaks')
    except KeyboardInterrupt:
        print("Forced exit: Exiting program...")


if __name__ == '__main__':
    sys.exit(main())
