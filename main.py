# This file extracts data from a remote repository in GitHub and creates a .csv file with
# any valuable leaks the file contains
import re
import git
import sys
import pandas as pd


def extract(url: str, **kwargs) -> git.Repo:
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


def load(dataframe: pd.DataFrame) -> None:
    print('Creating leaks.csv:')
    dataframe.to_csv('leaks.csv', sep=';', encoding='utf-8')
    return print(dataframe)


def main():
    url = 'https://github.com/skalenetwork/skale-manager'
    repository = extract(url=url)
    dataframe = transform(repo=repository, keys=['password', 'key'])
    load(dataframe=dataframe)
    print('Finished loading leaks')


if __name__ == '__main__':
    sys.exit(main())
