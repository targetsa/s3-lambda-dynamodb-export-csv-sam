import pytest


@pytest.mark.parametrize('key,expected_root_directory,expected_parent_directory', [
    ('LOK-PNA-15569/LOK-PNA-15569-1.png', "LOK-PNA-15569", ""),
    ('LOK-PNA-15569/LOK-PNA-15569-L-ROSA/LOK-PNA-15569-L-ROSA.png', "LOK-PNA-15569", "LOK-PNA-15569-L-ROSA"),
    ('LOK-PNA-15569/LOK-PNA-15569-L/LOK-PNA-15569-L-ROSA/LOK-PNA-15569-L-ROSA.png', "LOK-PNA-15569", "LOK-PNA-15569-L"),
    ('LOK-PNA-15569-1.png', "", ""),
])
def test_key_root_directory_and_parent_directory(key, expected_root_directory, expected_parent_directory):
    key_split = key.split('/')
    root_directory, parent_directory = key_split[:2] if len(key_split) > 2 else [
        key_split[0] if len(key_split) > 1 else '', '']

    assert (root_directory, parent_directory) == (expected_root_directory, expected_parent_directory)


@pytest.mark.parametrize('key,expected_root_directory,expected_parent_directory', [
    ('LOK-PNA-15569/LOK-PNA-15569-1.png', "LOK-PNA-15569", "LOK-PNA-15569"),
    ('LOK-PNA-15569/LOK-PNA-15569-L-ROSA/LOK-PNA-15569-L-ROSA.png', "LOK-PNA-15569", "LOK-PNA-15569-L-ROSA"),
    ('LOK-PNA-15569-1.png', "", ""),
    ('LOK-PNA-15569/LOK-PNA-15569-L/LOK-PNA-15569-L-ROSA/LOK-PNA-15569-L-ROSA.png',
     'LOK-PNA-15569',
     'LOK-PNA-15569-L-ROSA')
])
def test_key_root_directory_and_direct_parent_directory(key, expected_root_directory, expected_parent_directory):
    key_split = key.split('/')

    root_directory = key_split[0] if len(key_split) > 1 else ''
    parent_directory = key_split[-2] if len(key_split) > 2 else root_directory

    assert (root_directory, parent_directory) == (expected_root_directory, expected_parent_directory)
