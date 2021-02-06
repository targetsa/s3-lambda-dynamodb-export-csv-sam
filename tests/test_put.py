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


@pytest.mark.parametrize('key, expected_key', [
    ('FER-PGK+892/FER-PGK+892.png', 'FER-PGK 892/FER-PGK 892.png')
])
def test_s3_event_object_key_with_space(key, expected_key):
    from urllib.parse import unquote_plus

    actual = unquote_plus(key)
    expected = expected_key

    assert actual == expected


@pytest.mark.parametrize('key, expected_key', [
    ('FER-PGK+892/FER-PGK+892.png', 'FER-PGK%2B892/FER-PGK%2B892.png')
])
def test_s3_event_object_key_with_plus(key, expected_key):
    from urllib.parse import quote

    actual = quote(key)
    expected = expected_key

    assert actual == expected


@pytest.mark.parametrize('key', ['VTC-OLV-PNL-5590-D2-TAUPE/._VTC-OLV-PNL-5590-D2-TAUPE.png',
                                 'VTC-CEL-PNL-5590-EL-WHITE/._++VTC-CEL-PNL-5590-EL-WHITE-2.png',
                                 '.VTC-NYA-PNL-5590-EL-NAVY/VTC-NYA-PNL-5590-EL-NAVY.png',
                                 '.FER-00768/._FER-00768.jpg'
                                 ]
                         )
def test_s3_event_object_key_as_unix_hidden_file(key):
    object_path = key.split('/')
    filename = object_path[0] if len(object_path) == 1 else object_path[-1:][0]
    root_directory = object_path[0] if len(object_path) > 1 else ''
    parent_directory = object_path[-2] if len(object_path) > 2 else root_directory

    any_hidden_file_or_directory = any(df for df in [filename, root_directory, parent_directory] if df.startswith('.'))

    assert any_hidden_file_or_directory
