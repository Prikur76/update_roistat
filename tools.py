import re


def get_snakecase_row(s, prefix=None):
    pattern = r'[A-Z][a-z]+|[0-9A-Z]+(?=[A-Z][a-z])|[0-9A-Z]{2,}|[a-z0-9]{2,}|[a-zA-Z0-9]'
    re_words = re.findall(pattern, s)
    if prefix:
        re_words.insert(0, prefix)
    return '_'.join(re_words).lower()


def clean_phone(row):
    re_data = re.sub(r'[^0-9]+', '*', row)
    find_phone = re.findall(r'[789][0-9]{10}', re_data) if re_data else []
    return find_phone[0].replace('8', '7', 1) if find_phone else None


def split_fio(row):
    words = row.split()
    lastname = words[0].strip().capitalize()
    firstname = words[1].strip().capitalize() if len(words) > 1 else ''
    middlename = words[2].strip().capitalize() if len(words) > 2 else ''
    middlename_suffix = words[3].strip() if len(words) > 3 else ''
    return lastname, firstname, middlename, middlename_suffix
